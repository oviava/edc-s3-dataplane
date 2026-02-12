"""Data flow use-case service."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Protocol, runtime_checkable
from urllib.parse import urlparse
from uuid import uuid4

from simpl_bulk_dataplane.domain.callbacks import (
    ControlPlaneCallbackEvent,
    ControlPlaneCallbackEventType,
)
from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.errors import (
    DataFlowConflictError,
    DataFlowNotFoundError,
    DataFlowValidationError,
)
from simpl_bulk_dataplane.domain.monitoring_models import (
    DataFlowInfoResponse,
    DataFlowListResponse,
    DataFlowProgressResponse,
    TransferProgressSnapshot,
)
from simpl_bulk_dataplane.domain.ports import (
    CallbackOutboxRepository,
    ControlPlaneNotifier,
    DataFlowEventPublisher,
    DataFlowRepository,
    TransferJobRepository,
    TransferExecutor,
)
from simpl_bulk_dataplane.domain.signaling_models import (
    DataAddress,
    DataFlowBaseMessage,
    DataFlowPrepareMessage,
    DataFlowResponseMessage,
    DataFlowResumeMessage,
    DataFlowStartedNotificationMessage,
    DataFlowStartMessage,
    DataFlowStatusResponseMessage,
    TransferStartResponseMessage,
)
from simpl_bulk_dataplane.domain.transfer_types import (
    TERMINAL_STATES,
    DataFlowState,
    TransferMode,
    ensure_s3_transfer_type,
    parse_transfer_mode,
)
from simpl_bulk_dataplane.domain.transfer_jobs import TransferJobStatus

_FLOW_ORIGIN_METADATA_KEY = "_dpsFlowOrigin"
_FLOW_ORIGIN_PREPARE = "prepare"
_FLOW_ORIGIN_START = "start"
_STARTED_NOTIFICATION_SEEN_METADATA_KEY = "_dpsStartedNotificationSeen"
_DEFAULT_TRANSFER_JOB_RECOVERY_POLL_SECONDS = 1.0
_DEFAULT_TRANSFER_JOB_RECOVERY_BATCH_SIZE = 20
_DEFAULT_TRANSFER_JOB_LEASE_SECONDS = 30.0
_DEFAULT_TRANSFER_JOB_HEARTBEAT_SECONDS = 10.0

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CommandResult:
    """HTTP mapping result for command-style operations."""

    status_code: int
    body: DataFlowResponseMessage | None = None
    location: str | None = None


@runtime_checkable
class _CompletionCallbackAwareTransferExecutor(Protocol):
    """Optional transfer-executor extension for runtime completion callbacks."""

    def set_completion_callback(
        self,
        callback: Callable[[str], Awaitable[None]] | None,
    ) -> None:
        """Register callback invoked after runtime transfer completion."""


@runtime_checkable
class _CallbackOutboxDispatcher(Protocol):
    """Optional callback outbox dispatcher lifecycle + wake-up hooks."""

    async def start(self) -> None:
        """Start background callback delivery loop."""

    async def stop(self) -> None:
        """Stop background callback delivery loop."""

    async def notify_new_event(self) -> None:
        """Signal dispatcher that a new callback event was enqueued."""


class DataFlowService:
    """Orchestrates state transitions for signaling commands."""

    def __init__(
        self,
        dataplane_id: str,
        repository: DataFlowRepository,
        transfer_executor: TransferExecutor,
        control_plane_notifier: ControlPlaneNotifier,
        dataflow_event_publisher: DataFlowEventPublisher,
        callback_outbox_dispatcher: _CallbackOutboxDispatcher | None = None,
        transfer_job_repository: TransferJobRepository | None = None,
        transfer_job_recovery_poll_seconds: float = _DEFAULT_TRANSFER_JOB_RECOVERY_POLL_SECONDS,
        transfer_job_recovery_batch_size: int = _DEFAULT_TRANSFER_JOB_RECOVERY_BATCH_SIZE,
        transfer_job_lease_seconds: float = _DEFAULT_TRANSFER_JOB_LEASE_SECONDS,
        transfer_job_heartbeat_seconds: float = _DEFAULT_TRANSFER_JOB_HEARTBEAT_SECONDS,
    ) -> None:
        self._dataplane_id = dataplane_id
        self._repository = repository
        self._transfer_executor = transfer_executor
        self._control_plane_notifier = control_plane_notifier
        self._dataflow_event_publisher = dataflow_event_publisher
        self._callback_outbox_dispatcher = callback_outbox_dispatcher
        if transfer_job_repository is not None:
            self._transfer_job_repository = transfer_job_repository
        elif isinstance(repository, TransferJobRepository):
            self._transfer_job_repository = repository
        else:
            self._transfer_job_repository = None
        self._transfer_job_recovery_poll_seconds = max(transfer_job_recovery_poll_seconds, 0.05)
        self._transfer_job_recovery_batch_size = max(transfer_job_recovery_batch_size, 1)
        self._transfer_job_lease_seconds = max(transfer_job_lease_seconds, 1.0)
        self._transfer_job_heartbeat_seconds = max(
            min(transfer_job_heartbeat_seconds, self._transfer_job_lease_seconds),
            0.1,
        )
        self._transfer_job_lease_owner = f"{dataplane_id}:{uuid4()}"
        self._transfer_job_recovery_task: asyncio.Task[None] | None = None
        self._transfer_job_recovery_stop = asyncio.Event()
        self._transfer_job_recovery_wake = asyncio.Event()
        self._transfer_job_heartbeat_tasks: dict[str, asyncio.Task[None]] = {}
        self._heartbeat_tasks_lock = asyncio.Lock()
        self._service_running = False
        if isinstance(transfer_executor, _CompletionCallbackAwareTransferExecutor):
            transfer_executor.set_completion_callback(self._completed_from_runtime)

    async def startup(self) -> None:
        """Start optional background workers owned by this service."""

        self._service_running = True
        dispatcher = self._callback_outbox_dispatcher
        if dispatcher is not None:
            await dispatcher.start()
        if self._transfer_job_repository is None:
            return
        task = self._transfer_job_recovery_task
        if task is not None and not task.done():
            return
        self._transfer_job_recovery_stop.clear()
        self._transfer_job_recovery_wake.set()
        self._transfer_job_recovery_task = asyncio.create_task(
            self._run_transfer_job_recovery_loop(),
            name="transfer-job-recovery-loop",
        )

    async def shutdown(self) -> None:
        """Stop optional background workers owned by this service."""

        self._service_running = False
        await self._stop_transfer_job_recovery_loop()
        await self._stop_all_transfer_job_heartbeats()
        dispatcher = self._callback_outbox_dispatcher
        if dispatcher is not None:
            await dispatcher.stop()

    async def prepare(self, message: DataFlowPrepareMessage) -> CommandResult:
        """Handle `/dataflows/prepare`."""

        ensure_s3_transfer_type(message.transfer_type)
        existing = await self._repository.get_by_process_id(message.process_id)
        if existing is not None:
            raise DataFlowConflictError(
                f"A data flow already exists for processId '{message.process_id}'."
            )

        transfer_mode = parse_transfer_mode(message.transfer_type)
        data_flow = self._new_data_flow(
            message,
            transfer_mode,
            flow_origin=_FLOW_ORIGIN_PREPARE,
        )

        if self._is_async_requested(message.metadata, "Prepare"):
            data_flow.state = DataFlowState.PREPARING
            await self._repository.upsert(data_flow)
            await self._upsert_transfer_job(
                data_flow_id=data_flow.data_flow_id,
                status=TransferJobStatus.QUEUED,
            )
            self._wake_transfer_job_recovery_loop()
            await self._publish_state_event(data_flow)
            return CommandResult(
                status_code=202,
                body=self._response_message(data_flow),
                location=data_flow.status_path,
            )

        prepared_address = await self._transfer_executor.prepare(data_flow, message)
        if prepared_address is not None:
            data_flow.data_address = prepared_address

        data_flow.state = DataFlowState.PREPARED
        response = self._response_message(data_flow)
        await self._persist_transition_with_callback(
            data_flow,
            response,
            callback_event_type=ControlPlaneCallbackEventType.PREPARED,
        )
        await self._upsert_transfer_job(
            data_flow_id=data_flow.data_flow_id,
            status=TransferJobStatus.COMPLETED,
        )
        await self._publish_state_event(data_flow)
        return CommandResult(status_code=200, body=response)

    async def start(self, message: DataFlowStartMessage) -> CommandResult:
        """Handle `/dataflows/start`."""

        ensure_s3_transfer_type(message.transfer_type)
        transfer_mode = parse_transfer_mode(message.transfer_type)

        existing = await self._repository.get_by_process_id(message.process_id)
        if existing is not None:
            raise DataFlowConflictError(
                f"A data flow already exists for processId '{message.process_id}'. "
                "Use /dataflows/{id}/resume for suspend/start cycles."
            )

        data_flow = self._new_data_flow(
            message,
            transfer_mode,
            flow_origin=_FLOW_ORIGIN_START,
        )
        self._validate_start_data_address(transfer_mode, message.data_address)

        if message.data_address is not None:
            data_flow.data_address = message.data_address

        if self._is_async_requested(message.metadata, "Start"):
            data_flow.state = DataFlowState.STARTING
            await self._repository.upsert(data_flow)
            await self._upsert_transfer_job(
                data_flow_id=data_flow.data_flow_id,
                status=TransferJobStatus.QUEUED,
            )
            self._wake_transfer_job_recovery_loop()
            await self._publish_state_event(data_flow)
            return CommandResult(
                status_code=202,
                body=self._response_message(data_flow),
                location=data_flow.status_path,
            )

        started_address = await self._transfer_executor.start(data_flow, message)
        if started_address is not None:
            data_flow.data_address = started_address

        data_flow.state = DataFlowState.STARTED
        response = self._response_message(data_flow)
        await self._persist_transition_with_callback(
            data_flow,
            response,
            callback_event_type=ControlPlaneCallbackEventType.STARTED,
        )
        if transfer_mode is TransferMode.PUSH:
            await self._mark_transfer_job_running(data_flow.data_flow_id)
        else:
            await self._upsert_transfer_job(
                data_flow_id=data_flow.data_flow_id,
                status=TransferJobStatus.COMPLETED,
            )
        await self._publish_state_event(data_flow)
        return CommandResult(status_code=200, body=response)

    async def notify_started(
        self,
        data_flow_id: str,
        message: DataFlowStartedNotificationMessage | None,
    ) -> None:
        """Handle `/dataflows/{id}/started` for consumer-side transitions."""

        data_flow = await self._get_data_flow_or_raise(data_flow_id)
        data_flow = await self._reconcile_runtime_failure(data_flow)
        if data_flow.state in TERMINAL_STATES:
            raise DataFlowConflictError(
                f"Cannot mark data flow '{data_flow.data_flow_id}' as started in "
                f"state '{data_flow.state}'."
            )
        effective_message = message
        if (
            data_flow.transfer_mode is TransferMode.PULL
            and (effective_message is None or effective_message.data_address is None)
            and data_flow.data_address is not None
        ):
            # Resume can omit the source address if it was already persisted.
            effective_message = DataFlowStartedNotificationMessage(
                data_address=data_flow.data_address
            )

        self._validate_started_notification(data_flow.transfer_mode, effective_message)

        if effective_message is not None and effective_message.data_address is not None:
            data_flow.data_address = effective_message.data_address

        await self._transfer_executor.notify_started(data_flow, effective_message)
        data_flow.state = DataFlowState.STARTED
        data_flow.metadata[_STARTED_NOTIFICATION_SEEN_METADATA_KEY] = True
        await self._repository.upsert(data_flow)
        if data_flow.transfer_mode is TransferMode.PULL:
            await self._mark_transfer_job_running(data_flow.data_flow_id)
        else:
            await self._upsert_transfer_job(
                data_flow_id=data_flow.data_flow_id,
                status=TransferJobStatus.COMPLETED,
            )
        await self._publish_state_event(data_flow)

    async def resume(
        self,
        data_flow_id: str,
        message: DataFlowResumeMessage,
    ) -> TransferStartResponseMessage:
        """Handle `/dataflows/{id}/resume`."""

        data_flow = await self._get_data_flow_or_raise(data_flow_id)
        data_flow = await self._reconcile_runtime_failure(data_flow)
        if data_flow.state != DataFlowState.SUSPENDED:
            raise DataFlowValidationError(
                f"Resume is only allowed in SUSPENDED state, got '{data_flow.state}'."
            )
        if message.process_id != data_flow.process_id:
            raise DataFlowValidationError(
                "processId in resume message does not match the targeted data flow."
            )

        if message.data_address is not None:
            data_flow.data_address = message.data_address

        execution_started = False
        if self._should_resume_via_started_notification(data_flow):
            started_message = self._resume_started_notification_message(data_flow)
            await self._transfer_executor.notify_started(data_flow, started_message)
            data_flow.metadata[_STARTED_NOTIFICATION_SEEN_METADATA_KEY] = True
            execution_started = data_flow.transfer_mode is TransferMode.PULL
        else:
            if data_flow.transfer_mode is TransferMode.PUSH and data_flow.data_address is None:
                raise DataFlowValidationError(
                    "dataAddress is required to resume provider-push data flows."
                )
            started_address = await self._transfer_executor.start(
                data_flow,
                self._resume_start_message(data_flow),
            )
            if started_address is not None:
                data_flow.data_address = started_address
            execution_started = data_flow.transfer_mode is TransferMode.PUSH

        data_flow.state = DataFlowState.STARTED
        await self._repository.upsert(data_flow)
        if execution_started:
            await self._mark_transfer_job_running(data_flow.data_flow_id)
        else:
            await self._upsert_transfer_job(
                data_flow_id=data_flow.data_flow_id,
                status=TransferJobStatus.COMPLETED,
            )
        await self._publish_state_event(data_flow)
        return TransferStartResponseMessage(data_address=data_flow.data_address)

    async def suspend(self, data_flow_id: str, reason: str | None) -> None:
        """Handle `/dataflows/{id}/suspend`."""

        data_flow = await self._get_data_flow_or_raise(data_flow_id)
        data_flow = await self._reconcile_runtime_failure(data_flow)
        if data_flow.state != DataFlowState.STARTED:
            raise DataFlowValidationError(
                f"Suspend is only allowed in STARTED state, got '{data_flow.state}'."
            )

        await self._transfer_executor.suspend(data_flow, reason)
        data_flow.state = DataFlowState.SUSPENDED
        await self._repository.upsert(data_flow)
        await self._upsert_transfer_job(
            data_flow_id=data_flow.data_flow_id,
            status=TransferJobStatus.PAUSED,
        )
        await self._publish_state_event(data_flow)

    async def terminate(self, data_flow_id: str, reason: str | None) -> None:
        """Handle `/dataflows/{id}/terminate`."""

        data_flow = await self._get_data_flow_or_raise(data_flow_id)
        data_flow = await self._reconcile_runtime_failure(data_flow)
        if data_flow.state == DataFlowState.TERMINATED:
            return
        if data_flow.state in TERMINAL_STATES:
            raise DataFlowConflictError(
                f"Cannot terminate data flow '{data_flow.data_flow_id}' in "
                f"terminal state '{data_flow.state}'."
            )

        await self._transfer_executor.terminate(data_flow, reason)
        data_flow.state = DataFlowState.TERMINATED
        response = self._response_message(data_flow)
        await self._persist_transition_with_callback(
            data_flow,
            response,
            callback_event_type=ControlPlaneCallbackEventType.ERRORED,
        )
        await self._upsert_transfer_job(
            data_flow_id=data_flow.data_flow_id,
            status=TransferJobStatus.TERMINATED,
        )
        await self._publish_state_event(data_flow)

    async def completed(self, data_flow_id: str) -> None:
        """Handle `/dataflows/{id}/completed`."""

        data_flow = await self._get_data_flow_or_raise(data_flow_id)
        data_flow = await self._reconcile_runtime_failure(data_flow)
        if data_flow.state == DataFlowState.COMPLETED:
            return
        if data_flow.state in TERMINAL_STATES:
            raise DataFlowConflictError(
                f"Cannot complete data flow '{data_flow.data_flow_id}' in "
                f"terminal state '{data_flow.state}'."
            )

        await self._transfer_executor.complete(data_flow)
        data_flow.state = DataFlowState.COMPLETED
        response = self._response_message(data_flow)
        await self._persist_transition_with_callback(
            data_flow,
            response,
            callback_event_type=ControlPlaneCallbackEventType.COMPLETED,
        )
        await self._upsert_transfer_job(
            data_flow_id=data_flow.data_flow_id,
            status=TransferJobStatus.COMPLETED,
        )
        await self._publish_state_event(data_flow)

    async def get_status(self, data_flow_id: str) -> DataFlowStatusResponseMessage:
        """Handle `/dataflows/{id}/status`."""

        data_flow = await self._get_data_flow_or_raise(data_flow_id)
        data_flow = await self._reconcile_runtime_failure(data_flow)
        return DataFlowStatusResponseMessage(
            data_flow_id=data_flow.data_flow_id,
            state=data_flow.state,
        )

    async def list_data_flows(
        self,
        transfer_mode: TransferMode | None = None,
    ) -> DataFlowListResponse:
        """List flows with progress details for management UIs."""

        data_flows = await self._repository.list_data_flows()
        data_flows = [await self._reconcile_runtime_failure(flow) for flow in data_flows]
        if transfer_mode is not None:
            data_flows = [flow for flow in data_flows if flow.transfer_mode is transfer_mode]

        flow_infos = [await self._to_data_flow_info(flow) for flow in data_flows]
        return DataFlowListResponse(
            dataplane_id=self._dataplane_id,
            data_flows=flow_infos,
        )

    async def get_data_flow_info(self, data_flow_id: str) -> DataFlowInfoResponse:
        """Fetch one flow with progress details for management UIs."""

        data_flow = await self._get_data_flow_or_raise(data_flow_id)
        data_flow = await self._reconcile_runtime_failure(data_flow)
        return await self._to_data_flow_info(data_flow)

    def _new_data_flow(
        self,
        message: DataFlowBaseMessage,
        transfer_mode: TransferMode,
        *,
        flow_origin: str,
    ) -> DataFlow:
        """Build a new data flow aggregate from incoming request data."""

        metadata = dict(message.metadata)
        metadata[_FLOW_ORIGIN_METADATA_KEY] = flow_origin

        return DataFlow(
            data_flow_id=f"flow-{uuid4()}",
            process_id=message.process_id,
            transfer_type=message.transfer_type,
            transfer_mode=transfer_mode,
            participant_id=message.participant_id,
            counter_party_id=message.counter_party_id,
            dataspace_context=message.dataspace_context,
            agreement_id=message.agreement_id,
            dataset_id=message.dataset_id,
            callback_address=message.callback_address,
            labels=list(message.labels),
            metadata=metadata,
        )

    def _response_message(
        self, data_flow: DataFlow, error: str | None = None
    ) -> DataFlowResponseMessage:
        """Map aggregate state to signaling response payload."""

        return DataFlowResponseMessage(
            dataplane_id=self._dataplane_id,
            data_flow_id=data_flow.data_flow_id,
            state=data_flow.state,
            data_address=data_flow.data_address,
            error=error,
        )

    def _is_async_requested(self, metadata: dict[str, object], operation: str) -> bool:
        """Allow optional async mode simulation via metadata flags."""

        marker = f"simulateAsync{operation}"
        return bool(metadata.get(marker) is True)

    async def _get_data_flow_or_raise(self, data_flow_id: str) -> DataFlow:
        """Fetch a data flow or raise a not-found domain error."""

        data_flow = await self._repository.get_by_data_flow_id(data_flow_id)
        if data_flow is None:
            raise DataFlowNotFoundError(f"No data flow found for id '{data_flow_id}'.")
        return data_flow

    def _validate_start_data_address(
        self,
        transfer_mode: TransferMode,
        data_address: DataAddress | None,
    ) -> None:
        """Apply start-request address rules from DPS spec."""

        if transfer_mode is TransferMode.PUSH and data_address is None:
            raise DataFlowValidationError(
                "dataAddress is required for provider-push transfers on /dataflows/start."
            )
        if transfer_mode is TransferMode.PULL and data_address is not None:
            raise DataFlowValidationError(
                "dataAddress must be omitted for consumer-pull transfers on /dataflows/start."
            )

    def _validate_started_notification(
        self,
        transfer_mode: TransferMode,
        message: DataFlowStartedNotificationMessage | None,
    ) -> None:
        """Apply started-notification address rules from DPS spec."""

        data_address = None if message is None else message.data_address
        if transfer_mode is TransferMode.PULL and data_address is None:
            raise DataFlowValidationError(
                "dataAddress is required on /dataflows/{id}/started for consumer-pull transfers."
            )
        if transfer_mode is TransferMode.PUSH and data_address is not None:
            raise DataFlowValidationError(
                "dataAddress must be omitted on /dataflows/{id}/started "
                "for provider-push transfers."
            )

    def _flow_origin(self, data_flow: DataFlow) -> str:
        """Return the flow origin marker from metadata, defaulting to start."""

        origin = data_flow.metadata.get(_FLOW_ORIGIN_METADATA_KEY)
        if isinstance(origin, str) and origin in {_FLOW_ORIGIN_PREPARE, _FLOW_ORIGIN_START}:
            return origin
        return _FLOW_ORIGIN_START

    def _should_resume_via_started_notification(self, data_flow: DataFlow) -> bool:
        """Decide whether resume should reuse the `/started` execution path."""

        flow_origin = self._flow_origin(data_flow)
        if data_flow.transfer_mode is TransferMode.PUSH:
            return flow_origin == _FLOW_ORIGIN_PREPARE

        started_seen = data_flow.metadata.get(_STARTED_NOTIFICATION_SEEN_METADATA_KEY) is True
        return flow_origin == _FLOW_ORIGIN_PREPARE or started_seen

    def _resume_started_notification_message(
        self,
        data_flow: DataFlow,
    ) -> DataFlowStartedNotificationMessage | None:
        """Build the executor message used when resume follows `/started` semantics."""

        if data_flow.transfer_mode is TransferMode.PUSH:
            return None
        if data_flow.data_address is None:
            raise DataFlowValidationError(
                "dataAddress is required to resume consumer-pull data flows."
            )
        return DataFlowStartedNotificationMessage(data_address=data_flow.data_address)

    def _resume_start_message(self, data_flow: DataFlow) -> DataFlowStartMessage:
        """Build a synthetic start message for provider-side resume handling."""

        callback_address = data_flow.callback_address
        if callback_address is None:
            raise DataFlowValidationError(
                "Cannot resume flow without callbackAddress persisted on the data flow."
            )
        return DataFlowStartMessage(
            message_id=f"resume-{uuid4()}",
            participant_id=data_flow.participant_id,
            counter_party_id=data_flow.counter_party_id,
            dataspace_context=data_flow.dataspace_context,
            process_id=data_flow.process_id,
            agreement_id=data_flow.agreement_id,
            dataset_id=data_flow.dataset_id,
            callback_address=callback_address,
            transfer_type=data_flow.transfer_type,
            labels=list(data_flow.labels),
            metadata=dict(data_flow.metadata),
            data_address=data_flow.data_address,
        )

    def _resume_prepare_message(self, data_flow: DataFlow) -> DataFlowPrepareMessage:
        """Build a synthetic prepare message for recovery of PREPARING flows."""

        callback_address = data_flow.callback_address
        if callback_address is None:
            raise DataFlowValidationError(
                "Cannot recover prepare flow without callbackAddress persisted on the data flow."
            )
        return DataFlowPrepareMessage(
            message_id=f"recover-prepare-{uuid4()}",
            participant_id=data_flow.participant_id,
            counter_party_id=data_flow.counter_party_id,
            dataspace_context=data_flow.dataspace_context,
            process_id=data_flow.process_id,
            agreement_id=data_flow.agreement_id,
            dataset_id=data_flow.dataset_id,
            callback_address=callback_address,
            transfer_type=data_flow.transfer_type,
            labels=list(data_flow.labels),
            metadata=dict(data_flow.metadata),
        )

    async def _to_data_flow_info(self, data_flow: DataFlow) -> DataFlowInfoResponse:
        """Build management response payload for one data flow."""

        progress_snapshot = await self._transfer_executor.get_progress(data_flow.data_flow_id)
        progress = self._to_progress_response(data_flow.state, progress_snapshot)

        source_bucket = self._metadata_str(data_flow.metadata, "sourceBucket")
        source_key = self._metadata_str(data_flow.metadata, "sourceKey")
        destination_bucket = self._metadata_str(data_flow.metadata, "destinationBucket")
        destination_key = self._metadata_str(data_flow.metadata, "destinationKey")

        if data_flow.data_address is not None:
            bucket_from_address, key_from_address = self._bucket_key_from_data_address(
                data_flow.data_address
            )
            if data_flow.transfer_mode is TransferMode.PULL:
                if source_bucket is None:
                    source_bucket = bucket_from_address
                if source_key is None:
                    source_key = key_from_address
            if data_flow.transfer_mode is TransferMode.PUSH:
                if destination_bucket is None:
                    destination_bucket = bucket_from_address
                if destination_key is None:
                    destination_key = key_from_address

        return DataFlowInfoResponse(
            dataplane_id=self._dataplane_id,
            data_flow_id=data_flow.data_flow_id,
            process_id=data_flow.process_id,
            transfer_type=data_flow.transfer_type,
            transfer_mode=data_flow.transfer_mode,
            state=data_flow.state,
            source_bucket=source_bucket,
            source_key=source_key,
            destination_bucket=destination_bucket,
            destination_key=destination_key,
            progress=progress,
        )

    def _to_progress_response(
        self,
        state: DataFlowState,
        snapshot: TransferProgressSnapshot | None,
    ) -> DataFlowProgressResponse:
        """Merge state and executor snapshot into a stable progress payload."""

        if snapshot is None:
            return DataFlowProgressResponse(
                bytes_total=None,
                bytes_transferred=0,
                percent_complete=100.0 if state is DataFlowState.COMPLETED else None,
                running=state in {DataFlowState.STARTING, DataFlowState.STARTED},
                queued=False,
                paused=state is DataFlowState.SUSPENDED,
                finished=state in TERMINAL_STATES,
                last_error=None,
            )

        percent_complete = snapshot.percent_complete
        if percent_complete is None and state is DataFlowState.COMPLETED:
            percent_complete = 100.0

        return DataFlowProgressResponse(
            bytes_total=snapshot.bytes_total,
            bytes_transferred=snapshot.bytes_transferred,
            percent_complete=percent_complete,
            running=snapshot.running,
            queued=snapshot.queued,
            paused=snapshot.paused,
            finished=snapshot.finished or state in TERMINAL_STATES,
            last_error=snapshot.last_error,
        )

    def _metadata_str(self, metadata: dict[str, object], key: str) -> str | None:
        """Read metadata value as string when present."""

        value = metadata.get(key)
        if value is None:
            return None
        return str(value)

    def _bucket_key_from_data_address(
        self,
        data_address: DataAddress,
    ) -> tuple[str | None, str | None]:
        """Extract bucket/key from known S3 endpoint address formats."""

        parsed = urlparse(data_address.endpoint)
        scheme = parsed.scheme.lower()
        if scheme == "s3":
            bucket = parsed.netloc or None
            key = parsed.path.lstrip("/") or None
            return bucket, key
        if scheme in {"http", "https"}:
            path_parts = parsed.path.lstrip("/").split("/", 1)
            if len(path_parts) != 2:
                return None, None
            bucket, key = path_parts
            return (bucket or None), (key or None)
        return None, None

    async def _publish_state_event(self, data_flow: DataFlow) -> None:
        """Publish state events without affecting core signaling behavior."""

        progress = await self._transfer_executor.get_progress(data_flow.data_flow_id)
        with suppress(Exception):
            await self._dataflow_event_publisher.publish_state(data_flow, progress)

    async def _persist_transition_with_callback(
        self,
        data_flow: DataFlow,
        response: DataFlowResponseMessage,
        *,
        callback_event_type: ControlPlaneCallbackEventType,
    ) -> None:
        """Persist state and schedule callback delivery with fallback compatibility mode."""

        dispatcher = self._callback_outbox_dispatcher
        if dispatcher is None or not isinstance(self._repository, CallbackOutboxRepository):
            await self._repository.upsert(data_flow)
            await self._notify_control_plane_immediately(
                data_flow,
                response,
                callback_event_type=callback_event_type,
            )
            return

        callback_event = ControlPlaneCallbackEvent(
            process_id=data_flow.process_id,
            data_flow_id=data_flow.data_flow_id,
            event_type=callback_event_type,
            callback_address=data_flow.callback_address,
            payload=response,
        )
        await self._repository.persist_transition(data_flow, callback_event)
        await dispatcher.notify_new_event()

    async def _notify_control_plane_immediately(
        self,
        data_flow: DataFlow,
        response: DataFlowResponseMessage,
        *,
        callback_event_type: ControlPlaneCallbackEventType,
    ) -> None:
        if callback_event_type is ControlPlaneCallbackEventType.PREPARED:
            await self._control_plane_notifier.notify_prepared(data_flow, response)
            return
        if callback_event_type is ControlPlaneCallbackEventType.STARTED:
            await self._control_plane_notifier.notify_started(data_flow, response)
            return
        if callback_event_type is ControlPlaneCallbackEventType.COMPLETED:
            await self._control_plane_notifier.notify_completed(data_flow, response)
            return
        if callback_event_type is ControlPlaneCallbackEventType.ERRORED:
            await self._control_plane_notifier.notify_terminated(data_flow, response)
            return

        raise DataFlowValidationError(
            f"Unsupported callback event type '{callback_event_type}'."
        )

    async def _reconcile_runtime_failure(self, data_flow: DataFlow) -> DataFlow:
        """Promote failed runtime sessions to TERMINATED when executor reports errors."""

        if data_flow.state in TERMINAL_STATES:
            return data_flow

        snapshot = await self._transfer_executor.get_progress(data_flow.data_flow_id)
        if snapshot is None:
            return data_flow

        error = snapshot.last_error
        if error is None or not error.strip():
            return data_flow

        data_flow.state = DataFlowState.TERMINATED
        response = self._response_message(data_flow, error=error)
        await self._persist_transition_with_callback(
            data_flow,
            response,
            callback_event_type=ControlPlaneCallbackEventType.ERRORED,
        )
        await self._upsert_transfer_job(
            data_flow_id=data_flow.data_flow_id,
            status=TransferJobStatus.FAILED,
            last_error=error,
        )
        await self._publish_state_event(data_flow)
        return data_flow

    async def _completed_from_runtime(self, data_flow_id: str) -> None:
        """Best-effort completion hook fired by transfer runtime."""

        with suppress(DataFlowConflictError, DataFlowNotFoundError, DataFlowValidationError):
            await self.completed(data_flow_id)

    async def _run_transfer_job_recovery_loop(self) -> None:
        """Claim durable jobs and recover runtime execution after restarts."""

        repository = self._transfer_job_repository
        if repository is None:
            return

        while not self._transfer_job_recovery_stop.is_set():
            processed = 0
            try:
                claimed_jobs = await repository.claim_due_transfer_jobs(
                    lease_owner=self._transfer_job_lease_owner,
                    limit=self._transfer_job_recovery_batch_size,
                    lease_seconds=self._transfer_job_lease_seconds,
                )
                for claimed_job in claimed_jobs:
                    processed += 1
                    await self._recover_claimed_transfer_job(claimed_job.data_flow_id)
            except Exception:
                logger.exception("Transfer job recovery loop failed.")

            if processed > 0:
                continue

            self._transfer_job_recovery_wake.clear()
            try:
                await asyncio.wait_for(
                    self._transfer_job_recovery_wake.wait(),
                    timeout=self._transfer_job_recovery_poll_seconds,
                )
            except TimeoutError:
                pass

    async def _recover_claimed_transfer_job(self, data_flow_id: str) -> None:
        """Recover one claimed transfer job using persisted dataflow state."""

        data_flow = await self._repository.get_by_data_flow_id(data_flow_id)
        if data_flow is None:
            await self._upsert_transfer_job(
                data_flow_id=data_flow_id,
                status=TransferJobStatus.FAILED,
                last_error=f"No data flow found for recovery for id '{data_flow_id}'.",
            )
            return

        try:
            if data_flow.state is DataFlowState.PREPARING:
                prepared_address = await self._transfer_executor.prepare(
                    data_flow,
                    self._resume_prepare_message(data_flow),
                )
                if prepared_address is not None:
                    data_flow.data_address = prepared_address

                data_flow.state = DataFlowState.PREPARED
                response = self._response_message(data_flow)
                await self._persist_transition_with_callback(
                    data_flow,
                    response,
                    callback_event_type=ControlPlaneCallbackEventType.PREPARED,
                )
                await self._upsert_transfer_job(
                    data_flow_id=data_flow.data_flow_id,
                    status=TransferJobStatus.COMPLETED,
                )
                await self._publish_state_event(data_flow)
                return

            if data_flow.state is DataFlowState.STARTING:
                started_address = await self._transfer_executor.start(
                    data_flow,
                    self._resume_start_message(data_flow),
                )
                if started_address is not None:
                    data_flow.data_address = started_address

                data_flow.state = DataFlowState.STARTED
                response = self._response_message(data_flow)
                await self._persist_transition_with_callback(
                    data_flow,
                    response,
                    callback_event_type=ControlPlaneCallbackEventType.STARTED,
                )
                if data_flow.transfer_mode is TransferMode.PUSH:
                    await self._mark_transfer_job_running(data_flow.data_flow_id)
                else:
                    await self._upsert_transfer_job(
                        data_flow_id=data_flow.data_flow_id,
                        status=TransferJobStatus.COMPLETED,
                    )
                await self._publish_state_event(data_flow)
                return

            if data_flow.state is DataFlowState.SUSPENDED:
                await self._upsert_transfer_job(
                    data_flow_id=data_flow.data_flow_id,
                    status=TransferJobStatus.PAUSED,
                )
                return

            if data_flow.state is DataFlowState.COMPLETED:
                await self._upsert_transfer_job(
                    data_flow_id=data_flow.data_flow_id,
                    status=TransferJobStatus.COMPLETED,
                )
                return

            if data_flow.state is DataFlowState.TERMINATED:
                await self._upsert_transfer_job(
                    data_flow_id=data_flow.data_flow_id,
                    status=TransferJobStatus.TERMINATED,
                )
                return

            if data_flow.state is not DataFlowState.STARTED:
                await self._upsert_transfer_job(
                    data_flow_id=data_flow.data_flow_id,
                    status=TransferJobStatus.FAILED,
                    last_error=(
                        f"Transfer job recovery unsupported for state '{data_flow.state.value}'."
                    ),
                )
                return

            if self._should_resume_via_started_notification(data_flow):
                started_message = self._resume_started_notification_message(data_flow)
                await self._transfer_executor.notify_started(data_flow, started_message)
                data_flow.metadata[_STARTED_NOTIFICATION_SEEN_METADATA_KEY] = True
                await self._repository.upsert(data_flow)
                if data_flow.transfer_mode is TransferMode.PULL:
                    await self._mark_transfer_job_running(data_flow.data_flow_id)
                else:
                    await self._upsert_transfer_job(
                        data_flow_id=data_flow.data_flow_id,
                        status=TransferJobStatus.COMPLETED,
                    )
                return

            if data_flow.transfer_mode is TransferMode.PUSH and data_flow.data_address is None:
                raise DataFlowValidationError(
                    "dataAddress is required to recover provider-push data flows."
                )

            started_address = await self._transfer_executor.start(
                data_flow,
                self._resume_start_message(data_flow),
            )
            if started_address is not None:
                data_flow.data_address = started_address
            await self._repository.upsert(data_flow)
            if data_flow.transfer_mode is TransferMode.PUSH:
                await self._mark_transfer_job_running(data_flow.data_flow_id)
            else:
                await self._upsert_transfer_job(
                    data_flow_id=data_flow.data_flow_id,
                    status=TransferJobStatus.COMPLETED,
                )
        except Exception as exc:  # noqa: BLE001
            error_message = str(exc).strip() or "Transfer recovery failed."
            logger.warning(
                "Transfer job recovery failed for dataFlowId '%s': %s",
                data_flow.data_flow_id,
                error_message,
            )
            data_flow.state = DataFlowState.TERMINATED
            response = self._response_message(data_flow, error=error_message)
            await self._persist_transition_with_callback(
                data_flow,
                response,
                callback_event_type=ControlPlaneCallbackEventType.ERRORED,
            )
            await self._upsert_transfer_job(
                data_flow_id=data_flow.data_flow_id,
                status=TransferJobStatus.FAILED,
                last_error=error_message,
            )
            await self._publish_state_event(data_flow)

    async def _stop_transfer_job_recovery_loop(self) -> None:
        """Stop transfer-job recovery loop task."""

        task = self._transfer_job_recovery_task
        if task is None:
            return
        self._transfer_job_recovery_task = None
        self._transfer_job_recovery_stop.set()
        self._transfer_job_recovery_wake.set()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    def _wake_transfer_job_recovery_loop(self) -> None:
        """Wake transfer-job loop so newly queued jobs can be recovered quickly."""

        if self._transfer_job_repository is None:
            return
        self._transfer_job_recovery_wake.set()

    async def _mark_transfer_job_running(self, data_flow_id: str) -> None:
        """Mark one transfer job as running and start lease heartbeat."""

        await self._upsert_transfer_job(
            data_flow_id=data_flow_id,
            status=TransferJobStatus.RUNNING,
        )
        await self._ensure_transfer_job_heartbeat(data_flow_id)

    async def _upsert_transfer_job(
        self,
        *,
        data_flow_id: str,
        status: TransferJobStatus,
        last_error: str | None = None,
    ) -> None:
        """Persist transfer job status when durable transfer-job storage is configured."""

        repository = self._transfer_job_repository
        if repository is None:
            return
        lease_owner = (
            self._transfer_job_lease_owner if status is TransferJobStatus.RUNNING else None
        )
        lease_seconds = (
            self._transfer_job_lease_seconds if status is TransferJobStatus.RUNNING else None
        )
        await repository.upsert_transfer_job(
            data_flow_id=data_flow_id,
            status=status,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
            last_error=last_error,
        )
        if status is not TransferJobStatus.RUNNING:
            await self._stop_transfer_job_heartbeat(data_flow_id)

    async def _ensure_transfer_job_heartbeat(self, data_flow_id: str) -> None:
        """Ensure lease-heartbeat task exists for a running transfer job."""

        if not self._service_running:
            return
        repository = self._transfer_job_repository
        if repository is None:
            return
        _ = repository
        async with self._heartbeat_tasks_lock:
            existing = self._transfer_job_heartbeat_tasks.get(data_flow_id)
            if existing is not None and not existing.done():
                return
            self._transfer_job_heartbeat_tasks[data_flow_id] = asyncio.create_task(
                self._run_transfer_job_heartbeat(data_flow_id),
                name=f"transfer-job-heartbeat-{data_flow_id}",
            )

    async def _run_transfer_job_heartbeat(self, data_flow_id: str) -> None:
        """Refresh transfer-job lease ownership while runtime job is active."""

        repository = self._transfer_job_repository
        if repository is None:
            return

        try:
            while self._service_running and not self._transfer_job_recovery_stop.is_set():
                await asyncio.sleep(self._transfer_job_heartbeat_seconds)
                renewed = await repository.renew_transfer_job_lease(
                    data_flow_id=data_flow_id,
                    lease_owner=self._transfer_job_lease_owner,
                    lease_seconds=self._transfer_job_lease_seconds,
                )
                if not renewed:
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Transfer job lease heartbeat failed for dataFlowId '%s'.",
                data_flow_id,
            )
        finally:
            async with self._heartbeat_tasks_lock:
                task = self._transfer_job_heartbeat_tasks.get(data_flow_id)
                if task is asyncio.current_task():
                    self._transfer_job_heartbeat_tasks.pop(data_flow_id, None)

    async def _stop_transfer_job_heartbeat(self, data_flow_id: str) -> None:
        """Stop one lease-heartbeat task when transfer job leaves RUNNING."""

        async with self._heartbeat_tasks_lock:
            task = self._transfer_job_heartbeat_tasks.pop(data_flow_id, None)
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _stop_all_transfer_job_heartbeats(self) -> None:
        """Stop all active transfer-job heartbeat tasks."""

        async with self._heartbeat_tasks_lock:
            tasks = list(self._transfer_job_heartbeat_tasks.values())
            self._transfer_job_heartbeat_tasks.clear()
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task


__all__ = ["CommandResult", "DataFlowService"]
