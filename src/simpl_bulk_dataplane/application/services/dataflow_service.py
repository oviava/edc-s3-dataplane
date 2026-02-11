"""Data flow use-case service."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.errors import (
    DataFlowConflictError,
    DataFlowNotFoundError,
    DataFlowValidationError,
)
from simpl_bulk_dataplane.domain.ports import (
    ControlPlaneNotifier,
    DataFlowRepository,
    TransferExecutor,
)
from simpl_bulk_dataplane.domain.signaling_models import (
    DataAddress,
    DataFlowBaseMessage,
    DataFlowPrepareMessage,
    DataFlowResponseMessage,
    DataFlowStartedNotificationMessage,
    DataFlowStartMessage,
    DataFlowStatusResponseMessage,
)
from simpl_bulk_dataplane.domain.transfer_types import (
    TERMINAL_STATES,
    DataFlowState,
    TransferMode,
    ensure_s3_transfer_type,
    parse_transfer_mode,
)


@dataclass(slots=True)
class CommandResult:
    """HTTP mapping result for command-style operations."""

    status_code: int
    body: DataFlowResponseMessage | None = None
    location: str | None = None


class DataFlowService:
    """Orchestrates state transitions for signaling commands."""

    def __init__(
        self,
        dataplane_id: str,
        repository: DataFlowRepository,
        transfer_executor: TransferExecutor,
        control_plane_notifier: ControlPlaneNotifier,
    ) -> None:
        self._dataplane_id = dataplane_id
        self._repository = repository
        self._transfer_executor = transfer_executor
        self._control_plane_notifier = control_plane_notifier

    async def prepare(self, message: DataFlowPrepareMessage) -> CommandResult:
        """Handle `/dataflows/prepare`."""

        ensure_s3_transfer_type(message.transfer_type)
        existing = await self._repository.get_by_process_id(message.process_id)
        if existing is not None:
            raise DataFlowConflictError(
                f"A data flow already exists for processId '{message.process_id}'."
            )

        transfer_mode = parse_transfer_mode(message.transfer_type)
        data_flow = self._new_data_flow(message, transfer_mode)

        if self._is_async_requested(message.metadata, "Prepare"):
            data_flow.state = DataFlowState.PREPARING
            await self._repository.upsert(data_flow)
            return CommandResult(
                status_code=202,
                body=self._response_message(data_flow),
                location=data_flow.status_path,
            )

        prepared_address = await self._transfer_executor.prepare(data_flow, message)
        if prepared_address is not None:
            data_flow.data_address = prepared_address

        data_flow.state = DataFlowState.PREPARED
        await self._repository.upsert(data_flow)
        response = self._response_message(data_flow)
        await self._control_plane_notifier.notify_prepared(data_flow, response)
        return CommandResult(status_code=200, body=response)

    async def start(self, message: DataFlowStartMessage) -> CommandResult:
        """Handle `/dataflows/start`."""

        ensure_s3_transfer_type(message.transfer_type)
        transfer_mode = parse_transfer_mode(message.transfer_type)

        existing = await self._repository.get_by_process_id(message.process_id)
        if existing is None:
            data_flow = self._new_data_flow(message, transfer_mode)
        elif existing.state in TERMINAL_STATES:
            raise DataFlowConflictError(
                f"Cannot start data flow '{existing.data_flow_id}' in state '{existing.state}'."
            )
        else:
            data_flow = existing

        if existing is not None and existing.transfer_mode is not transfer_mode:
            raise DataFlowConflictError(
                "Incoming transferType is incompatible with existing flow mode "
                f"for processId '{message.process_id}'."
            )

        effective_data_address = message.data_address
        if (
            transfer_mode is TransferMode.PUSH
            and effective_data_address is None
            and existing is not None
            and existing.data_address is not None
        ):
            # Resume on start can reuse the established push destination.
            effective_data_address = existing.data_address

        self._validate_start_data_address(transfer_mode, effective_data_address)

        if effective_data_address is not None:
            data_flow.data_address = effective_data_address

        if self._is_async_requested(message.metadata, "Start"):
            data_flow.state = DataFlowState.STARTING
            await self._repository.upsert(data_flow)
            return CommandResult(
                status_code=202,
                body=self._response_message(data_flow),
                location=data_flow.status_path,
            )

        executor_message = message
        if effective_data_address is not message.data_address:
            executor_message = message.model_copy(update={"data_address": effective_data_address})

        started_address = await self._transfer_executor.start(data_flow, executor_message)
        if started_address is not None:
            data_flow.data_address = started_address

        data_flow.state = DataFlowState.STARTED
        await self._repository.upsert(data_flow)
        response = self._response_message(data_flow)
        await self._control_plane_notifier.notify_started(data_flow, response)
        return CommandResult(status_code=200, body=response)

    async def notify_started(
        self,
        data_flow_id: str,
        message: DataFlowStartedNotificationMessage | None,
    ) -> None:
        """Handle `/dataflows/{id}/started` for consumer-side transitions."""

        data_flow = await self._get_data_flow_or_raise(data_flow_id)
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
        await self._repository.upsert(data_flow)

    async def suspend(self, data_flow_id: str, reason: str | None) -> None:
        """Handle `/dataflows/{id}/suspend`."""

        data_flow = await self._get_data_flow_or_raise(data_flow_id)
        if data_flow.state != DataFlowState.STARTED:
            raise DataFlowValidationError(
                f"Suspend is only allowed in STARTED state, got '{data_flow.state}'."
            )

        await self._transfer_executor.suspend(data_flow, reason)
        data_flow.state = DataFlowState.SUSPENDED
        await self._repository.upsert(data_flow)

    async def terminate(self, data_flow_id: str, reason: str | None) -> None:
        """Handle `/dataflows/{id}/terminate`."""

        data_flow = await self._get_data_flow_or_raise(data_flow_id)
        if data_flow.state == DataFlowState.TERMINATED:
            return

        await self._transfer_executor.terminate(data_flow, reason)
        data_flow.state = DataFlowState.TERMINATED
        await self._repository.upsert(data_flow)
        response = self._response_message(data_flow)
        await self._control_plane_notifier.notify_terminated(data_flow, response)

    async def completed(self, data_flow_id: str) -> None:
        """Handle `/dataflows/{id}/completed`."""

        data_flow = await self._get_data_flow_or_raise(data_flow_id)
        if data_flow.state == DataFlowState.COMPLETED:
            return

        await self._transfer_executor.complete(data_flow)
        data_flow.state = DataFlowState.COMPLETED
        await self._repository.upsert(data_flow)
        response = self._response_message(data_flow)
        await self._control_plane_notifier.notify_completed(data_flow, response)

    async def get_status(self, data_flow_id: str) -> DataFlowStatusResponseMessage:
        """Handle `/dataflows/{id}/status`."""

        data_flow = await self._get_data_flow_or_raise(data_flow_id)
        return DataFlowStatusResponseMessage(
            data_flow_id=data_flow.data_flow_id,
            state=data_flow.state,
        )

    def _new_data_flow(self, message: DataFlowBaseMessage, transfer_mode: TransferMode) -> DataFlow:
        """Build a new data flow aggregate from incoming request data."""

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
            metadata=dict(message.metadata),
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


__all__ = ["CommandResult", "DataFlowService"]
