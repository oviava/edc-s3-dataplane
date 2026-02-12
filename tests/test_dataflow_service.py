from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from simpl_bulk_dataplane.application.services import DataFlowService
from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.errors import DataFlowConflictError, DataFlowValidationError
from simpl_bulk_dataplane.domain.monitoring_models import TransferProgressSnapshot
from simpl_bulk_dataplane.domain.ports import TransferExecutor
from simpl_bulk_dataplane.domain.signaling_models import (
    DataAddress,
    DataFlowPrepareMessage,
    DataFlowResumeMessage,
    DataFlowStartedNotificationMessage,
    DataFlowStartMessage,
)
from simpl_bulk_dataplane.domain.transfer_types import DataFlowState
from simpl_bulk_dataplane.infrastructure.callbacks import NoopControlPlaneNotifier
from simpl_bulk_dataplane.infrastructure.events import NoopDataFlowEventPublisher
from simpl_bulk_dataplane.infrastructure.repositories import InMemoryDataFlowRepository
from simpl_bulk_dataplane.infrastructure.transfers import S3TransferExecutor


@pytest.fixture
def service() -> DataFlowService:
    return DataFlowService(
        dataplane_id="dataplane-test",
        repository=InMemoryDataFlowRepository(),
        transfer_executor=S3TransferExecutor(),
        control_plane_notifier=NoopControlPlaneNotifier(),
        dataflow_event_publisher=NoopDataFlowEventPublisher(),
    )


def prepare_message(
    process_id: str, transfer_type: str = "com.test.s3-PUSH"
) -> DataFlowPrepareMessage:
    return DataFlowPrepareMessage(
        message_id="msg-1",
        participant_id="did:web:provider",
        counter_party_id="did:web:consumer",
        dataspace_context="context-1",
        process_id=process_id,
        agreement_id="agreement-1",
        dataset_id="dataset-1",
        callback_address="https://example.com/callback",
        transfer_type=transfer_type,
        metadata={"destinationBucket": "bucket-a", "destinationKey": "payload.json"},
    )


def start_message(
    process_id: str,
    transfer_type: str = "com.test.s3-PULL",
) -> DataFlowStartMessage:
    return DataFlowStartMessage(
        message_id="msg-2",
        participant_id="did:web:provider",
        counter_party_id="did:web:consumer",
        dataspace_context="context-1",
        process_id=process_id,
        agreement_id="agreement-1",
        dataset_id="dataset-1",
        callback_address="https://example.com/callback",
        transfer_type=transfer_type,
        metadata={"sourceBucket": "bucket-src", "sourceKey": "object.csv"},
    )


def test_prepare_push_returns_prepared(service: DataFlowService) -> None:
    result = asyncio.run(service.prepare(prepare_message("proc-prepare")))

    assert result.status_code == 200
    assert result.body is not None
    assert result.body.state is DataFlowState.PREPARED
    assert result.body.data_address is not None


def test_start_pull_returns_started_with_data_address(service: DataFlowService) -> None:
    result = asyncio.run(service.start(start_message("proc-start-pull")))

    assert result.status_code == 200
    assert result.body is not None
    assert result.body.state is DataFlowState.STARTED
    assert result.body.data_address is not None
    assert result.body.data_address.endpoint.startswith("s3://")


def test_start_push_requires_data_address(service: DataFlowService) -> None:
    message = start_message("proc-start-push", transfer_type="com.test.s3-PUSH")

    with pytest.raises(DataFlowValidationError):
        asyncio.run(service.start(message))


class RecordingTransferExecutor(TransferExecutor):
    """Test double that avoids real S3 calls for service behavior checks."""

    def __init__(self) -> None:
        self.start_messages: list[DataFlowStartMessage] = []
        self.started_notifications: list[DataFlowStartedNotificationMessage | None] = []

    async def prepare(self, *_: object, **__: object) -> DataAddress | None:
        return None

    async def start(
        self,
        _data_flow: object,
        message: DataFlowStartMessage,
    ) -> DataAddress | None:
        self.start_messages.append(message)
        return None

    async def notify_started(
        self,
        _data_flow: DataFlow,
        message: DataFlowStartedNotificationMessage | None,
    ) -> None:
        self.started_notifications.append(message)
        return None

    async def suspend(self, *_: object, **__: object) -> None:
        return None

    async def terminate(self, *_: object, **__: object) -> None:
        return None

    async def complete(self, *_: object, **__: object) -> None:
        return None

    async def get_progress(self, *_: object, **__: object) -> TransferProgressSnapshot | None:
        return None


class FailingProgressTransferExecutor(RecordingTransferExecutor):
    """Test double that reports runtime transfer failure via progress."""

    def __init__(self, error_message: str) -> None:
        super().__init__()
        self._error_message = error_message
        self._progress_by_flow: dict[str, TransferProgressSnapshot] = {}

    async def start(
        self,
        data_flow: DataFlow,
        message: DataFlowStartMessage,
    ) -> DataAddress | None:
        self.start_messages.append(message)
        flow_id = data_flow.data_flow_id
        self._progress_by_flow[flow_id] = TransferProgressSnapshot(
            bytes_total=None,
            bytes_transferred=0,
            running=False,
            paused=False,
            finished=False,
            last_error=self._error_message,
        )
        return None

    async def get_progress(self, data_flow_id: str) -> TransferProgressSnapshot | None:
        return self._progress_by_flow.get(data_flow_id)


class CompletionAwareTransferExecutor(RecordingTransferExecutor):
    """Test double exposing runtime completion callback registration."""

    def __init__(self) -> None:
        super().__init__()
        self._completion_callback: Callable[[str], Awaitable[None]] | None = None

    def set_completion_callback(
        self,
        callback: Callable[[str], Awaitable[None]] | None,
    ) -> None:
        self._completion_callback = callback

    async def emit_runtime_completion(self, data_flow_id: str) -> None:
        callback = self._completion_callback
        assert callback is not None
        await callback(data_flow_id)


def test_resume_can_restart_push_without_repeating_data_address() -> None:
    transfer_executor = RecordingTransferExecutor()
    service = DataFlowService(
        dataplane_id="dataplane-test",
        repository=InMemoryDataFlowRepository(),
        transfer_executor=transfer_executor,
        control_plane_notifier=NoopControlPlaneNotifier(),
        dataflow_event_publisher=NoopDataFlowEventPublisher(),
    )

    initial = DataFlowStartMessage(
        message_id="msg-3",
        participant_id="did:web:provider",
        counter_party_id="did:web:consumer",
        dataspace_context="context-1",
        process_id="proc-resume-push",
        agreement_id="agreement-1",
        dataset_id="dataset-1",
        callback_address="https://example.com/callback",
        transfer_type="com.test.s3-PUSH",
        data_address=DataAddress(
            type_="DataAddress",
            endpoint_type="urn:aws:s3",
            endpoint="s3://dst-bucket/result.csv",
            endpoint_properties=[],
        ),
        metadata={"sourceBucket": "src-bucket", "sourceKey": "in.csv"},
    )

    first_result = asyncio.run(service.start(initial))
    assert first_result.body is not None
    flow_id = first_result.body.data_flow_id

    asyncio.run(service.suspend(flow_id, "maintenance"))

    resume = DataFlowResumeMessage(
        message_id="msg-4",
        process_id="proc-resume-push",
    )

    resumed_result = asyncio.run(service.resume(flow_id, resume))
    assert resumed_result.data_address is not None
    assert len(transfer_executor.start_messages) == 2
    assert transfer_executor.start_messages[1].data_address is not None
    assert transfer_executor.start_messages[1].data_address.endpoint == "s3://dst-bucket/result.csv"


def test_resume_pull_after_started_notification_uses_notify_started() -> None:
    transfer_executor = RecordingTransferExecutor()
    service = DataFlowService(
        dataplane_id="dataplane-test",
        repository=InMemoryDataFlowRepository(),
        transfer_executor=transfer_executor,
        control_plane_notifier=NoopControlPlaneNotifier(),
        dataflow_event_publisher=NoopDataFlowEventPublisher(),
    )

    started_result = asyncio.run(service.start(start_message("proc-resume-pull")))
    assert started_result.body is not None
    flow_id = started_result.body.data_flow_id
    source_address = DataAddress(
        type_="DataAddress",
        endpoint_type="urn:aws:s3",
        endpoint="s3://src-bucket/source.csv",
        endpoint_properties=[],
    )

    asyncio.run(
        service.notify_started(
            flow_id,
            DataFlowStartedNotificationMessage(data_address=source_address),
        )
    )
    asyncio.run(service.suspend(flow_id, "maintenance"))

    resumed_result = asyncio.run(
        service.resume(
            flow_id,
            DataFlowResumeMessage(
                message_id="msg-5",
                process_id="proc-resume-pull",
            ),
        )
    )

    assert resumed_result.data_address is not None
    assert len(transfer_executor.started_notifications) >= 2
    resume_notification = transfer_executor.started_notifications[-1]
    assert resume_notification is not None
    assert resume_notification.data_address is not None


def test_resume_requires_process_id_to_match_flow() -> None:
    transfer_executor = RecordingTransferExecutor()
    service = DataFlowService(
        dataplane_id="dataplane-test",
        repository=InMemoryDataFlowRepository(),
        transfer_executor=transfer_executor,
        control_plane_notifier=NoopControlPlaneNotifier(),
        dataflow_event_publisher=NoopDataFlowEventPublisher(),
    )

    initial = DataFlowStartMessage(
        message_id="msg-3",
        participant_id="did:web:provider",
        counter_party_id="did:web:consumer",
        dataspace_context="context-1",
        process_id="proc-resume-process-check",
        agreement_id="agreement-1",
        dataset_id="dataset-1",
        callback_address="https://example.com/callback",
        transfer_type="com.test.s3-PUSH",
        data_address=DataAddress(
            type_="DataAddress",
            endpoint_type="urn:aws:s3",
            endpoint="s3://dst-bucket/result.csv",
            endpoint_properties=[],
        ),
        metadata={"sourceBucket": "src-bucket", "sourceKey": "in.csv"},
    )
    first_result = asyncio.run(service.start(initial))
    assert first_result.body is not None
    flow_id = first_result.body.data_flow_id
    asyncio.run(service.suspend(flow_id, "maintenance"))

    with pytest.raises(DataFlowValidationError):
        asyncio.run(
            service.resume(
                flow_id,
                DataFlowResumeMessage(
                    message_id="msg-6",
                    process_id="different-process-id",
                ),
            )
        )


def test_runtime_failure_reconciles_started_flow_to_terminated() -> None:
    transfer_executor = FailingProgressTransferExecutor("Invalid endpoint: http")
    service = DataFlowService(
        dataplane_id="dataplane-test",
        repository=InMemoryDataFlowRepository(),
        transfer_executor=transfer_executor,
        control_plane_notifier=NoopControlPlaneNotifier(),
        dataflow_event_publisher=NoopDataFlowEventPublisher(),
    )

    started_result = asyncio.run(service.start(start_message("proc-runtime-error")))
    assert started_result.status_code == 200
    assert started_result.body is not None
    assert started_result.body.state is DataFlowState.STARTED
    flow_id = started_result.body.data_flow_id

    flow_info = asyncio.run(service.get_data_flow_info(flow_id))
    assert flow_info.state is DataFlowState.TERMINATED
    assert flow_info.progress.last_error == "Invalid endpoint: http"

    status = asyncio.run(service.get_status(flow_id))
    assert status.state is DataFlowState.TERMINATED


def test_start_rejects_resume_when_runtime_error_already_terminated_flow() -> None:
    transfer_executor = FailingProgressTransferExecutor("Invalid endpoint: http")
    service = DataFlowService(
        dataplane_id="dataplane-test",
        repository=InMemoryDataFlowRepository(),
        transfer_executor=transfer_executor,
        control_plane_notifier=NoopControlPlaneNotifier(),
        dataflow_event_publisher=NoopDataFlowEventPublisher(),
    )

    initial_result = asyncio.run(service.start(start_message("proc-runtime-error-restart")))
    assert initial_result.body is not None
    flow_id = initial_result.body.data_flow_id

    # Trigger reconciliation before attempting a second start.
    status = asyncio.run(service.get_status(flow_id))
    assert status.state is DataFlowState.TERMINATED

    with pytest.raises(DataFlowConflictError):
        asyncio.run(service.start(start_message("proc-runtime-error-restart")))


def test_runtime_completion_callback_marks_started_flow_completed() -> None:
    transfer_executor = CompletionAwareTransferExecutor()
    service = DataFlowService(
        dataplane_id="dataplane-test",
        repository=InMemoryDataFlowRepository(),
        transfer_executor=transfer_executor,
        control_plane_notifier=NoopControlPlaneNotifier(),
        dataflow_event_publisher=NoopDataFlowEventPublisher(),
    )

    started_result = asyncio.run(service.start(start_message("proc-runtime-complete")))
    assert started_result.status_code == 200
    assert started_result.body is not None
    flow_id = started_result.body.data_flow_id
    assert started_result.body.state is DataFlowState.STARTED

    asyncio.run(transfer_executor.emit_runtime_completion(flow_id))

    status = asyncio.run(service.get_status(flow_id))
    assert status.state is DataFlowState.COMPLETED


def test_terminate_rejects_transition_from_completed_terminal_state() -> None:
    service = DataFlowService(
        dataplane_id="dataplane-test",
        repository=InMemoryDataFlowRepository(),
        transfer_executor=RecordingTransferExecutor(),
        control_plane_notifier=NoopControlPlaneNotifier(),
        dataflow_event_publisher=NoopDataFlowEventPublisher(),
    )

    started_result = asyncio.run(service.start(start_message("proc-terminal-completed")))
    assert started_result.body is not None
    flow_id = started_result.body.data_flow_id
    asyncio.run(service.completed(flow_id))

    with pytest.raises(DataFlowConflictError):
        asyncio.run(service.terminate(flow_id, "late-terminate"))

    status = asyncio.run(service.get_status(flow_id))
    assert status.state is DataFlowState.COMPLETED


def test_completed_rejects_transition_from_terminated_terminal_state() -> None:
    service = DataFlowService(
        dataplane_id="dataplane-test",
        repository=InMemoryDataFlowRepository(),
        transfer_executor=RecordingTransferExecutor(),
        control_plane_notifier=NoopControlPlaneNotifier(),
        dataflow_event_publisher=NoopDataFlowEventPublisher(),
    )

    started_result = asyncio.run(service.start(start_message("proc-terminal-terminated")))
    assert started_result.body is not None
    flow_id = started_result.body.data_flow_id
    asyncio.run(service.terminate(flow_id, "manual-stop"))

    with pytest.raises(DataFlowConflictError):
        asyncio.run(service.completed(flow_id))

    status = asyncio.run(service.get_status(flow_id))
    assert status.state is DataFlowState.TERMINATED
