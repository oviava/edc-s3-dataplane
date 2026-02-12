from __future__ import annotations

import asyncio

from simpl_bulk_dataplane.application.services import DataFlowService
from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.monitoring_models import TransferProgressSnapshot
from simpl_bulk_dataplane.domain.ports import TransferExecutor
from simpl_bulk_dataplane.domain.signaling_models import (
    DataAddress,
    DataFlowPrepareMessage,
    DataFlowStartedNotificationMessage,
    DataFlowStartMessage,
)
from simpl_bulk_dataplane.domain.transfer_jobs import TransferJobStatus
from simpl_bulk_dataplane.domain.transfer_types import DataFlowState, TransferMode
from simpl_bulk_dataplane.infrastructure.callbacks import NoopControlPlaneNotifier
from simpl_bulk_dataplane.infrastructure.events import NoopDataFlowEventPublisher
from simpl_bulk_dataplane.infrastructure.repositories import InMemoryDataFlowRepository


class RecordingTransferExecutor(TransferExecutor):
    """Transfer-executor double that records start invocations."""

    def __init__(self) -> None:
        self.start_messages: list[DataFlowStartMessage] = []

    async def prepare(
        self,
        _data_flow: DataFlow,
        _message: DataFlowPrepareMessage,
    ) -> DataAddress | None:
        return None

    async def start(
        self,
        _data_flow: DataFlow,
        message: DataFlowStartMessage,
    ) -> DataAddress | None:
        self.start_messages.append(message)
        return None

    async def notify_started(
        self,
        _data_flow: DataFlow,
        _message: DataFlowStartedNotificationMessage | None,
    ) -> None:
        return None

    async def suspend(self, _data_flow: DataFlow, _reason: str | None) -> None:
        return None

    async def terminate(self, _data_flow: DataFlow, _reason: str | None) -> None:
        return None

    async def complete(self, _data_flow: DataFlow) -> None:
        return None

    async def get_progress(self, _data_flow_id: str) -> TransferProgressSnapshot | None:
        return None


def _build_started_push_flow() -> DataFlow:
    return DataFlow(
        data_flow_id="flow-recovery",
        process_id="proc-recovery",
        transfer_type="com.test.s3-PUSH",
        transfer_mode=TransferMode.PUSH,
        participant_id="did:web:provider",
        counter_party_id="did:web:consumer",
        dataspace_context="context-1",
        agreement_id="agreement-1",
        dataset_id="dataset-1",
        callback_address="https://example.com/callback",
        state=DataFlowState.STARTED,
        data_address=DataAddress(
            type_="DataAddress",
            endpoint_type="urn:aws:s3",
            endpoint="s3://dst-bucket/result.csv",
            endpoint_properties=[],
        ),
        metadata={
            "sourceBucket": "src-bucket",
            "sourceKey": "source.csv",
            "destinationBucket": "dst-bucket",
            "destinationKey": "result.csv",
            "_dpsFlowOrigin": "start",
        },
    )


def test_in_memory_transfer_job_claims_and_renews_lease() -> None:
    repository = InMemoryDataFlowRepository()

    async def scenario() -> None:
        await repository.upsert_transfer_job(
            data_flow_id="flow-1",
            status=TransferJobStatus.QUEUED,
        )

        claimed = await repository.claim_due_transfer_jobs(
            lease_owner="worker-a",
            limit=10,
            lease_seconds=30,
        )
        assert len(claimed) == 1
        assert claimed[0].data_flow_id == "flow-1"
        assert claimed[0].status is TransferJobStatus.RUNNING
        assert claimed[0].attempt == 1

        renewed = await repository.renew_transfer_job_lease(
            data_flow_id="flow-1",
            lease_owner="worker-a",
            lease_seconds=30,
        )
        assert renewed is True

        wrong_owner = await repository.renew_transfer_job_lease(
            data_flow_id="flow-1",
            lease_owner="worker-b",
            lease_seconds=30,
        )
        assert wrong_owner is False

    asyncio.run(scenario())


def test_startup_recovers_running_transfer_job() -> None:
    repository = InMemoryDataFlowRepository()
    transfer_executor = RecordingTransferExecutor()
    service = DataFlowService(
        dataplane_id="dataplane-test",
        repository=repository,
        transfer_executor=transfer_executor,
        control_plane_notifier=NoopControlPlaneNotifier(),
        dataflow_event_publisher=NoopDataFlowEventPublisher(),
        transfer_job_recovery_poll_seconds=0.05,
        transfer_job_recovery_batch_size=10,
        transfer_job_lease_seconds=1.0,
        transfer_job_heartbeat_seconds=0.2,
    )

    async def scenario() -> None:
        flow = _build_started_push_flow()
        await repository.upsert(flow)
        await repository.upsert_transfer_job(
            data_flow_id=flow.data_flow_id,
            status=TransferJobStatus.RUNNING,
            lease_owner=None,
            lease_seconds=None,
        )

        await service.startup()
        try:
            async def _wait_for_recovery_start() -> None:
                for _ in range(50):
                    if transfer_executor.start_messages:
                        return
                    await asyncio.sleep(0.02)
                raise AssertionError("Recovery did not re-dispatch transfer start.")

            await _wait_for_recovery_start()
        finally:
            await service.shutdown()

    asyncio.run(scenario())

    assert len(transfer_executor.start_messages) == 1
    assert transfer_executor.start_messages[0].process_id == "proc-recovery"
