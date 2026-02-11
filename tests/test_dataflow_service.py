from __future__ import annotations

import asyncio

import pytest

from simpl_bulk_dataplane.application.services import DataFlowService
from simpl_bulk_dataplane.domain.errors import DataFlowValidationError
from simpl_bulk_dataplane.domain.ports import TransferExecutor
from simpl_bulk_dataplane.domain.signaling_models import (
    DataAddress,
    DataFlowPrepareMessage,
    DataFlowStartMessage,
)
from simpl_bulk_dataplane.domain.transfer_types import DataFlowState
from simpl_bulk_dataplane.infrastructure.callbacks import NoopControlPlaneNotifier
from simpl_bulk_dataplane.infrastructure.repositories import InMemoryDataFlowRepository
from simpl_bulk_dataplane.infrastructure.transfers import S3TransferExecutor


@pytest.fixture
def service() -> DataFlowService:
    return DataFlowService(
        dataplane_id="dataplane-test",
        repository=InMemoryDataFlowRepository(),
        transfer_executor=S3TransferExecutor(),
        control_plane_notifier=NoopControlPlaneNotifier(),
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
        *_: object,
        **__: object,
    ) -> None:
        return None

    async def suspend(self, *_: object, **__: object) -> None:
        return None

    async def terminate(self, *_: object, **__: object) -> None:
        return None

    async def complete(self, *_: object, **__: object) -> None:
        return None


def test_start_can_resume_push_without_repeating_data_address() -> None:
    transfer_executor = RecordingTransferExecutor()
    service = DataFlowService(
        dataplane_id="dataplane-test",
        repository=InMemoryDataFlowRepository(),
        transfer_executor=transfer_executor,
        control_plane_notifier=NoopControlPlaneNotifier(),
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

    resume = DataFlowStartMessage(
        message_id="msg-4",
        participant_id="did:web:provider",
        counter_party_id="did:web:consumer",
        dataspace_context="context-1",
        process_id="proc-resume-push",
        agreement_id="agreement-1",
        dataset_id="dataset-1",
        callback_address="https://example.com/callback",
        transfer_type="com.test.s3-PUSH",
        metadata={"sourceBucket": "src-bucket", "sourceKey": "in.csv"},
    )

    resumed_result = asyncio.run(service.start(resume))
    assert resumed_result.status_code == 200
    assert resumed_result.body is not None
    assert resumed_result.body.state is DataFlowState.STARTED
    assert len(transfer_executor.start_messages) == 2
    assert transfer_executor.start_messages[1].data_address is not None
    assert transfer_executor.start_messages[1].data_address.endpoint == "s3://dst-bucket/result.csv"
