from __future__ import annotations

import asyncio

import pytest

from simpl_bulk_dataplane.application.services import DataFlowService
from simpl_bulk_dataplane.domain.errors import DataFlowValidationError
from simpl_bulk_dataplane.domain.signaling_models import (
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
