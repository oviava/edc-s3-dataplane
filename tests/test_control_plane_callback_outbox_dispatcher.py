from __future__ import annotations

import asyncio
import time

import httpx

from simpl_bulk_dataplane.domain.callbacks import (
    ControlPlaneCallbackEvent,
    ControlPlaneCallbackEventType,
)
from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.signaling_models import DataFlowResponseMessage
from simpl_bulk_dataplane.domain.transfer_types import DataFlowState, TransferMode
from simpl_bulk_dataplane.infrastructure.callbacks import ControlPlaneCallbackOutboxDispatcher
from simpl_bulk_dataplane.infrastructure.control_plane import ControlPlaneClient
from simpl_bulk_dataplane.infrastructure.repositories import InMemoryDataFlowRepository


def _data_flow(callback_address: str | None) -> DataFlow:
    return DataFlow(
        data_flow_id="flow-1",
        process_id="transfer-123",
        transfer_type="com.test.s3-PUSH",
        transfer_mode=TransferMode.PUSH,
        participant_id="did:web:provider",
        counter_party_id="did:web:consumer",
        dataspace_context="ctx-1",
        agreement_id="agreement-1",
        dataset_id="dataset-1",
        callback_address=callback_address,
        state=DataFlowState.STARTED,
    )


def _response_message() -> DataFlowResponseMessage:
    return DataFlowResponseMessage(
        dataplane_id="dataplane-local",
        data_flow_id="flow-1",
        state=DataFlowState.STARTED,
    )


def test_dispatcher_delivers_due_outbox_events() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code=200)

    repository = InMemoryDataFlowRepository()
    data_flow = _data_flow("https://callbacks.example.com/provider-callback")
    event = ControlPlaneCallbackEvent(
        process_id=data_flow.process_id,
        data_flow_id=data_flow.data_flow_id,
        event_type=ControlPlaneCallbackEventType.STARTED,
        callback_address=data_flow.callback_address,
        payload=_response_message(),
    )

    asyncio.run(repository.persist_transition(data_flow, event))

    dispatcher = ControlPlaneCallbackOutboxDispatcher(
        outbox_repository=repository,
        control_plane_client=ControlPlaneClient(
            base_url="https://controlplane.example.com/signaling/v1",
            transport=httpx.MockTransport(handler),
        ),
        retry_jitter_ratio=0.0,
    )

    processed = asyncio.run(dispatcher.dispatch_due_events_once())
    assert processed == 1
    assert len(requests) == 1
    assert (
        str(requests[0].url)
        == "https://callbacks.example.com/provider-callback/transfers/transfer-123/dataflow/started"
    )

    processed_again = asyncio.run(dispatcher.dispatch_due_events_once())
    assert processed_again == 0


def test_dispatcher_retries_failed_outbox_events() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(status_code=503, json={"detail": "unavailable"})
        return httpx.Response(status_code=200)

    repository = InMemoryDataFlowRepository()
    data_flow = _data_flow("https://callbacks.example.com/provider-callback")
    event = ControlPlaneCallbackEvent(
        process_id=data_flow.process_id,
        data_flow_id=data_flow.data_flow_id,
        event_type=ControlPlaneCallbackEventType.STARTED,
        callback_address=data_flow.callback_address,
        payload=_response_message(),
    )

    asyncio.run(repository.persist_transition(data_flow, event))

    dispatcher = ControlPlaneCallbackOutboxDispatcher(
        outbox_repository=repository,
        control_plane_client=ControlPlaneClient(
            base_url="https://controlplane.example.com/signaling/v1",
            transport=httpx.MockTransport(handler),
        ),
        retry_base_delay_seconds=0.01,
        retry_max_delay_seconds=0.02,
        retry_jitter_ratio=0.0,
        lease_seconds=0.01,
    )

    first_processed = asyncio.run(dispatcher.dispatch_due_events_once())
    assert first_processed == 1
    assert len(requests) == 1

    immediate_retry = asyncio.run(dispatcher.dispatch_due_events_once())
    assert immediate_retry == 0
    assert len(requests) == 1

    time.sleep(0.03)

    retried = asyncio.run(dispatcher.dispatch_due_events_once())
    assert retried == 1
    assert len(requests) == 2
