from __future__ import annotations

import asyncio

import httpx

from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.signaling_models import DataFlowResponseMessage
from simpl_bulk_dataplane.domain.transfer_types import DataFlowState, TransferMode
from simpl_bulk_dataplane.infrastructure.callbacks import HttpControlPlaneNotifier
from simpl_bulk_dataplane.infrastructure.control_plane import ControlPlaneClient


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
    )


def _response_message() -> DataFlowResponseMessage:
    return DataFlowResponseMessage(
        dataplane_id="dataplane-local",
        data_flow_id="flow-1",
        state=DataFlowState.STARTED,
    )


def test_http_notifier_routes_callbacks_to_data_flow_callback_address() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code=200)

    client = ControlPlaneClient(
        base_url="https://controlplane.example.com/signaling/v1",
        transport=httpx.MockTransport(handler),
    )
    notifier = HttpControlPlaneNotifier(client)

    asyncio.run(
        notifier.notify_started(
            _data_flow("https://callbacks.example.com/provider-callback"),
            _response_message(),
        )
    )

    assert len(requests) == 1
    assert (
        str(requests[0].url)
        == "https://callbacks.example.com/provider-callback/transfers/transfer-123/dataflow/started"
    )


def test_http_notifier_falls_back_to_client_base_url_when_callback_address_is_missing() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code=200)

    client = ControlPlaneClient(
        base_url="https://controlplane.example.com/signaling/v1",
        transport=httpx.MockTransport(handler),
    )
    notifier = HttpControlPlaneNotifier(client)

    asyncio.run(notifier.notify_started(_data_flow(None), _response_message()))

    assert len(requests) == 1
    assert (
        str(requests[0].url)
        == "https://controlplane.example.com/signaling/v1/transfers/transfer-123/dataflow/started"
    )
