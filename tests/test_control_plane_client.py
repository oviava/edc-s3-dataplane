from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from simpl_bulk_dataplane.domain.signaling_models import (
    DataFlowResponseMessage,
    DataPlaneRegistrationMessage,
)
from simpl_bulk_dataplane.domain.transfer_types import DataFlowState
from simpl_bulk_dataplane.infrastructure.control_plane import (
    ControlPlaneClient,
    ControlPlaneClientError,
)


def test_register_dataplane_calls_openapi_registration_endpoint() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code=200)

    client = ControlPlaneClient(
        base_url="https://controlplane.example.com/signaling/v1",
        transport=httpx.MockTransport(handler),
    )
    client.register_dataplane(
        DataPlaneRegistrationMessage(
            dataplane_id="dataplane-local",
            name="Dataplane",
            endpoint="https://dataplane.example.com/signaling/v1",
            transfer_types=["com.test.s3-PUSH", "com.test.s3-PULL"],
        )
    )

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert str(request.url) == "https://controlplane.example.com/signaling/v1/dataplanes/register"
    payload = json.loads(request.content.decode())
    assert payload["dataplaneId"] == "dataplane-local"
    assert payload["transferTypes"] == ["com.test.s3-PUSH", "com.test.s3-PULL"]


@pytest.mark.parametrize(
    ("method_name", "state_path"),
    [
        ("signal_prepared", "prepared"),
        ("signal_started", "started"),
        ("signal_completed", "completed"),
        ("signal_errored", "errored"),
    ],
)
def test_signal_methods_call_openapi_callback_paths(method_name: str, state_path: str) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code=200)

    client = ControlPlaneClient(
        base_url="https://controlplane.example.com/signaling/v1",
        transport=httpx.MockTransport(handler),
    )
    message = DataFlowResponseMessage(
        dataplane_id="dataplane-local",
        data_flow_id="flow-1",
        state=DataFlowState.STARTED,
    )

    method = getattr(client, method_name)
    asyncio.run(method("transfer-123", message))

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert str(request.url).endswith(f"/transfers/transfer-123/dataflow/{state_path}")
    payload = json.loads(request.content.decode())
    assert payload["dataplaneId"] == "dataplane-local"


def test_register_dataplane_raises_descriptive_error_for_non_success_status() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code=404, json={"detail": "transfer not found"})

    client = ControlPlaneClient(
        base_url="https://controlplane.example.com/signaling/v1",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(ControlPlaneClientError, match="404 transfer not found"):
        client.register_dataplane(
            DataPlaneRegistrationMessage(
                dataplane_id="dataplane-local",
                name="Dataplane",
                endpoint="https://dataplane.example.com/signaling/v1",
                transfer_types=["com.test.s3-PUSH"],
            )
        )


def test_signal_methods_use_callback_base_url_override() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code=200)

    client = ControlPlaneClient(
        base_url="https://controlplane.example.com/signaling/v1",
        transport=httpx.MockTransport(handler),
    )
    message = DataFlowResponseMessage(
        dataplane_id="dataplane-local",
        data_flow_id="flow-1",
        state=DataFlowState.STARTED,
    )

    asyncio.run(
        client.signal_started(
            "transfer-123",
            message,
            callback_base_url="https://callbacks.example.com/custom",
        )
    )

    assert len(requests) == 1
    assert (
        str(requests[0].url)
        == "https://callbacks.example.com/custom/transfers/transfer-123/dataflow/started"
    )


def test_signal_methods_fall_back_to_default_base_url_when_callback_override_is_blank() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code=200)

    client = ControlPlaneClient(
        base_url="https://controlplane.example.com/signaling/v1",
        transport=httpx.MockTransport(handler),
    )
    message = DataFlowResponseMessage(
        dataplane_id="dataplane-local",
        data_flow_id="flow-1",
        state=DataFlowState.STARTED,
    )

    asyncio.run(
        client.signal_started(
            "transfer-123",
            message,
            callback_base_url="  ",
        )
    )

    assert len(requests) == 1
    assert (
        str(requests[0].url)
        == "https://controlplane.example.com/signaling/v1/transfers/transfer-123/dataflow/started"
    )
