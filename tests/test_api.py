from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from simpl_bulk_dataplane.api.dependencies import get_dataflow_service, get_settings
from simpl_bulk_dataplane.main import app


def _base_payload(process_id: str, transfer_type: str) -> dict[str, object]:
    return {
        "messageId": f"msg-{uuid4()}",
        "participantId": "did:web:provider",
        "counterPartyId": "did:web:consumer",
        "dataspaceContext": "context-1",
        "processId": process_id,
        "agreementId": "agreement-1",
        "datasetId": "dataset-1",
        "callbackAddress": "https://example.com/callback",
        "transferType": transfer_type,
        "metadata": {
            "destinationBucket": "bucket-a",
            "destinationKey": "payload.json",
            "sourceBucket": "bucket-b",
            "sourceKey": "object.csv",
        },
        "labels": ["gold"],
    }


def test_healthz() -> None:
    with TestClient(app) as client:
        response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_prepare_endpoint_returns_prepared() -> None:
    get_dataflow_service.cache_clear()
    get_settings.cache_clear()

    payload = _base_payload(f"proc-{uuid4()}", "com.test.s3-PUSH")
    with TestClient(app) as client:
        response = client.post("/dataflows/prepare", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["state"] == "PREPARED"
    assert body["dataplaneId"] == "dataplane-local"
    assert "dataFlowId" in body


def test_prepare_without_callback_address_returns_422() -> None:
    get_dataflow_service.cache_clear()
    get_settings.cache_clear()

    payload = _base_payload(f"proc-{uuid4()}", "com.test.s3-PUSH")
    payload.pop("callbackAddress")
    with TestClient(app) as client:
        response = client.post("/dataflows/prepare", json=payload)

    assert response.status_code == 422


def test_start_push_without_data_address_returns_400() -> None:
    get_dataflow_service.cache_clear()
    get_settings.cache_clear()

    payload = _base_payload(f"proc-{uuid4()}", "com.test.s3-PUSH")
    with TestClient(app) as client:
        response = client.post("/dataflows/start", json=payload)

    assert response.status_code == 400
    assert "dataAddress is required" in response.json()["detail"]


def test_start_without_callback_address_returns_422() -> None:
    get_dataflow_service.cache_clear()
    get_settings.cache_clear()

    payload = _base_payload(f"proc-{uuid4()}", "com.test.s3-PUSH")
    payload.pop("callbackAddress")
    with TestClient(app) as client:
        response = client.post("/dataflows/start", json=payload)

    assert response.status_code == 422
