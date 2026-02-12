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


def test_resume_endpoint_resumes_suspended_flow() -> None:
    get_dataflow_service.cache_clear()
    get_settings.cache_clear()

    process_id = f"proc-{uuid4()}"
    payload = _base_payload(process_id, "com.test.s3-PULL")

    with TestClient(app) as client:
        start_response = client.post("/dataflows/start", json=payload)
        assert start_response.status_code == 200
        flow_id = start_response.json()["dataFlowId"]

        suspend_response = client.post(
            f"/dataflows/{flow_id}/suspend",
            json={"reason": "pause-for-resume"},
        )
        assert suspend_response.status_code == 200

        resume_response = client.post(
            f"/dataflows/{flow_id}/resume",
            json={
                "messageId": f"resume-{uuid4()}",
                "processId": process_id,
            },
        )

    assert resume_response.status_code == 200
    body = resume_response.json()
    assert "dataAddress" in body
    assert body["dataAddress"]["endpoint"] == "s3://bucket-b/object.csv"


def test_resume_endpoint_requires_matching_process_id() -> None:
    get_dataflow_service.cache_clear()
    get_settings.cache_clear()

    process_id = f"proc-{uuid4()}"
    payload = _base_payload(process_id, "com.test.s3-PULL")

    with TestClient(app) as client:
        start_response = client.post("/dataflows/start", json=payload)
        assert start_response.status_code == 200
        flow_id = start_response.json()["dataFlowId"]
        assert client.post(f"/dataflows/{flow_id}/suspend").status_code == 200

        resume_response = client.post(
            f"/dataflows/{flow_id}/resume",
            json={
                "messageId": f"resume-{uuid4()}",
                "processId": f"different-{uuid4()}",
            },
        )

    assert resume_response.status_code == 400
    assert "processId" in resume_response.json()["detail"]
