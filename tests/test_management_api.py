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
            "destinationBucket": "bucket-dst",
            "destinationKey": "payload.bin",
            "sourceBucket": "bucket-src",
            "sourceKey": "source.bin",
        },
        "labels": ["gold"],
    }


def test_management_lists_dataflows_with_progress_fields() -> None:
    get_dataflow_service.cache_clear()
    get_settings.cache_clear()

    push_payload = _base_payload(f"proc-push-{uuid4()}", "com.test.s3-PUSH")
    pull_payload = _base_payload(f"proc-pull-{uuid4()}", "com.test.s3-PULL")

    with TestClient(app) as client:
        prepare_response = client.post("/dataflows/prepare", json=push_payload)
        assert prepare_response.status_code == 200

        start_response = client.post("/dataflows/start", json=pull_payload)
        assert start_response.status_code == 200

        response = client.get("/management/dataflows")

    assert response.status_code == 200
    body = response.json()
    assert body["dataplaneId"] == "dataplane-local"

    flows_by_mode = {flow["transferMode"]: flow for flow in body["dataFlows"]}
    assert "PUSH" in flows_by_mode
    assert "PULL" in flows_by_mode

    push_flow = flows_by_mode["PUSH"]
    assert push_flow["destinationBucket"] == "bucket-dst"
    assert push_flow["destinationKey"] == "payload.bin"
    assert push_flow["progress"]["bytesTransferred"] == 0

    pull_flow = flows_by_mode["PULL"]
    assert pull_flow["sourceBucket"] == "bucket-src"
    assert pull_flow["sourceKey"] == "source.bin"
    assert "percentComplete" in pull_flow["progress"]


def test_management_list_filters_by_transfer_mode() -> None:
    get_dataflow_service.cache_clear()
    get_settings.cache_clear()

    push_payload = _base_payload(f"proc-push-{uuid4()}", "com.test.s3-PUSH")
    pull_payload = _base_payload(f"proc-pull-{uuid4()}", "com.test.s3-PULL")

    with TestClient(app) as client:
        assert client.post("/dataflows/prepare", json=push_payload).status_code == 200
        assert client.post("/dataflows/start", json=pull_payload).status_code == 200

        push_only = client.get("/management/dataflows", params={"mode": "PUSH"})
        pull_only = client.get("/management/dataflows", params={"mode": "PULL"})

    assert push_only.status_code == 200
    assert [flow["transferMode"] for flow in push_only.json()["dataFlows"]] == ["PUSH"]

    assert pull_only.status_code == 200
    assert [flow["transferMode"] for flow in pull_only.json()["dataFlows"]] == ["PULL"]


def test_management_detail_reports_completed_as_100_percent() -> None:
    get_dataflow_service.cache_clear()
    get_settings.cache_clear()

    push_payload = _base_payload(f"proc-complete-{uuid4()}", "com.test.s3-PUSH")

    with TestClient(app) as client:
        prepare_response = client.post("/dataflows/prepare", json=push_payload)
        assert prepare_response.status_code == 200
        flow_id = prepare_response.json()["dataFlowId"]

        completed_response = client.post(f"/dataflows/{flow_id}/completed")
        assert completed_response.status_code == 200

        detail_response = client.get(f"/management/dataflows/{flow_id}")

    assert detail_response.status_code == 200
    body = detail_response.json()
    assert body["state"] == "COMPLETED"
    assert body["progress"]["percentComplete"] == 100.0

