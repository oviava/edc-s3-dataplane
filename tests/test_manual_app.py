from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from simpl_bulk_manual_app.client import DataPlaneClientError
from simpl_bulk_manual_app.main import create_app


class FakeDataPlaneClient:
    def __init__(self, *_: object, **__: object) -> None:
        self.start_calls: list[tuple[str, dict[str, Any]]] = []
        self.started_calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.suspend_calls: list[tuple[str, str, str | None]] = []
        self.list_calls: list[str] = []

    async def close(self) -> None:
        return None

    async def start_dataflow(
        self,
        dataplane_url: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        self.start_calls.append((dataplane_url, payload))
        return {
            "dataFlowId": "flow-1",
            "state": "STARTED",
            "dataAddress": {
                "@type": "DataAddress",
                "endpointType": "urn:aws:s3",
                "endpoint": "s3://bucket/key",
                "endpointProperties": [],
            },
        }

    async def notify_started(
        self,
        dataplane_url: str,
        data_flow_id: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.started_calls.append((dataplane_url, data_flow_id, payload))

    async def suspend_dataflow(
        self,
        dataplane_url: str,
        data_flow_id: str,
        reason: str | None = None,
    ) -> None:
        self.suspend_calls.append((dataplane_url, data_flow_id, reason))

    async def list_dataflows(self, dataplane_url: str) -> dict[str, Any]:
        self.list_calls.append(dataplane_url)
        if "broken" in dataplane_url:
            raise DataPlaneClientError("dataplane unreachable")
        return {
            "dataplaneId": "dp-test",
            "dataFlows": [
                {
                    "dataFlowId": "flow-1",
                    "processId": "proc-1",
                    "transferType": "com.test.s3-PUSH",
                    "transferMode": "PUSH",
                    "state": "STARTED",
                    "sourceBucket": "src",
                    "sourceKey": "file.bin",
                    "destinationBucket": "dst",
                    "destinationKey": "file.bin",
                    "progress": {
                        "bytesTotal": 10,
                        "bytesTransferred": 5,
                        "percentComplete": 50.0,
                        "running": True,
                        "paused": False,
                        "finished": False,
                        "lastError": None,
                    },
                }
            ],
        }


def test_manual_ui_start_push_and_start_existing_replays_start(monkeypatch: Any) -> None:
    monkeypatch.setattr("simpl_bulk_manual_app.main.DataPlaneClient", FakeDataPlaneClient)
    app = create_app()

    with TestClient(app) as client:
        start_response = client.post(
            "/api/transfers/start",
            json={
                "dataplaneUrl": "http://localhost:8080",
                "transferMode": "PUSH",
                "sourceBucket": "source-bucket",
                "sourceKey": "payload.bin",
                "destinationBucket": "destination-bucket",
                "destinationKey": "payload-copy.bin",
                "autoStartedNotification": True,
            },
        )
        assert start_response.status_code == 200
        assert start_response.json()["dataFlowId"] == "flow-1"

        start_again_response = client.post(
            "/api/transfers/flow-1/start",
            json={"dataplaneUrl": "http://localhost:8080"},
        )
        assert start_again_response.status_code == 200

        fake_client = client.app.state.dataplane_client
        assert len(fake_client.start_calls) == 2


def test_manual_ui_query_dataflows_includes_errors(monkeypatch: Any) -> None:
    monkeypatch.setattr("simpl_bulk_manual_app.main.DataPlaneClient", FakeDataPlaneClient)
    app = create_app()

    with TestClient(app) as client:
        response = client.post(
            "/api/dataflows/query",
            json={
                "dataplaneUrls": [
                    "http://dp-a:8080",
                    "http://broken-dp:8080",
                ]
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["results"]) == 2
    assert payload["results"][0]["error"] is None
    assert payload["results"][1]["error"] is not None
