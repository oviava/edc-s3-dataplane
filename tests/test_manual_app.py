from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

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


class FakeMqttDataFlowEventStore:
    def __init__(self, *_: object, **__: object) -> None:
        self.closed = False
        self._revision = 1
        self._flows = [
            {
                "dataplaneId": "dp-test",
                "dataplaneUrl": "http://dp-a:8080",
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
        ]

    def close(self) -> None:
        self.closed = True

    def snapshot(self) -> list[dict[str, object]]:
        return self._flows

    def snapshot_with_revision(self) -> tuple[int, list[dict[str, object]]]:
        return self._revision, self._flows


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


def test_manual_ui_start_push_passes_credentials_via_data_address(monkeypatch: Any) -> None:
    monkeypatch.setattr("simpl_bulk_manual_app.main.DataPlaneClient", FakeDataPlaneClient)
    app = create_app()

    with TestClient(app) as client:
        start_response = client.post(
            "/api/transfers/start",
            json={
                "dataplaneUrl": "http://localhost:8080",
                "transferMode": "PUSH",
                "sourceBucket": "bucket-a",
                "sourceKey": "100mb.bin",
                "destinationBucket": "bucket-b",
                "destinationKey": "payload-copy.bin",
                "sourceEndpointUrl": "http://minio-a:9000",
                "destinationEndpointUrl": "http://minio-b:9000",
                "sourceAccessKeyId": "minioadmin",
                "sourceSecretAccessKey": "minioadmin",
                "destinationAccessKeyId": "minioadmin",
                "destinationSecretAccessKey": "minioadmin",
                "autoStartedNotification": True,
            },
        )
        assert start_response.status_code == 200

        fake_client = client.app.state.dataplane_client
        assert len(fake_client.start_calls) == 1
        _, payload = fake_client.start_calls[0]
        metadata = payload["metadata"]

        source_data_address = metadata["sourceDataAddress"]
        assert source_data_address["endpoint"] == "s3://bucket-a/100mb.bin"
        assert {"name": "endpointUrl", "value": "http://minio-a:9000"} in source_data_address[
            "endpointProperties"
        ]
        assert {"name": "accessKeyId", "value": "minioadmin"} in source_data_address[
            "endpointProperties"
        ]
        assert {"name": "secretAccessKey", "value": "minioadmin"} in source_data_address[
            "endpointProperties"
        ]

        destination_data_address = payload["dataAddress"]
        assert destination_data_address["endpoint"] == "s3://bucket-b/payload-copy.bin"
        assert {"name": "endpointUrl", "value": "http://minio-b:9000"} in destination_data_address[
            "endpointProperties"
        ]
        assert {"name": "accessKeyId", "value": "minioadmin"} in destination_data_address[
            "endpointProperties"
        ]
        assert {"name": "secretAccessKey", "value": "minioadmin"} in destination_data_address[
            "endpointProperties"
        ]

        metadata_destination_data_address = metadata["destinationDataAddress"]
        assert metadata_destination_data_address["endpoint"] == "s3://bucket-b/payload-copy.bin"


def test_manual_ui_start_push_allows_bucket_transfer_without_keys(monkeypatch: Any) -> None:
    monkeypatch.setattr("simpl_bulk_manual_app.main.DataPlaneClient", FakeDataPlaneClient)
    app = create_app()

    with TestClient(app) as client:
        start_response = client.post(
            "/api/transfers/start",
            json={
                "dataplaneUrl": "http://localhost:8080",
                "transferMode": "PUSH",
                "sourceBucket": "source-bucket",
                "sourceKey": None,
                "destinationBucket": "destination-bucket",
                "destinationKey": None,
                "autoStartedNotification": True,
            },
        )
        assert start_response.status_code == 200

        fake_client = client.app.state.dataplane_client
        assert len(fake_client.start_calls) == 1
        _, payload = fake_client.start_calls[0]

    metadata = payload["metadata"]
    assert metadata["sourceBucket"] == "source-bucket"
    assert metadata["destinationBucket"] == "destination-bucket"
    assert "sourceKey" not in metadata
    assert "destinationKey" not in metadata
    assert payload["dataAddress"]["endpoint"] == "s3://destination-bucket"


def test_manual_ui_start_rejects_partial_credential_pairs(monkeypatch: Any) -> None:
    monkeypatch.setattr("simpl_bulk_manual_app.main.DataPlaneClient", FakeDataPlaneClient)
    app = create_app()

    with TestClient(app) as client:
        response = client.post(
            "/api/transfers/start",
            json={
                "dataplaneUrl": "http://localhost:8080",
                "transferMode": "PUSH",
                "sourceBucket": "source-bucket",
                "sourceKey": "payload.bin",
                "destinationBucket": "destination-bucket",
                "destinationKey": "payload-copy.bin",
                "sourceAccessKeyId": "only-access-key",
                "autoStartedNotification": True,
            },
        )

    assert response.status_code == 422
    assert "sourceAccessKeyId and sourceSecretAccessKey must be set together." in response.text


def test_manual_ui_query_dataflows_includes_errors(monkeypatch: Any) -> None:
    monkeypatch.setattr("simpl_bulk_manual_app.main.DataPlaneClient", FakeDataPlaneClient)
    monkeypatch.setattr("simpl_bulk_manual_app.main._mqtt_enabled", lambda: True)
    monkeypatch.setenv("SIMPL_MANUAL_MQTT_HOST", "localhost")
    monkeypatch.setattr(
        "simpl_bulk_manual_app.main.MqttDataFlowEventStore",
        FakeMqttDataFlowEventStore,
    )
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
    assert payload["results"][0]["dataplaneId"] == "dp-test"
    assert len(payload["results"][0]["dataFlows"]) == 1
    assert payload["results"][1]["error"] is None
    assert payload["results"][1]["dataFlows"] == []


def test_manual_ui_query_requires_mqtt_when_disabled(monkeypatch: Any) -> None:
    monkeypatch.setattr("simpl_bulk_manual_app.main.DataPlaneClient", FakeDataPlaneClient)
    monkeypatch.setattr("simpl_bulk_manual_app.main._mqtt_enabled", lambda: False)
    app = create_app()

    with TestClient(app) as client:
        response = client.post(
            "/api/dataflows/query",
            json={"dataplaneUrls": ["http://dp-a:8080"]},
        )

    assert response.status_code == 400
    assert "MQTT monitoring is not enabled" in response.json()["detail"]


def test_manual_ui_stream_dataflows_over_websocket(monkeypatch: Any) -> None:
    monkeypatch.setattr("simpl_bulk_manual_app.main.DataPlaneClient", FakeDataPlaneClient)
    monkeypatch.setattr("simpl_bulk_manual_app.main._mqtt_enabled", lambda: True)
    monkeypatch.setenv("SIMPL_MANUAL_MQTT_HOST", "localhost")
    monkeypatch.setattr(
        "simpl_bulk_manual_app.main.MqttDataFlowEventStore",
        FakeMqttDataFlowEventStore,
    )
    app = create_app()

    with TestClient(app) as client:
        with client.websocket_connect(
            "/ws/dataflows?dataplaneUrls=http://dp-a:8080",
        ) as websocket:
            payload = websocket.receive_json()

    assert payload["type"] == "snapshot"
    assert payload["revision"] == 1
    assert len(payload["results"]) == 1
    assert payload["results"][0]["dataplaneUrl"] == "http://dp-a:8080"
    assert len(payload["results"][0]["dataFlows"]) == 1


def test_manual_ui_stream_requires_mqtt_when_disabled(monkeypatch: Any) -> None:
    monkeypatch.setattr("simpl_bulk_manual_app.main.DataPlaneClient", FakeDataPlaneClient)
    monkeypatch.setattr("simpl_bulk_manual_app.main._mqtt_enabled", lambda: False)
    app = create_app()

    with TestClient(app) as client:
        with client.websocket_connect("/ws/dataflows") as websocket:
            payload = websocket.receive_json()

    assert payload["type"] == "error"
    assert "MQTT monitoring is not enabled" in payload["detail"]
