"""MQTT subscriber that stores latest dataflow events."""

from __future__ import annotations

import json
import threading
import time
from copy import deepcopy
from typing import Any


class MqttDataFlowEventStore:
    """Consume dataplane MQTT events and keep latest flow snapshots in memory."""

    def __init__(
        self,
        broker_host: str,
        broker_port: int = 1883,
        topic_prefix: str = "simpl/dataplane",
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        if not broker_host.strip():
            raise ValueError("broker_host cannot be empty.")

        try:
            import paho.mqtt.client as mqtt  # type: ignore[import-untyped]
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "paho-mqtt is required for MQTT monitoring. Install project dependencies first."
            ) from exc

        self._lock = threading.Lock()
        self._flows: dict[tuple[str, str], dict[str, object]] = {}
        self._revision = 0

        try:
            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id="simpl-manual-ui-subscriber",
            )
        except (AttributeError, TypeError):
            client = mqtt.Client(client_id="simpl-manual-ui-subscriber")

        if username is not None:
            client.username_pw_set(username=username, password=password)
        client.on_message = self._on_message
        self._connect_with_retry(
            client=client,
            broker_host=broker_host,
            broker_port=broker_port,
        )
        client.subscribe(f"{topic_prefix.strip().strip('/')}/+/dataflows/+/+")
        client.loop_start()
        self._client = client

    def close(self) -> None:
        """Stop MQTT background loop."""

        self._client.loop_stop()
        self._client.disconnect()

    def snapshot(self) -> list[dict[str, object]]:
        """Return latest flow snapshots."""

        with self._lock:
            return [deepcopy(value) for value in self._flows.values()]

    def snapshot_with_revision(self) -> tuple[int, list[dict[str, object]]]:
        """Return snapshots with a revision for change tracking."""

        with self._lock:
            return self._revision, [deepcopy(value) for value in self._flows.values()]

    def _on_message(
        self,
        _client: object,
        _userdata: object,
        message: Any,
    ) -> None:
        try:
            payload = json.loads(message.payload.decode("utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return

        dataplane_id = payload.get("dataplaneId")
        data_flow_id = payload.get("dataFlowId")
        if not isinstance(dataplane_id, str) or not dataplane_id:
            return
        if not isinstance(data_flow_id, str) or not data_flow_id:
            return

        key = (dataplane_id, data_flow_id)
        with self._lock:
            current = self._flows.get(
                key,
                {
                    "dataplaneId": dataplane_id,
                    "dataFlowId": data_flow_id,
                    "progress": {
                        "bytesTotal": None,
                        "bytesTransferred": 0,
                        "percentComplete": None,
                        "running": False,
                        "paused": False,
                        "finished": False,
                        "lastError": None,
                    },
                },
            )

            for field in (
                "dataplaneUrl",
                "processId",
                "transferType",
                "transferMode",
                "state",
                "sourceBucket",
                "sourceKey",
                "destinationBucket",
                "destinationKey",
            ):
                if field in payload:
                    current[field] = payload[field]

            progress = payload.get("progress")
            if isinstance(progress, dict):
                current_progress = current.get("progress")
                if not isinstance(current_progress, dict):
                    current_progress = {}
                    current["progress"] = current_progress
                for field in (
                    "bytesTotal",
                    "bytesTransferred",
                    "percentComplete",
                    "running",
                    "paused",
                    "finished",
                    "lastError",
                ):
                    if field in progress:
                        current_progress[field] = progress[field]

            if "timestamp" in payload:
                current["updatedAt"] = payload["timestamp"]

            self._flows[key] = current
            self._revision += 1

    def _connect_with_retry(
        self,
        client: Any,
        broker_host: str,
        broker_port: int,
        max_attempts: int = 20,
    ) -> None:
        """Connect to MQTT broker with bounded retry/backoff."""

        delay_seconds = 0.5
        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                client.connect(host=broker_host, port=broker_port, keepalive=60)
                return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt == max_attempts:
                    break
                time.sleep(delay_seconds)
                delay_seconds = min(3.0, delay_seconds * 1.5)

        raise RuntimeError(
            f"Failed to connect to MQTT broker {broker_host}:{broker_port} "
            f"after {max_attempts} attempts."
        ) from last_error


__all__ = ["MqttDataFlowEventStore"]
