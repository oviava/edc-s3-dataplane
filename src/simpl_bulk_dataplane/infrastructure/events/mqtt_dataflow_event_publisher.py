"""MQTT dataflow event publisher."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.monitoring_models import TransferProgressSnapshot
from simpl_bulk_dataplane.domain.ports import DataFlowEventPublisher
from simpl_bulk_dataplane.domain.transfer_types import TransferMode


class MqttDataFlowEventPublisher(DataFlowEventPublisher):
    """Publish dataflow state/progress events to MQTT topics."""

    def __init__(
        self,
        dataplane_id: str,
        broker_host: str,
        broker_port: int = 1883,
        topic_prefix: str = "simpl/dataplane",
        qos: int = 0,
        username: str | None = None,
        password: str | None = None,
        dataplane_public_url: str | None = None,
    ) -> None:
        if not broker_host.strip():
            raise ValueError("broker_host cannot be empty.")
        if qos not in {0, 1, 2}:
            raise ValueError("qos must be one of 0, 1, 2.")

        self._dataplane_id = dataplane_id
        self._dataplane_public_url = dataplane_public_url
        self._topic_prefix = topic_prefix.strip().strip("/")
        self._qos = qos

        try:
            import paho.mqtt.client as mqtt  # type: ignore[import-untyped]
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "paho-mqtt is required for MQTT dataflow events. "
                "Install project dependencies first."
            ) from exc

        try:
            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"simpl-dataplane-{dataplane_id}",
            )
        except (AttributeError, TypeError):
            client = mqtt.Client(client_id=f"simpl-dataplane-{dataplane_id}")

        if username is not None:
            client.username_pw_set(username=username, password=password)
        self._connect_with_retry(
            client=client,
            broker_host=broker_host,
            broker_port=broker_port,
        )
        client.loop_start()
        self._client = client

    async def publish_state(
        self,
        data_flow: DataFlow,
        progress: TransferProgressSnapshot | None = None,
    ) -> None:
        payload: dict[str, object] = {
            "eventType": "state",
            "timestamp": self._timestamp(),
            **self._flow_payload(data_flow),
        }
        if progress is not None:
            payload["progress"] = self._progress_payload(progress)

        topic = (
            f"{self._topic_prefix}/{self._dataplane_id}/"
            f"dataflows/{data_flow.data_flow_id}/state"
        )
        await self._publish(topic, payload)

    async def publish_progress(
        self,
        data_flow_id: str,
        progress: TransferProgressSnapshot,
        data_flow: DataFlow | None = None,
    ) -> None:
        payload: dict[str, object] = {
            "eventType": "progress",
            "timestamp": self._timestamp(),
            "dataplaneId": self._dataplane_id,
            "dataFlowId": data_flow_id,
            "progress": self._progress_payload(progress),
        }
        if self._dataplane_public_url is not None:
            payload["dataplaneUrl"] = self._dataplane_public_url
        if data_flow is not None:
            payload.update(self._flow_payload(data_flow))

        topic = f"{self._topic_prefix}/{self._dataplane_id}/dataflows/{data_flow_id}/progress"
        await self._publish(topic, payload)

    async def _publish(self, topic: str, payload: dict[str, object]) -> None:
        message = json.dumps(payload, separators=(",", ":"))
        await asyncio.to_thread(self._client.publish, topic, message, self._qos)

    def _flow_payload(self, data_flow: DataFlow) -> dict[str, object]:
        source_bucket = self._metadata_str(data_flow, "sourceBucket")
        source_key = self._metadata_str(data_flow, "sourceKey")
        destination_bucket = self._metadata_str(data_flow, "destinationBucket")
        destination_key = self._metadata_str(data_flow, "destinationKey")

        if data_flow.data_address is not None:
            bucket, key = self._bucket_key_from_endpoint(data_flow.data_address.endpoint)
            if data_flow.transfer_mode is TransferMode.PULL:
                if source_bucket is None:
                    source_bucket = bucket
                if source_key is None:
                    source_key = key
            if data_flow.transfer_mode is TransferMode.PUSH:
                if destination_bucket is None:
                    destination_bucket = bucket
                if destination_key is None:
                    destination_key = key

        payload: dict[str, object] = {
            "dataplaneId": self._dataplane_id,
            "dataFlowId": data_flow.data_flow_id,
            "processId": data_flow.process_id,
            "transferType": data_flow.transfer_type,
            "transferMode": data_flow.transfer_mode.value,
            "state": data_flow.state.value,
            "sourceBucket": source_bucket,
            "sourceKey": source_key,
            "destinationBucket": destination_bucket,
            "destinationKey": destination_key,
        }
        if self._dataplane_public_url is not None:
            payload["dataplaneUrl"] = self._dataplane_public_url
        return payload

    def _metadata_str(self, data_flow: DataFlow, key: str) -> str | None:
        value = data_flow.metadata.get(key)
        if value is None:
            return None
        return str(value)

    def _bucket_key_from_endpoint(self, endpoint: str) -> tuple[str | None, str | None]:
        parsed = urlparse(endpoint)
        if parsed.scheme.lower() == "s3":
            return parsed.netloc or None, parsed.path.lstrip("/") or None
        if parsed.scheme.lower() in {"http", "https"}:
            parts = parsed.path.lstrip("/").split("/", 1)
            if len(parts) != 2:
                return None, None
            bucket, key = parts
            return (bucket or None), (key or None)
        return None, None

    def _progress_payload(self, progress: TransferProgressSnapshot) -> dict[str, object]:
        return {
            "bytesTotal": progress.bytes_total,
            "bytesTransferred": progress.bytes_transferred,
            "percentComplete": progress.percent_complete,
            "running": progress.running,
            "queued": progress.queued,
            "paused": progress.paused,
            "finished": progress.finished,
            "lastError": progress.last_error,
        }

    def _timestamp(self) -> str:
        return datetime.now(tz=UTC).isoformat()

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


__all__ = ["MqttDataFlowEventPublisher"]
