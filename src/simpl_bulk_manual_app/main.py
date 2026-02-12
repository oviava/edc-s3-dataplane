"""Manual E2E UI app for testing dataplane transfer flows."""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from simpl_bulk_dataplane.domain.transfer_types import TransferMode
from simpl_bulk_manual_app.client import DataPlaneClient, DataPlaneClientError
from simpl_bulk_manual_app.models import (
    DataflowQueryRequest,
    DataflowQueryResponse,
    DataplanePollResult,
    PauseTransferRequest,
    StartExistingTransferRequest,
    StartTransferRequest,
    StartTransferResponse,
)
from simpl_bulk_manual_app.mqtt_events import MqttDataFlowEventStore

_PARTICIPANT_ID = "did:web:manual-provider"
_COUNTER_PARTY_ID = "did:web:manual-consumer"
_DATASPACE_CONTEXT = "manual-e2e"
_AGREEMENT_ID = "manual-agreement"
_DATASET_ID = "manual-dataset"
_CALLBACK_ADDRESS = "https://manual-ui.local/callback"


@dataclass(slots=True)
class ReplayCommand:
    """Command payloads required for starting suspended operations."""

    transfer_mode: TransferMode
    process_id: str
    latest_data_address: dict[str, Any] | None = None


def _build_resume_payload(command: ReplayCommand) -> dict[str, Any]:
    """Build resume payload for `/dataflows/{id}/resume`."""

    payload: dict[str, Any] = {
        "messageId": f"manual-resume-msg-{uuid4()}",
        "processId": command.process_id,
    }
    if command.latest_data_address is not None:
        payload["dataAddress"] = command.latest_data_address
    return payload


def _normalize_dataplane_url(raw_url: str) -> str:
    normalized = raw_url.strip().rstrip("/")
    if not normalized:
        raise ValueError("Dataplane URL cannot be empty.")
    return normalized


def _default_dataplane_urls() -> list[str]:
    raw = os.getenv("SIMPL_MANUAL_DP_URLS", "http://localhost:8080")
    values = [value.strip() for value in raw.split(",") if value.strip()]
    if not values:
        return ["http://localhost:8080"]
    # Preserve order while de-duplicating.
    deduped: list[str] = []
    for value in values:
        if value not in deduped:
            deduped.append(value)
    return deduped


def _normalize_dataplane_urls(values: list[str]) -> list[str]:
    urls: list[str] = []
    for value in values:
        normalized = _normalize_dataplane_url(value)
        if normalized not in urls:
            urls.append(normalized)
    return urls


def _mqtt_enabled() -> bool:
    raw = os.getenv("SIMPL_MANUAL_MQTT_ENABLED", "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _build_s3_data_address(
    *,
    bucket: str,
    key: str | None,
    endpoint_url: str | None = None,
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
) -> dict[str, Any]:
    """Build a DataAddress payload for S3-compatible storage."""

    endpoint_properties: list[dict[str, str]] = [{"name": "storageType", "value": "s3"}]
    if endpoint_url:
        endpoint_properties.append({"name": "endpointUrl", "value": endpoint_url})
    if access_key_id:
        endpoint_properties.append({"name": "accessKeyId", "value": access_key_id})
    if secret_access_key:
        endpoint_properties.append({"name": "secretAccessKey", "value": secret_access_key})

    endpoint = f"s3://{bucket}" if key is None else f"s3://{bucket}/{key}"
    return {
        "@type": "DataAddress",
        "endpointType": "urn:aws:s3",
        "endpoint": endpoint,
        "endpointProperties": endpoint_properties,
    }


def _build_optional_metadata_data_address(
    *,
    bucket: str,
    key: str | None,
    endpoint_url: str | None = None,
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
) -> dict[str, Any] | None:
    """Build metadata DataAddress only when extra S3 connection hints are provided."""

    if endpoint_url is None and access_key_id is None and secret_access_key is None:
        return None
    return _build_s3_data_address(
        bucket=bucket,
        key=key,
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )


def _build_start_payload(request: StartTransferRequest, process_id: str) -> dict[str, Any]:
    transfer_type = f"com.test.s3-{request.transfer_mode.value}"
    source_key = request.source_key
    destination_key = request.destination_key
    metadata: dict[str, Any] = {
        "sourceBucket": request.source_bucket,
        "destinationBucket": request.destination_bucket,
    }
    if source_key is not None:
        metadata["sourceKey"] = source_key
    if destination_key is not None:
        metadata["destinationKey"] = destination_key
    if request.source_endpoint_url:
        metadata["sourceEndpointUrl"] = request.source_endpoint_url
    if request.destination_endpoint_url:
        metadata["destinationEndpointUrl"] = request.destination_endpoint_url
    source_data_address = _build_optional_metadata_data_address(
        bucket=request.source_bucket,
        key=source_key,
        endpoint_url=request.source_endpoint_url,
        access_key_id=request.source_access_key_id,
        secret_access_key=request.source_secret_access_key,
    )
    if source_data_address is not None:
        metadata["sourceDataAddress"] = source_data_address
    destination_data_address = _build_optional_metadata_data_address(
        bucket=request.destination_bucket,
        key=destination_key,
        endpoint_url=request.destination_endpoint_url,
        access_key_id=request.destination_access_key_id,
        secret_access_key=request.destination_secret_access_key,
    )
    if destination_data_address is not None:
        metadata["destinationDataAddress"] = destination_data_address

    payload: dict[str, Any] = {
        "messageId": f"manual-msg-{uuid4()}",
        "participantId": _PARTICIPANT_ID,
        "counterPartyId": _COUNTER_PARTY_ID,
        "dataspaceContext": _DATASPACE_CONTEXT,
        "processId": process_id,
        "agreementId": _AGREEMENT_ID,
        "datasetId": _DATASET_ID,
        "callbackAddress": _CALLBACK_ADDRESS,
        "transferType": transfer_type,
        "metadata": metadata,
        "labels": ["manual-e2e"],
    }

    if request.transfer_mode is TransferMode.PUSH:
        payload["dataAddress"] = _build_s3_data_address(
            bucket=request.destination_bucket,
            key=destination_key,
            endpoint_url=request.destination_endpoint_url,
            access_key_id=request.destination_access_key_id,
            secret_access_key=request.destination_secret_access_key,
        )
    return payload


def _dataflow_results_for_urls(
    *,
    all_flows: list[dict[str, object]],
    dataplane_urls: list[str],
) -> list[DataplanePollResult]:
    results: list[DataplanePollResult] = []
    has_single_url = len(dataplane_urls) == 1
    for dataplane_url in dataplane_urls:
        matching = [
            flow
            for flow in all_flows
            if isinstance(flow.get("dataplaneUrl"), str)
            and flow.get("dataplaneUrl") == dataplane_url
        ]
        if not matching and has_single_url:
            # Allow single-target setups even when dataplaneUrl is omitted in MQTT payload.
            matching = [flow for flow in all_flows if flow.get("dataplaneUrl") is None]
            for flow in matching:
                flow["dataplaneUrl"] = dataplane_url

        dataplane_id: str | None = None
        if matching:
            raw_dataplane_id = matching[0].get("dataplaneId")
            if isinstance(raw_dataplane_id, str):
                dataplane_id = raw_dataplane_id

        results.append(
            DataplanePollResult(
                dataplane_url=dataplane_url,
                dataplane_id=dataplane_id,
                data_flows=matching,
            )
        )
    return results


@asynccontextmanager
async def _lifespan(app: FastAPI):
    app.state.dataplane_client = DataPlaneClient()
    app.state.replay_commands: dict[tuple[str, str], ReplayCommand] = {}
    app.state.default_dataplane_urls = _default_dataplane_urls()
    app.state.mqtt_event_store = None

    if _mqtt_enabled():
        mqtt_host = os.getenv("SIMPL_MANUAL_MQTT_HOST")
        if mqtt_host is None or not mqtt_host.strip():
            raise RuntimeError(
                "SIMPL_MANUAL_MQTT_HOST is required when SIMPL_MANUAL_MQTT_ENABLED=true."
            )
        app.state.mqtt_event_store = MqttDataFlowEventStore(
            broker_host=mqtt_host,
            broker_port=int(os.getenv("SIMPL_MANUAL_MQTT_PORT", "1883")),
            topic_prefix=os.getenv("SIMPL_MANUAL_MQTT_TOPIC_PREFIX", "simpl/dataplane"),
            username=os.getenv("SIMPL_MANUAL_MQTT_USERNAME"),
            password=os.getenv("SIMPL_MANUAL_MQTT_PASSWORD"),
        )
    try:
        yield
    finally:
        mqtt_event_store: MqttDataFlowEventStore | None = app.state.mqtt_event_store
        if mqtt_event_store is not None:
            mqtt_event_store.close()
        await app.state.dataplane_client.close()


def create_app() -> FastAPI:
    """Create manual UI FastAPI app."""

    app = FastAPI(title="Simpl Bulk Manual E2E UI", version="0.1.0", lifespan=_lifespan)
    static_dir = Path(__file__).resolve().parent / "static"
    static_index = static_dir / "index.html"
    app.mount("/static", StaticFiles(directory=static_dir), name="manual-ui-static")

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(static_index)

    @app.get("/api/config")
    async def get_config(request: Request) -> dict[str, object]:
        return {
            "defaultDataplaneUrls": list(request.app.state.default_dataplane_urls),
            "mqttMonitoringEnabled": request.app.state.mqtt_event_store is not None,
        }

    @app.post("/api/transfers/start", response_model=StartTransferResponse, status_code=200)
    async def start_transfer(
        body: StartTransferRequest,
        request: Request,
    ) -> StartTransferResponse:
        dataplane_url = _normalize_dataplane_url(body.dataplane_url)
        process_id = body.process_id or f"manual-process-{uuid4()}"
        start_payload = _build_start_payload(body, process_id)
        client: DataPlaneClient = request.app.state.dataplane_client

        try:
            response = await client.start_dataflow(dataplane_url, start_payload)
        except DataPlaneClientError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        raw_flow_id = response.get("dataFlowId")
        if not isinstance(raw_flow_id, str) or not raw_flow_id:
            raise HTTPException(
                status_code=500,
                detail="Dataplane /dataflows/start response missing dataFlowId.",
            )

        started_payload: dict[str, Any] | None = None
        if body.transfer_mode is TransferMode.PULL and body.auto_started_notification:
            raw_data_address = response.get("dataAddress")
            if isinstance(raw_data_address, dict):
                started_payload = {"dataAddress": raw_data_address}
                try:
                    await client.notify_started(
                        dataplane_url=dataplane_url,
                        data_flow_id=raw_flow_id,
                        payload=started_payload,
                    )
                except DataPlaneClientError as exc:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Start succeeded but /started failed: {exc}",
                    ) from exc

        latest_data_address: dict[str, Any] | None = None
        raw_response_data_address = response.get("dataAddress")
        if isinstance(raw_response_data_address, dict):
            latest_data_address = raw_response_data_address
        elif started_payload is not None:
            started_data_address = started_payload.get("dataAddress")
            if isinstance(started_data_address, dict):
                latest_data_address = started_data_address
        if latest_data_address is None:
            start_payload_data_address = start_payload.get("dataAddress")
            if isinstance(start_payload_data_address, dict):
                latest_data_address = start_payload_data_address

        request.app.state.replay_commands[(dataplane_url, raw_flow_id)] = ReplayCommand(
            transfer_mode=body.transfer_mode,
            process_id=process_id,
            latest_data_address=latest_data_address,
        )

        state = response.get("state")
        return StartTransferResponse(
            dataplane_url=dataplane_url,
            data_flow_id=raw_flow_id,
            process_id=process_id,
            state=str(state) if state is not None else "UNKNOWN",
        )

    @app.post("/api/dataflows/query", response_model=DataflowQueryResponse, status_code=200)
    async def query_dataflows(
        body: DataflowQueryRequest,
        request: Request,
    ) -> DataflowQueryResponse:
        mqtt_event_store: MqttDataFlowEventStore | None = request.app.state.mqtt_event_store
        if mqtt_event_store is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "MQTT monitoring is not enabled. Configure SIMPL_MANUAL_MQTT_ENABLED=true "
                    "and MQTT connection settings."
                ),
            )

        urls = _normalize_dataplane_urls(body.dataplane_urls)
        all_flows = mqtt_event_store.snapshot()
        results = _dataflow_results_for_urls(all_flows=all_flows, dataplane_urls=urls)
        return DataflowQueryResponse(results=results)

    @app.websocket("/ws/dataflows")
    async def stream_dataflows(websocket: WebSocket) -> None:
        await websocket.accept()
        mqtt_event_store: MqttDataFlowEventStore | None = websocket.app.state.mqtt_event_store
        if mqtt_event_store is None:
            await websocket.send_json(
                {
                    "type": "error",
                    "detail": (
                        "MQTT monitoring is not enabled. Configure "
                        "SIMPL_MANUAL_MQTT_ENABLED=true and MQTT connection settings."
                    ),
                }
            )
            await websocket.close(code=1013)
            return

        raw_urls = websocket.query_params.get("dataplaneUrls", "")
        candidate_urls = [value.strip() for value in raw_urls.split(",") if value.strip()]
        if not candidate_urls:
            candidate_urls = list(websocket.app.state.default_dataplane_urls)

        try:
            dataplane_urls = _normalize_dataplane_urls(candidate_urls)
        except ValueError as exc:
            await websocket.send_json({"type": "error", "detail": str(exc)})
            await websocket.close(code=1008)
            return

        last_revision = -1
        try:
            while True:
                revision, all_flows = mqtt_event_store.snapshot_with_revision()
                if revision != last_revision:
                    results = _dataflow_results_for_urls(
                        all_flows=all_flows,
                        dataplane_urls=dataplane_urls,
                    )
                    await websocket.send_json(
                        {
                            "type": "snapshot",
                            "revision": revision,
                            "results": [result.model_dump(by_alias=True) for result in results],
                        }
                    )
                    last_revision = revision
                await asyncio.sleep(0.5)
        except (WebSocketDisconnect, RuntimeError):
            return

    @app.post("/api/transfers/{data_flow_id}/pause", status_code=200)
    async def pause_transfer(
        data_flow_id: str,
        body: PauseTransferRequest,
        request: Request,
    ) -> dict[str, str]:
        client: DataPlaneClient = request.app.state.dataplane_client
        dataplane_url = _normalize_dataplane_url(body.dataplane_url)
        try:
            await client.suspend_dataflow(dataplane_url, data_flow_id, body.reason)
        except DataPlaneClientError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"status": "ok"}

    @app.post("/api/transfers/{data_flow_id}/start", status_code=200)
    @app.post("/api/transfers/{data_flow_id}/resume", status_code=200)
    async def resume_existing_transfer(
        data_flow_id: str,
        body: StartExistingTransferRequest,
        request: Request,
    ) -> dict[str, str]:
        """Resume a suspended transfer created via this manual UI."""

        client: DataPlaneClient = request.app.state.dataplane_client
        dataplane_url = _normalize_dataplane_url(body.dataplane_url)
        replay_commands: dict[tuple[str, str], ReplayCommand] = request.app.state.replay_commands
        command = replay_commands.get((dataplane_url, data_flow_id))
        if command is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    "No replay payload found for this flow. Start replay is only available "
                    "for flows started from this manual UI instance."
                ),
            )

        try:
            resume_response = await client.resume_dataflow(
                dataplane_url=dataplane_url,
                data_flow_id=data_flow_id,
                payload=_build_resume_payload(command),
            )
        except DataPlaneClientError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        raw_data_address = resume_response.get("dataAddress")
        if isinstance(raw_data_address, dict):
            command.latest_data_address = raw_data_address

        return {"status": "ok"}

    return app


app = create_app()


def run() -> None:
    """Run manual UI development server."""

    uvicorn.run(
        "simpl_bulk_manual_app.main:app",
        host=os.getenv("SIMPL_MANUAL_HOST", "0.0.0.0"),
        port=int(os.getenv("SIMPL_MANUAL_PORT", "8090")),
        reload=False,
    )


__all__ = ["app", "create_app", "run"]
