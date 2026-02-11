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
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse

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
    start_payload: dict[str, Any]
    pull_started_payload: dict[str, Any] | None = None


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


def _build_start_payload(request: StartTransferRequest, process_id: str) -> dict[str, Any]:
    transfer_type = f"com.test.s3-{request.transfer_mode.value}"
    metadata: dict[str, Any] = {
        "sourceBucket": request.source_bucket,
        "sourceKey": request.source_key,
        "destinationBucket": request.destination_bucket,
        "destinationKey": request.destination_key,
    }
    if request.source_endpoint_url:
        metadata["sourceEndpointUrl"] = request.source_endpoint_url
    if request.destination_endpoint_url:
        metadata["destinationEndpointUrl"] = request.destination_endpoint_url

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
        endpoint_properties: list[dict[str, str]] = [{"name": "storageType", "value": "s3"}]
        if request.destination_endpoint_url:
            endpoint_properties.append(
                {"name": "endpointUrl", "value": request.destination_endpoint_url}
            )

        payload["dataAddress"] = {
            "@type": "DataAddress",
            "endpointType": "urn:aws:s3",
            "endpoint": f"s3://{request.destination_bucket}/{request.destination_key}",
            "endpointProperties": endpoint_properties,
        }
    return payload


@asynccontextmanager
async def _lifespan(app: FastAPI):
    app.state.dataplane_client = DataPlaneClient()
    app.state.replay_commands: dict[tuple[str, str], ReplayCommand] = {}
    app.state.default_dataplane_urls = _default_dataplane_urls()
    try:
        yield
    finally:
        await app.state.dataplane_client.close()


def create_app() -> FastAPI:
    """Create manual UI FastAPI app."""

    app = FastAPI(title="Simpl Bulk Manual E2E UI", version="0.1.0", lifespan=_lifespan)
    static_index = Path(__file__).resolve().parent / "static" / "index.html"

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(static_index)

    @app.get("/api/config")
    async def get_config(request: Request) -> dict[str, object]:
        return {
            "defaultDataplaneUrls": list(request.app.state.default_dataplane_urls),
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

        request.app.state.replay_commands[(dataplane_url, raw_flow_id)] = ReplayCommand(
            transfer_mode=body.transfer_mode,
            start_payload=start_payload,
            pull_started_payload=started_payload,
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
        client: DataPlaneClient = request.app.state.dataplane_client
        urls: list[str] = []
        for value in body.dataplane_urls:
            normalized = _normalize_dataplane_url(value)
            if normalized not in urls:
                urls.append(normalized)

        async def poll_one(dataplane_url: str) -> DataplanePollResult:
            try:
                payload = await client.list_dataflows(dataplane_url)
            except DataPlaneClientError as exc:
                return DataplanePollResult(dataplane_url=dataplane_url, error=str(exc))

            dataplane_id = payload.get("dataplaneId")
            raw_data_flows = payload.get("dataFlows")
            if not isinstance(raw_data_flows, list):
                return DataplanePollResult(
                    dataplane_url=dataplane_url,
                    dataplane_id=str(dataplane_id) if dataplane_id is not None else None,
                    error="Management response missing dataFlows list.",
                )

            flows: list[dict[str, object]] = []
            for item in raw_data_flows:
                if isinstance(item, dict):
                    flows.append(item)

            return DataplanePollResult(
                dataplane_url=dataplane_url,
                dataplane_id=str(dataplane_id) if dataplane_id is not None else None,
                data_flows=flows,
            )

        results = await asyncio.gather(*(poll_one(url) for url in urls))
        return DataflowQueryResponse(results=results)

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
    async def start_existing_transfer(
        data_flow_id: str,
        body: StartExistingTransferRequest,
        request: Request,
    ) -> dict[str, str]:
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
            if command.transfer_mode is TransferMode.PUSH:
                await client.start_dataflow(dataplane_url, command.start_payload)
            else:
                started_payload = command.pull_started_payload
                if started_payload is None:
                    started_response = await client.start_dataflow(
                        dataplane_url,
                        command.start_payload,
                    )
                    raw_data_address = started_response.get("dataAddress")
                    if isinstance(raw_data_address, dict):
                        started_payload = {"dataAddress": raw_data_address}
                        command.pull_started_payload = started_payload
                await client.notify_started(dataplane_url, data_flow_id, started_payload)
        except DataPlaneClientError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

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
