"""Data plane signaling routes."""

from __future__ import annotations

from typing import NoReturn

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Response
from fastapi.responses import JSONResponse

from simpl_bulk_dataplane.api.dependencies import get_dataflow_service
from simpl_bulk_dataplane.application.services import DataFlowService
from simpl_bulk_dataplane.domain.errors import (
    DataFlowConflictError,
    DataFlowNotFoundError,
    DataFlowValidationError,
    UnsupportedTransferTypeError,
)
from simpl_bulk_dataplane.domain.signaling_models import (
    DataFlowPrepareMessage,
    DataFlowResponseMessage,
    DataFlowStartedNotificationMessage,
    DataFlowStartMessage,
    DataFlowStatusResponseMessage,
    DataFlowSuspendMessage,
    DataFlowTerminateMessage,
)

router = APIRouter(tags=["data-plane endpoints"])


def _raise_http_exception(exc: Exception) -> NoReturn:
    if isinstance(exc, DataFlowNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    validation_errors = (
        DataFlowValidationError | UnsupportedTransferTypeError | DataFlowConflictError
    )
    if isinstance(exc, validation_errors):
        raise HTTPException(status_code=400, detail=str(exc))
    raise HTTPException(status_code=500, detail="Unexpected data flow error")


@router.post(
    "/dataflows/prepare",
    response_model=DataFlowResponseMessage,
    responses={202: {"model": DataFlowResponseMessage}, 400: {"description": "Bad request"}},
)
async def prepare_data_flow(
    message: DataFlowPrepareMessage,
    service: DataFlowService = Depends(get_dataflow_service),
) -> JSONResponse:
    """Prepare a data flow and its S3 transfer resources."""

    try:
        result = await service.prepare(message)
    except Exception as exc:  # noqa: BLE001
        _raise_http_exception(exc)

    assert result.body is not None
    headers = {} if result.location is None else {"Location": result.location}
    return JSONResponse(
        status_code=result.status_code,
        content=result.body.model_dump(by_alias=True, exclude_none=True),
        headers=headers,
    )


@router.post(
    "/dataflows/start",
    response_model=DataFlowResponseMessage,
    responses={202: {"model": DataFlowResponseMessage}, 400: {"description": "Bad request"}},
)
async def start_data_flow(
    message: DataFlowStartMessage,
    service: DataFlowService = Depends(get_dataflow_service),
) -> JSONResponse:
    """Start a provider-side data flow."""

    try:
        result = await service.start(message)
    except Exception as exc:  # noqa: BLE001
        _raise_http_exception(exc)

    assert result.body is not None
    headers = {} if result.location is None else {"Location": result.location}
    return JSONResponse(
        status_code=result.status_code,
        content=result.body.model_dump(by_alias=True, exclude_none=True),
        headers=headers,
    )


@router.post("/dataflows/{id}/started", status_code=200)
async def notify_started(
    id: str = Path(...),
    message: DataFlowStartedNotificationMessage | None = Body(default=None),
    service: DataFlowService = Depends(get_dataflow_service),
) -> Response:
    """Mark a consumer-side transfer as started."""

    try:
        await service.notify_started(id, message)
    except Exception as exc:  # noqa: BLE001
        _raise_http_exception(exc)

    return Response(status_code=200)


@router.post("/dataflows/{id}/suspend", status_code=200)
async def suspend_data_flow(
    id: str = Path(...),
    message: DataFlowSuspendMessage | None = Body(default=None),
    service: DataFlowService = Depends(get_dataflow_service),
) -> Response:
    """Suspend an active data flow."""

    try:
        await service.suspend(id, None if message is None else message.reason)
    except Exception as exc:  # noqa: BLE001
        _raise_http_exception(exc)

    return Response(status_code=200)


@router.post("/dataflows/{id}/terminate", status_code=200)
async def terminate_data_flow(
    id: str = Path(...),
    message: DataFlowTerminateMessage | None = Body(default=None),
    service: DataFlowService = Depends(get_dataflow_service),
) -> Response:
    """Terminate a data flow."""

    try:
        await service.terminate(id, None if message is None else message.reason)
    except Exception as exc:  # noqa: BLE001
        _raise_http_exception(exc)

    return Response(status_code=200)


@router.get(
    "/dataflows/{id}/status",
    response_model=DataFlowStatusResponseMessage,
    status_code=200,
)
async def get_data_flow_status(
    id: str = Path(...),
    service: DataFlowService = Depends(get_dataflow_service),
) -> DataFlowStatusResponseMessage:
    """Retrieve data flow status."""

    try:
        return await service.get_status(id)
    except Exception as exc:  # noqa: BLE001
        _raise_http_exception(exc)


@router.get("/dataflows/{id}/completed", status_code=200)
@router.post("/dataflows/{id}/completed", status_code=200, include_in_schema=False)
async def notify_completed(
    id: str = Path(...),
    service: DataFlowService = Depends(get_dataflow_service),
) -> Response:
    """Mark data transmission as completed.

    OpenAPI currently declares GET while narrative docs describe POST. The
    scaffold accepts both to avoid protocol drift during integration.
    """

    try:
        await service.completed(id)
    except Exception as exc:  # noqa: BLE001
        _raise_http_exception(exc)

    return Response(status_code=200)


__all__ = ["router"]
