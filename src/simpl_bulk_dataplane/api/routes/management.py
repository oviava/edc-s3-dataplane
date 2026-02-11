"""Dataplane management routes for monitoring dataflows."""

from __future__ import annotations

from typing import NoReturn

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from simpl_bulk_dataplane.api.dependencies import get_dataflow_service
from simpl_bulk_dataplane.application.services import DataFlowService
from simpl_bulk_dataplane.domain.errors import (
    DataFlowNotFoundError,
    DataFlowValidationError,
)
from simpl_bulk_dataplane.domain.monitoring_models import (
    DataFlowInfoResponse,
    DataFlowListResponse,
)
from simpl_bulk_dataplane.domain.transfer_types import TransferMode

router = APIRouter(prefix="/management", tags=["data-plane management"])


def _raise_http_exception(exc: Exception) -> NoReturn:
    if isinstance(exc, DataFlowNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, DataFlowValidationError):
        raise HTTPException(status_code=400, detail=str(exc))
    raise HTTPException(status_code=500, detail="Unexpected data flow error")


@router.get("/dataflows", response_model=DataFlowListResponse, status_code=200)
async def list_data_flows(
    mode: TransferMode | None = Query(default=None),
    service: DataFlowService = Depends(get_dataflow_service),
) -> DataFlowListResponse:
    """List dataflows with transfer progress snapshots."""

    try:
        return await service.list_data_flows(transfer_mode=mode)
    except Exception as exc:  # noqa: BLE001
        _raise_http_exception(exc)


@router.get("/dataflows/{id}", response_model=DataFlowInfoResponse, status_code=200)
async def get_data_flow_details(
    id: str = Path(...),
    service: DataFlowService = Depends(get_dataflow_service),
) -> DataFlowInfoResponse:
    """Get one dataflow with transfer progress snapshot."""

    try:
        return await service.get_data_flow_info(id)
    except Exception as exc:  # noqa: BLE001
        _raise_http_exception(exc)


__all__ = ["router"]
