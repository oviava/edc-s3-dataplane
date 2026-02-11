"""Monitoring models for internal dataflow management APIs."""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict, Field

from simpl_bulk_dataplane.domain.transfer_types import DataFlowState, TransferMode


@dataclass(slots=True, frozen=True)
class TransferProgressSnapshot:
    """Runtime transfer snapshot exposed by transfer executors."""

    bytes_total: int | None = None
    bytes_transferred: int = 0
    running: bool = False
    paused: bool = False
    finished: bool = False
    last_error: str | None = None

    @property
    def percent_complete(self) -> float | None:
        """Return completion ratio in percent when total size is known."""

        if self.bytes_total is None or self.bytes_total <= 0:
            return None
        ratio = (self.bytes_transferred / self.bytes_total) * 100
        return max(0.0, min(100.0, round(ratio, 2)))


class MonitoringModel(BaseModel):
    """Base model for non-signaling monitoring routes."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class DataFlowProgressResponse(MonitoringModel):
    """Progress data shown by management endpoints."""

    bytes_total: int | None = Field(default=None, alias="bytesTotal")
    bytes_transferred: int = Field(default=0, alias="bytesTransferred")
    percent_complete: float | None = Field(default=None, alias="percentComplete")
    running: bool = False
    paused: bool = False
    finished: bool = False
    last_error: str | None = Field(default=None, alias="lastError")


class DataFlowInfoResponse(MonitoringModel):
    """Single flow management payload."""

    dataplane_id: str = Field(alias="dataplaneId")
    data_flow_id: str = Field(alias="dataFlowId")
    process_id: str = Field(alias="processId")
    transfer_type: str = Field(alias="transferType")
    transfer_mode: TransferMode = Field(alias="transferMode")
    state: DataFlowState
    source_bucket: str | None = Field(default=None, alias="sourceBucket")
    source_key: str | None = Field(default=None, alias="sourceKey")
    destination_bucket: str | None = Field(default=None, alias="destinationBucket")
    destination_key: str | None = Field(default=None, alias="destinationKey")
    progress: DataFlowProgressResponse


class DataFlowListResponse(MonitoringModel):
    """Collection wrapper for management list endpoint."""

    dataplane_id: str = Field(alias="dataplaneId")
    data_flows: list[DataFlowInfoResponse] = Field(alias="dataFlows")


__all__ = [
    "DataFlowInfoResponse",
    "DataFlowListResponse",
    "DataFlowProgressResponse",
    "TransferProgressSnapshot",
]
