"""Request/response models for the manual E2E UI backend."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from simpl_bulk_dataplane.domain.transfer_types import TransferMode


class UiModel(BaseModel):
    """Base model with strict payload validation."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class StartTransferRequest(UiModel):
    """Start transfer form payload."""

    dataplane_url: str = Field(alias="dataplaneUrl")
    transfer_mode: TransferMode = Field(alias="transferMode")
    source_bucket: str = Field(alias="sourceBucket")
    source_key: str = Field(alias="sourceKey")
    destination_bucket: str = Field(alias="destinationBucket")
    destination_key: str = Field(alias="destinationKey")
    source_endpoint_url: str | None = Field(default=None, alias="sourceEndpointUrl")
    destination_endpoint_url: str | None = Field(default=None, alias="destinationEndpointUrl")
    process_id: str | None = Field(default=None, alias="processId")
    auto_started_notification: bool = Field(default=True, alias="autoStartedNotification")


class PauseTransferRequest(UiModel):
    """Pause transfer form payload."""

    dataplane_url: str = Field(alias="dataplaneUrl")
    reason: str | None = None


class StartExistingTransferRequest(UiModel):
    """Start existing transfer form payload."""

    dataplane_url: str = Field(alias="dataplaneUrl")


class DataflowQueryRequest(UiModel):
    """Query payload for multi-dataplane flow polling."""

    dataplane_urls: list[str] = Field(alias="dataplaneUrls")


class StartTransferResponse(UiModel):
    """Start transfer API result."""

    dataplane_url: str = Field(alias="dataplaneUrl")
    data_flow_id: str = Field(alias="dataFlowId")
    process_id: str = Field(alias="processId")
    state: str


class DataplanePollResult(UiModel):
    """One dataplane's management query response."""

    dataplane_url: str = Field(alias="dataplaneUrl")
    dataplane_id: str | None = Field(default=None, alias="dataplaneId")
    data_flows: list[dict[str, object]] = Field(default_factory=list, alias="dataFlows")
    error: str | None = None


class DataflowQueryResponse(UiModel):
    """Aggregate poll response across dataplanes."""

    results: list[DataplanePollResult]


__all__ = [
    "DataflowQueryRequest",
    "DataflowQueryResponse",
    "DataplanePollResult",
    "PauseTransferRequest",
    "StartExistingTransferRequest",
    "StartTransferRequest",
    "StartTransferResponse",
]
