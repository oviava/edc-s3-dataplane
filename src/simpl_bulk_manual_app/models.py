"""Request/response models for the manual E2E UI backend."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

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
    source_access_key_id: str | None = Field(default=None, alias="sourceAccessKeyId")
    source_secret_access_key: str | None = Field(default=None, alias="sourceSecretAccessKey")
    destination_access_key_id: str | None = Field(default=None, alias="destinationAccessKeyId")
    destination_secret_access_key: str | None = Field(
        default=None, alias="destinationSecretAccessKey"
    )
    process_id: str | None = Field(default=None, alias="processId")
    auto_started_notification: bool = Field(default=True, alias="autoStartedNotification")

    @model_validator(mode="after")
    def validate_s3_credential_pairs(self) -> StartTransferRequest:
        """Require complete static credential pairs per source/destination."""

        self._validate_pair(
            access_key_id=self.source_access_key_id,
            secret_access_key=self.source_secret_access_key,
            scope="source",
        )
        self._validate_pair(
            access_key_id=self.destination_access_key_id,
            secret_access_key=self.destination_secret_access_key,
            scope="destination",
        )
        return self

    @staticmethod
    def _validate_pair(
        *,
        access_key_id: str | None,
        secret_access_key: str | None,
        scope: str,
    ) -> None:
        """Reject partial static credential configuration."""

        if (access_key_id is None) == (secret_access_key is None):
            return
        raise ValueError(
            f"{scope}AccessKeyId and {scope}SecretAccessKey must be set together."
        )


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
