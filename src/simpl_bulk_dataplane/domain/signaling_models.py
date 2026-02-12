"""Pydantic models mapped from signaling JSON schemas."""

from __future__ import annotations

from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from simpl_bulk_dataplane.domain.transfer_types import DataFlowState


class SignalingModel(BaseModel):
    """Base model for signaling messages."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class EndpointProperty(SignalingModel):
    """Endpoint property for a DataAddress."""

    type_: str = Field(
        default="EndpointProperty",
        alias="@type",
        validation_alias=AliasChoices("@type", "type"),
        serialization_alias="@type",
    )
    name: str
    value: str


class DataAddress(SignalingModel):
    """Serialized data address used by signaling endpoints."""

    type_: str = Field(
        default="DataAddress",
        alias="@type",
        validation_alias=AliasChoices("@type", "type"),
        serialization_alias="@type",
    )
    endpoint_type: str = Field(alias="endpointType")
    endpoint: str
    endpoint_properties: list[EndpointProperty] = Field(
        default_factory=list, alias="endpointProperties"
    )


class DataFlowBaseMessage(SignalingModel):
    """Base request fields shared across prepare/start."""

    message_id: str = Field(alias="messageId")
    participant_id: str = Field(alias="participantId")
    counter_party_id: str = Field(alias="counterPartyId")
    dataspace_context: str = Field(alias="dataspaceContext")
    process_id: str = Field(alias="processId")
    agreement_id: str = Field(alias="agreementId")
    dataset_id: str = Field(alias="datasetId")
    callback_address: str | None = Field(default=None, alias="callbackAddress")
    transfer_type: str = Field(alias="transferType")
    metadata: dict[str, Any] = Field(default_factory=dict)
    labels: list[str] = Field(default_factory=list)


class DataFlowPrepareMessage(DataFlowBaseMessage):
    """Prepare request."""


class DataFlowStartMessage(DataFlowBaseMessage):
    """Start request."""

    data_address: DataAddress | None = Field(default=None, alias="dataAddress")


class DataFlowStartedNotificationMessage(SignalingModel):
    """Consumer-side started notification request."""

    data_address: DataAddress | None = Field(default=None, alias="dataAddress")


class DataFlowSuspendMessage(SignalingModel):
    """Suspend request."""

    reason: str | None = None


class DataFlowTerminateMessage(SignalingModel):
    """Terminate request."""

    reason: str | None = None


class DataFlowResponseMessage(SignalingModel):
    """Standard response message for prepare/start callbacks."""

    dataplane_id: str = Field(alias="dataplaneId")
    data_flow_id: str = Field(alias="dataFlowId")
    state: DataFlowState
    data_address: DataAddress | None = Field(default=None, alias="dataAddress")
    error: str | None = None


class DataFlowStatusResponseMessage(SignalingModel):
    """Status response for data flow state polling."""

    data_flow_id: str = Field(alias="dataFlowId")
    state: DataFlowState


class DataPlaneRegistrationMessage(SignalingModel):
    """Data plane registration payload sent to control plane."""

    dataplane_id: str = Field(alias="dataplaneId")
    name: str
    description: str | None = None
    endpoint: str
    transfer_types: list[str] = Field(alias="transferTypes")
    authorization: list[dict[str, Any]] = Field(default_factory=list)
    labels: list[str] = Field(default_factory=list)


__all__ = [
    "DataAddress",
    "DataPlaneRegistrationMessage",
    "DataFlowBaseMessage",
    "DataFlowPrepareMessage",
    "DataFlowResponseMessage",
    "DataFlowStartMessage",
    "DataFlowStartedNotificationMessage",
    "DataFlowStatusResponseMessage",
    "DataFlowSuspendMessage",
    "DataFlowTerminateMessage",
    "EndpointProperty",
]
