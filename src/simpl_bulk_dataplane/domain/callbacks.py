"""Control-plane callback outbox models."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from simpl_bulk_dataplane.domain.signaling_models import DataFlowResponseMessage


class ControlPlaneCallbackEventType(StrEnum):
    """Callback event kind mapped to control-plane transfer callback paths."""

    PREPARED = "prepared"
    STARTED = "started"
    COMPLETED = "completed"
    ERRORED = "errored"


@dataclass(slots=True, frozen=True)
class ControlPlaneCallbackEvent:
    """Outbox payload persisted with a data-flow state transition."""

    process_id: str
    data_flow_id: str
    event_type: ControlPlaneCallbackEventType
    callback_address: str | None
    payload: DataFlowResponseMessage


@dataclass(slots=True, frozen=True)
class PendingControlPlaneCallbackEvent:
    """Claimed outbox event ready for callback delivery attempts."""

    outbox_id: int
    process_id: str
    data_flow_id: str
    event_type: ControlPlaneCallbackEventType
    callback_address: str | None
    payload: DataFlowResponseMessage
    attempts: int


__all__ = [
    "ControlPlaneCallbackEvent",
    "ControlPlaneCallbackEventType",
    "PendingControlPlaneCallbackEvent",
]
