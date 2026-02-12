"""Ports for repository, transfer execution, and callbacks."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from simpl_bulk_dataplane.domain.callbacks import (
    ControlPlaneCallbackEvent,
    PendingControlPlaneCallbackEvent,
)
from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.monitoring_models import TransferProgressSnapshot
from simpl_bulk_dataplane.domain.signaling_models import (
    DataAddress,
    DataFlowPrepareMessage,
    DataFlowResponseMessage,
    DataFlowStartedNotificationMessage,
    DataFlowStartMessage,
)


class DataFlowRepository(Protocol):
    """Persistence port for data flows."""

    async def get_by_data_flow_id(self, data_flow_id: str) -> DataFlow | None:
        """Return a data flow by its generated dataFlowId."""

    async def get_by_process_id(self, process_id: str) -> DataFlow | None:
        """Return a data flow by transfer process id."""

    async def list_data_flows(self) -> list[DataFlow]:
        """Return all known data flows."""

    async def upsert(self, data_flow: DataFlow) -> None:
        """Create or update a data flow."""

    async def persist_transition(
        self,
        data_flow: DataFlow,
        callback_event: ControlPlaneCallbackEvent | None = None,
    ) -> None:
        """Persist state transition and optional callback event atomically."""


@runtime_checkable
class CallbackOutboxRepository(Protocol):
    """Durable callback outbox port used by dispatcher workers."""

    async def claim_due_callback_events(
        self,
        *,
        limit: int,
        lease_seconds: float,
    ) -> list[PendingControlPlaneCallbackEvent]:
        """Claim due callback events for delivery and apply a processing lease."""

    async def mark_callback_event_sent(self, outbox_id: int) -> None:
        """Mark callback event as delivered successfully."""

    async def mark_callback_event_failed(
        self,
        outbox_id: int,
        *,
        error: str,
        next_attempt_at: datetime,
    ) -> None:
        """Record callback delivery failure and schedule next retry."""


class TransferExecutor(Protocol):
    """Wire protocol execution port."""

    async def prepare(
        self, data_flow: DataFlow, message: DataFlowPrepareMessage
    ) -> DataAddress | None:
        """Prepare resources before transfer start."""

    async def start(self, data_flow: DataFlow, message: DataFlowStartMessage) -> DataAddress | None:
        """Start transfer execution."""

    async def notify_started(
        self,
        data_flow: DataFlow,
        message: DataFlowStartedNotificationMessage | None,
    ) -> None:
        """Handle consumer-side started signal."""

    async def suspend(self, data_flow: DataFlow, reason: str | None) -> None:
        """Suspend ongoing transfer activity."""

    async def terminate(self, data_flow: DataFlow, reason: str | None) -> None:
        """Terminate transfer activity."""

    async def complete(self, data_flow: DataFlow) -> None:
        """Finalize transfer activity."""

    async def get_progress(self, data_flow_id: str) -> TransferProgressSnapshot | None:
        """Return runtime transfer progress when available."""


class ControlPlaneNotifier(Protocol):
    """Outbound callback port to control plane endpoints."""

    async def notify_prepared(self, data_flow: DataFlow, message: DataFlowResponseMessage) -> None:
        """Signal PREPARED state to control plane."""

    async def notify_started(self, data_flow: DataFlow, message: DataFlowResponseMessage) -> None:
        """Signal STARTED state to control plane."""

    async def notify_completed(self, data_flow: DataFlow, message: DataFlowResponseMessage) -> None:
        """Signal COMPLETED state to control plane."""

    async def notify_terminated(
        self, data_flow: DataFlow, message: DataFlowResponseMessage
    ) -> None:
        """Signal TERMINATED state to control plane."""


class DataFlowEventPublisher(Protocol):
    """Outbound event publisher for dataflow state/progress updates."""

    async def publish_state(
        self,
        data_flow: DataFlow,
        progress: TransferProgressSnapshot | None = None,
    ) -> None:
        """Publish state-oriented flow event."""

    async def publish_progress(
        self,
        data_flow_id: str,
        progress: TransferProgressSnapshot,
        data_flow: DataFlow | None = None,
    ) -> None:
        """Publish progress-oriented flow event."""


__all__ = [
    "CallbackOutboxRepository",
    "ControlPlaneNotifier",
    "DataFlowEventPublisher",
    "DataFlowRepository",
    "TransferExecutor",
]
