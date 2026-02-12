"""In-memory repository implementation for data flows."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from simpl_bulk_dataplane.domain.callbacks import (
    ControlPlaneCallbackEvent,
    ControlPlaneCallbackEventType,
    PendingControlPlaneCallbackEvent,
)
from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.ports import CallbackOutboxRepository, DataFlowRepository
from simpl_bulk_dataplane.domain.signaling_models import DataFlowResponseMessage


@dataclass(slots=True)
class _InMemoryOutboxEvent:
    outbox_id: int
    process_id: str
    data_flow_id: str
    event_type: ControlPlaneCallbackEventType
    callback_address: str | None
    payload: DataFlowResponseMessage
    attempts: int
    next_attempt_at: datetime
    sent_at: datetime | None
    last_error: str | None


class InMemoryDataFlowRepository(DataFlowRepository, CallbackOutboxRepository):
    """Simple repository for local development and tests."""

    def __init__(self) -> None:
        self._by_data_flow_id: dict[str, DataFlow] = {}
        self._process_index: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._outbox_events: dict[int, _InMemoryOutboxEvent] = {}
        self._next_outbox_id = 1

    async def get_by_data_flow_id(self, data_flow_id: str) -> DataFlow | None:
        """Return by data flow id."""

        return self._by_data_flow_id.get(data_flow_id)

    async def get_by_process_id(self, process_id: str) -> DataFlow | None:
        """Return by process id."""

        data_flow_id = self._process_index.get(process_id)
        if data_flow_id is None:
            return None
        return self._by_data_flow_id.get(data_flow_id)

    async def list_data_flows(self) -> list[DataFlow]:
        """Return all flows in insertion order."""

        async with self._lock:
            return list(self._by_data_flow_id.values())

    async def upsert(self, data_flow: DataFlow) -> None:
        """Persist entity state."""

        async with self._lock:
            self._upsert_unlocked(data_flow)

    async def persist_transition(
        self,
        data_flow: DataFlow,
        callback_event: ControlPlaneCallbackEvent | None = None,
    ) -> None:
        """Persist transition and optional callback event under one lock."""

        async with self._lock:
            self._upsert_unlocked(data_flow)
            if callback_event is None:
                return
            outbox_id = self._next_outbox_id
            self._next_outbox_id += 1
            self._outbox_events[outbox_id] = _InMemoryOutboxEvent(
                outbox_id=outbox_id,
                process_id=callback_event.process_id,
                data_flow_id=callback_event.data_flow_id,
                event_type=callback_event.event_type,
                callback_address=callback_event.callback_address,
                payload=callback_event.payload,
                attempts=0,
                next_attempt_at=datetime.now(tz=UTC),
                sent_at=None,
                last_error=None,
            )

    async def claim_due_callback_events(
        self,
        *,
        limit: int,
        lease_seconds: float,
    ) -> list[PendingControlPlaneCallbackEvent]:
        """Claim due outbox events and apply processing lease."""

        lease_duration = timedelta(seconds=max(lease_seconds, 0.0))
        now = datetime.now(tz=UTC)
        async with self._lock:
            due = [
                event
                for event in self._outbox_events.values()
                if event.sent_at is None and event.next_attempt_at <= now
            ]
            due.sort(key=lambda event: (event.next_attempt_at, event.outbox_id))

            claimed = due[: max(limit, 0)]
            for event in claimed:
                event.next_attempt_at = now + lease_duration

            return [
                PendingControlPlaneCallbackEvent(
                    outbox_id=event.outbox_id,
                    process_id=event.process_id,
                    data_flow_id=event.data_flow_id,
                    event_type=event.event_type,
                    callback_address=event.callback_address,
                    payload=event.payload,
                    attempts=event.attempts,
                )
                for event in claimed
            ]

    async def mark_callback_event_sent(self, outbox_id: int) -> None:
        """Mark one outbox row as sent."""

        async with self._lock:
            event = self._outbox_events.get(outbox_id)
            if event is None:
                return
            event.sent_at = datetime.now(tz=UTC)
            event.last_error = None

    async def mark_callback_event_failed(
        self,
        outbox_id: int,
        *,
        error: str,
        next_attempt_at: datetime,
    ) -> None:
        """Record callback failure metadata for one outbox row."""

        async with self._lock:
            event = self._outbox_events.get(outbox_id)
            if event is None:
                return
            event.attempts += 1
            event.last_error = error
            event.next_attempt_at = next_attempt_at

    def _upsert_unlocked(self, data_flow: DataFlow) -> None:
        self._by_data_flow_id[data_flow.data_flow_id] = data_flow
        self._process_index[data_flow.process_id] = data_flow.data_flow_id


__all__ = ["InMemoryDataFlowRepository"]
