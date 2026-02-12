"""In-memory repository implementation for data flows."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from simpl_bulk_dataplane.domain.callbacks import (
    ControlPlaneCallbackEvent,
    ControlPlaneCallbackEventType,
    PendingControlPlaneCallbackEvent,
)
from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.ports import (
    CallbackOutboxRepository,
    DataFlowRepository,
    TransferJobRepository,
)
from simpl_bulk_dataplane.domain.signaling_models import DataFlowResponseMessage
from simpl_bulk_dataplane.domain.transfer_jobs import ClaimedTransferJob, TransferJobStatus


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


@dataclass(slots=True)
class _InMemoryTransferJob:
    data_flow_id: str
    status: TransferJobStatus
    lease_owner: str | None
    lease_until: datetime | None
    attempt: int
    checkpoint: dict[str, Any]
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class InMemoryDataFlowRepository(
    DataFlowRepository,
    CallbackOutboxRepository,
    TransferJobRepository,
):
    """Simple repository for local development and tests."""

    def __init__(self) -> None:
        self._by_data_flow_id: dict[str, DataFlow] = {}
        self._process_index: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._outbox_events: dict[int, _InMemoryOutboxEvent] = {}
        self._next_outbox_id = 1
        self._transfer_jobs: dict[str, _InMemoryTransferJob] = {}

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

    async def upsert_transfer_job(
        self,
        *,
        data_flow_id: str,
        status: TransferJobStatus,
        lease_owner: str | None = None,
        lease_seconds: float | None = None,
        checkpoint: dict[str, Any] | None = None,
        last_error: str | None = None,
    ) -> None:
        """Create or update one transfer job."""

        now = datetime.now(tz=UTC)
        lease_until = None
        if lease_owner is not None and lease_seconds is not None:
            lease_until = now + timedelta(seconds=max(lease_seconds, 0.0))
        async with self._lock:
            existing = self._transfer_jobs.get(data_flow_id)
            if existing is None:
                self._transfer_jobs[data_flow_id] = _InMemoryTransferJob(
                    data_flow_id=data_flow_id,
                    status=status,
                    lease_owner=lease_owner,
                    lease_until=lease_until,
                    attempt=0,
                    checkpoint={} if checkpoint is None else dict(checkpoint),
                    last_error=last_error,
                    created_at=now,
                    updated_at=now,
                    started_at=now if status is TransferJobStatus.RUNNING else None,
                    completed_at=(
                        now
                        if status
                        in {
                            TransferJobStatus.COMPLETED,
                            TransferJobStatus.FAILED,
                            TransferJobStatus.TERMINATED,
                        }
                        else None
                    ),
                )
                return

            existing.status = status
            existing.lease_owner = lease_owner
            existing.lease_until = lease_until
            if checkpoint is not None:
                existing.checkpoint = dict(checkpoint)
            if last_error is not None:
                existing.last_error = last_error
            elif status in {TransferJobStatus.RUNNING, TransferJobStatus.COMPLETED}:
                existing.last_error = None
            if status is TransferJobStatus.RUNNING and existing.started_at is None:
                existing.started_at = now
            if status in {
                TransferJobStatus.COMPLETED,
                TransferJobStatus.FAILED,
                TransferJobStatus.TERMINATED,
            }:
                existing.completed_at = now
            else:
                existing.completed_at = None
            existing.updated_at = now

    async def claim_due_transfer_jobs(
        self,
        *,
        lease_owner: str,
        limit: int,
        lease_seconds: float,
    ) -> list[ClaimedTransferJob]:
        """Claim due queued/running jobs for execution recovery."""

        if limit <= 0:
            return []

        now = datetime.now(tz=UTC)
        lease_until = now + timedelta(seconds=max(lease_seconds, 0.0))
        async with self._lock:
            due = [
                job
                for job in self._transfer_jobs.values()
                if job.status in {TransferJobStatus.QUEUED, TransferJobStatus.RUNNING}
                and (job.lease_until is None or job.lease_until <= now)
            ]
            due.sort(key=lambda job: (job.updated_at, job.data_flow_id))
            claimed = due[:limit]

            results: list[ClaimedTransferJob] = []
            for job in claimed:
                job.status = TransferJobStatus.RUNNING
                job.lease_owner = lease_owner
                job.lease_until = lease_until
                job.attempt += 1
                job.updated_at = now
                if job.started_at is None:
                    job.started_at = now
                results.append(
                    ClaimedTransferJob(
                        data_flow_id=job.data_flow_id,
                        status=job.status,
                        lease_owner=lease_owner,
                        attempt=job.attempt,
                        checkpoint=dict(job.checkpoint),
                    )
                )

            return results

    async def renew_transfer_job_lease(
        self,
        *,
        data_flow_id: str,
        lease_owner: str,
        lease_seconds: float,
    ) -> bool:
        """Renew one running job lease if still owned by worker."""

        now = datetime.now(tz=UTC)
        lease_until = now + timedelta(seconds=max(lease_seconds, 0.0))
        async with self._lock:
            job = self._transfer_jobs.get(data_flow_id)
            if job is None:
                return False
            if job.status is not TransferJobStatus.RUNNING:
                return False
            if job.lease_owner != lease_owner:
                return False
            job.lease_until = lease_until
            job.updated_at = now
            return True

    def _upsert_unlocked(self, data_flow: DataFlow) -> None:
        self._by_data_flow_id[data_flow.data_flow_id] = data_flow
        self._process_index[data_flow.process_id] = data_flow.data_flow_id


__all__ = ["InMemoryDataFlowRepository"]
