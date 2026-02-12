"""PostgreSQL repository implementation for data flows."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Any

import asyncpg  # type: ignore[import-untyped]

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
from simpl_bulk_dataplane.domain.signaling_models import DataAddress, DataFlowResponseMessage
from simpl_bulk_dataplane.domain.transfer_types import DataFlowState, TransferMode
from simpl_bulk_dataplane.domain.transfer_jobs import ClaimedTransferJob, TransferJobStatus

_SELECT_COLUMNS = """
    data_flow_id,
    process_id,
    transfer_type,
    transfer_mode,
    participant_id,
    counter_party_id,
    dataspace_context,
    agreement_id,
    dataset_id,
    callback_address,
    state,
    data_address,
    labels,
    metadata
"""


class PostgresDataFlowRepository(
    DataFlowRepository,
    CallbackOutboxRepository,
    TransferJobRepository,
):
    """Data flow repository backed by PostgreSQL."""

    def __init__(
        self,
        dsn: str,
        min_pool_size: int = 1,
        max_pool_size: int = 10,
    ) -> None:
        self._dsn = dsn
        self._min_pool_size = min_pool_size
        self._max_pool_size = max_pool_size
        self._pool: asyncpg.Pool | None = None
        self._pool_lock = asyncio.Lock()

    async def get_by_data_flow_id(self, data_flow_id: str) -> DataFlow | None:
        """Return by data flow id."""

        pool = await self._get_pool()
        row = await pool.fetchrow(
            f"SELECT {_SELECT_COLUMNS} FROM dataflows WHERE data_flow_id = $1",
            data_flow_id,
        )
        if row is None:
            return None
        return self._to_entity(row)

    async def get_by_process_id(self, process_id: str) -> DataFlow | None:
        """Return by process id."""

        pool = await self._get_pool()
        row = await pool.fetchrow(
            f"SELECT {_SELECT_COLUMNS} FROM dataflows WHERE process_id = $1",
            process_id,
        )
        if row is None:
            return None
        return self._to_entity(row)

    async def list_data_flows(self) -> list[DataFlow]:
        """Return all flows."""

        pool = await self._get_pool()
        rows = await pool.fetch(
            f"SELECT {_SELECT_COLUMNS} FROM dataflows ORDER BY process_id ASC",
        )
        return [self._to_entity(row) for row in rows]

    async def upsert(self, data_flow: DataFlow) -> None:
        """Persist entity state."""

        pool = await self._get_pool()
        async with pool.acquire() as connection:
            await self._upsert_on_connection(connection, data_flow)

    async def persist_transition(
        self,
        data_flow: DataFlow,
        callback_event: ControlPlaneCallbackEvent | None = None,
    ) -> None:
        """Persist dataflow transition and optional callback event atomically."""

        pool = await self._get_pool()
        async with pool.acquire() as connection:
            async with connection.transaction():
                await self._upsert_on_connection(connection, data_flow)
                if callback_event is not None:
                    await self._insert_callback_event(connection, callback_event)

    async def claim_due_callback_events(
        self,
        *,
        limit: int,
        lease_seconds: float,
    ) -> list[PendingControlPlaneCallbackEvent]:
        """Claim due callback events and move visibility window via lease."""

        if limit <= 0:
            return []

        pool = await self._get_pool()
        rows = await pool.fetch(
            """
            WITH due AS (
                SELECT id
                FROM control_plane_outbox
                WHERE sent_at IS NULL
                  AND next_attempt_at <= NOW()
                ORDER BY next_attempt_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            )
            UPDATE control_plane_outbox AS outbox
            SET
                next_attempt_at = NOW() + ($2::double precision * INTERVAL '1 second'),
                updated_at = NOW()
            FROM due
            WHERE outbox.id = due.id
            RETURNING
                outbox.id,
                outbox.process_id,
                outbox.data_flow_id,
                outbox.event_type,
                outbox.callback_address,
                outbox.payload,
                outbox.attempts
            """,
            limit,
            max(lease_seconds, 0.0),
        )
        return [self._to_pending_callback_event(row) for row in rows]

    async def mark_callback_event_sent(self, outbox_id: int) -> None:
        """Mark one callback event as sent."""

        pool = await self._get_pool()
        await pool.execute(
            """
            UPDATE control_plane_outbox
            SET
                sent_at = NOW(),
                last_error = NULL,
                updated_at = NOW()
            WHERE id = $1
            """,
            outbox_id,
        )

    async def mark_callback_event_failed(
        self,
        outbox_id: int,
        *,
        error: str,
        next_attempt_at: datetime,
    ) -> None:
        """Record one callback delivery failure with retry schedule."""

        pool = await self._get_pool()
        await pool.execute(
            """
            UPDATE control_plane_outbox
            SET
                attempts = attempts + 1,
                next_attempt_at = $2,
                last_error = $3,
                updated_at = NOW()
            WHERE id = $1
            """,
            outbox_id,
            next_attempt_at,
            error,
        )

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
        """Create or update transfer job state."""

        pool = await self._get_pool()
        checkpoint_json = json.dumps(checkpoint or {})
        lease_window_seconds = None if lease_seconds is None else max(lease_seconds, 0.0)
        await pool.execute(
            """
            INSERT INTO transfer_jobs (
                data_flow_id,
                status,
                lease_owner,
                lease_until,
                checkpoint_json,
                last_error,
                started_at,
                completed_at,
                created_at,
                updated_at
            ) VALUES (
                $1,
                $2,
                $3,
                CASE
                    WHEN $3::text IS NOT NULL AND $4::double precision IS NOT NULL
                    THEN NOW() + ($4::double precision * INTERVAL '1 second')
                    ELSE NULL
                END,
                $5::jsonb,
                $6,
                CASE WHEN $2 = 'RUNNING' THEN NOW() ELSE NULL END,
                CASE
                    WHEN $2 IN ('COMPLETED', 'FAILED', 'TERMINATED') THEN NOW()
                    ELSE NULL
                END,
                NOW(),
                NOW()
            )
            ON CONFLICT (data_flow_id) DO UPDATE SET
                status = EXCLUDED.status,
                lease_owner = EXCLUDED.lease_owner,
                lease_until = EXCLUDED.lease_until,
                checkpoint_json = EXCLUDED.checkpoint_json,
                last_error = EXCLUDED.last_error,
                started_at = CASE
                    WHEN EXCLUDED.status = 'RUNNING'
                    THEN COALESCE(transfer_jobs.started_at, NOW())
                    ELSE transfer_jobs.started_at
                END,
                completed_at = CASE
                    WHEN EXCLUDED.status IN ('COMPLETED', 'FAILED', 'TERMINATED') THEN NOW()
                    ELSE NULL
                END,
                updated_at = NOW()
            """,
            data_flow_id,
            status.value,
            lease_owner,
            lease_window_seconds,
            checkpoint_json,
            last_error,
        )

    async def claim_due_transfer_jobs(
        self,
        *,
        lease_owner: str,
        limit: int,
        lease_seconds: float,
    ) -> list[ClaimedTransferJob]:
        """Claim queued/running jobs whose lease is absent/expired."""

        if limit <= 0:
            return []

        pool = await self._get_pool()
        rows = await pool.fetch(
            """
            WITH due AS (
                SELECT data_flow_id
                FROM transfer_jobs
                WHERE status IN ('QUEUED', 'RUNNING')
                  AND (lease_until IS NULL OR lease_until <= NOW())
                ORDER BY updated_at ASC, data_flow_id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            )
            UPDATE transfer_jobs AS jobs
            SET
                status = 'RUNNING',
                lease_owner = $2,
                lease_until = NOW() + ($3::double precision * INTERVAL '1 second'),
                attempt = jobs.attempt + 1,
                started_at = COALESCE(jobs.started_at, NOW()),
                updated_at = NOW()
            FROM due
            WHERE jobs.data_flow_id = due.data_flow_id
            RETURNING
                jobs.data_flow_id,
                jobs.status,
                jobs.lease_owner,
                jobs.attempt,
                jobs.checkpoint_json
            """,
            limit,
            lease_owner,
            max(lease_seconds, 0.0),
        )
        return [self._to_claimed_transfer_job(row) for row in rows]

    async def renew_transfer_job_lease(
        self,
        *,
        data_flow_id: str,
        lease_owner: str,
        lease_seconds: float,
    ) -> bool:
        """Extend one running job lease if ownership still matches."""

        pool = await self._get_pool()
        result = await pool.execute(
            """
            UPDATE transfer_jobs
            SET
                lease_until = NOW() + ($3::double precision * INTERVAL '1 second'),
                updated_at = NOW()
            WHERE data_flow_id = $1
              AND status = 'RUNNING'
              AND lease_owner = $2
            """,
            data_flow_id,
            lease_owner,
            max(lease_seconds, 0.0),
        )
        return result.endswith("1")

    async def close(self) -> None:
        """Close the pool if it was initialized."""

        pool = self._pool
        self._pool = None
        if pool is not None:
            await pool.close()

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is not None:
            return self._pool

        async with self._pool_lock:
            if self._pool is None:
                pool = await asyncpg.create_pool(
                    dsn=self._dsn,
                    min_size=self._min_pool_size,
                    max_size=self._max_pool_size,
                )
                await self._ensure_schema(pool)
                self._pool = pool
        assert self._pool is not None
        return self._pool

    async def _ensure_schema(self, pool: asyncpg.Pool) -> None:
        await pool.execute(
            """
            CREATE TABLE IF NOT EXISTS dataflows (
                data_flow_id TEXT PRIMARY KEY,
                process_id TEXT UNIQUE NOT NULL,
                transfer_type TEXT NOT NULL,
                transfer_mode TEXT NOT NULL,
                participant_id TEXT NOT NULL,
                counter_party_id TEXT NOT NULL,
                dataspace_context TEXT NOT NULL,
                agreement_id TEXT NOT NULL,
                dataset_id TEXT NOT NULL,
                callback_address TEXT,
                state TEXT NOT NULL,
                data_address JSONB,
                labels JSONB NOT NULL DEFAULT '[]'::jsonb,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb
            );
            """
        )
        await pool.execute(
            """
            CREATE TABLE IF NOT EXISTS control_plane_outbox (
                id BIGSERIAL PRIMARY KEY,
                process_id TEXT NOT NULL,
                data_flow_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                callback_address TEXT,
                payload JSONB NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                sent_at TIMESTAMPTZ,
                last_error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_control_plane_outbox_pending
                ON control_plane_outbox (next_attempt_at, id)
                WHERE sent_at IS NULL;
            """
        )
        await pool.execute(
            """
            CREATE TABLE IF NOT EXISTS transfer_jobs (
                data_flow_id TEXT PRIMARY KEY REFERENCES dataflows(data_flow_id) ON DELETE CASCADE,
                status TEXT NOT NULL,
                lease_owner TEXT,
                lease_until TIMESTAMPTZ,
                attempt INTEGER NOT NULL DEFAULT 0,
                checkpoint_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                last_error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS idx_transfer_jobs_recovery
                ON transfer_jobs (status, lease_until, updated_at, data_flow_id)
                WHERE status IN ('QUEUED', 'RUNNING');
            """
        )

    async def _upsert_on_connection(
        self,
        connection: asyncpg.Connection,
        data_flow: DataFlow,
    ) -> None:
        data_address = (
            None
            if data_flow.data_address is None
            else json.dumps(data_flow.data_address.model_dump(by_alias=True))
        )
        await connection.execute(
            """
            INSERT INTO dataflows (
                data_flow_id,
                process_id,
                transfer_type,
                transfer_mode,
                participant_id,
                counter_party_id,
                dataspace_context,
                agreement_id,
                dataset_id,
                callback_address,
                state,
                data_address,
                labels,
                metadata
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13::jsonb, $14::jsonb
            )
            ON CONFLICT (data_flow_id) DO UPDATE SET
                process_id = EXCLUDED.process_id,
                transfer_type = EXCLUDED.transfer_type,
                transfer_mode = EXCLUDED.transfer_mode,
                participant_id = EXCLUDED.participant_id,
                counter_party_id = EXCLUDED.counter_party_id,
                dataspace_context = EXCLUDED.dataspace_context,
                agreement_id = EXCLUDED.agreement_id,
                dataset_id = EXCLUDED.dataset_id,
                callback_address = EXCLUDED.callback_address,
                state = EXCLUDED.state,
                data_address = EXCLUDED.data_address,
                labels = EXCLUDED.labels,
                metadata = EXCLUDED.metadata
            """,
            data_flow.data_flow_id,
            data_flow.process_id,
            data_flow.transfer_type,
            data_flow.transfer_mode.value,
            data_flow.participant_id,
            data_flow.counter_party_id,
            data_flow.dataspace_context,
            data_flow.agreement_id,
            data_flow.dataset_id,
            data_flow.callback_address,
            data_flow.state.value,
            data_address,
            json.dumps(data_flow.labels),
            json.dumps(data_flow.metadata),
        )

    async def _insert_callback_event(
        self,
        connection: asyncpg.Connection,
        callback_event: ControlPlaneCallbackEvent,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO control_plane_outbox (
                process_id,
                data_flow_id,
                event_type,
                callback_address,
                payload,
                attempts,
                next_attempt_at
            ) VALUES (
                $1, $2, $3, $4, $5::jsonb, 0, NOW()
            )
            """,
            callback_event.process_id,
            callback_event.data_flow_id,
            callback_event.event_type.value,
            callback_event.callback_address,
            json.dumps(callback_event.payload.model_dump(by_alias=True, exclude_none=True)),
        )

    def _to_entity(self, row: asyncpg.Record) -> DataFlow:
        raw_data_address = self._decode_json_field(row["data_address"])
        data_address = (
            None if raw_data_address is None else DataAddress.model_validate(raw_data_address)
        )
        labels = self._decode_list(row["labels"])
        metadata = self._decode_dict(row["metadata"])

        state_literal = str(row["state"])
        if state_literal == "UNINITIALIZED":
            # Backward compatibility for rows persisted before INITIALIZED rename.
            state_literal = DataFlowState.INITIALIZED.value

        return DataFlow(
            data_flow_id=str(row["data_flow_id"]),
            process_id=str(row["process_id"]),
            transfer_type=str(row["transfer_type"]),
            transfer_mode=TransferMode(str(row["transfer_mode"])),
            participant_id=str(row["participant_id"]),
            counter_party_id=str(row["counter_party_id"]),
            dataspace_context=str(row["dataspace_context"]),
            agreement_id=str(row["agreement_id"]),
            dataset_id=str(row["dataset_id"]),
            callback_address=self._as_optional_str(row["callback_address"]),
            state=DataFlowState(state_literal),
            data_address=data_address,
            labels=labels,
            metadata=metadata,
        )

    def _to_pending_callback_event(
        self,
        row: asyncpg.Record,
    ) -> PendingControlPlaneCallbackEvent:
        payload = DataFlowResponseMessage.model_validate(self._decode_json_field(row["payload"]))
        return PendingControlPlaneCallbackEvent(
            outbox_id=int(row["id"]),
            process_id=str(row["process_id"]),
            data_flow_id=str(row["data_flow_id"]),
            event_type=ControlPlaneCallbackEventType(str(row["event_type"])),
            callback_address=self._as_optional_str(row["callback_address"]),
            payload=payload,
            attempts=int(row["attempts"]),
        )

    def _to_claimed_transfer_job(self, row: asyncpg.Record) -> ClaimedTransferJob:
        checkpoint_raw = self._decode_json_field(row["checkpoint_json"])
        checkpoint = checkpoint_raw if isinstance(checkpoint_raw, dict) else {}
        return ClaimedTransferJob(
            data_flow_id=str(row["data_flow_id"]),
            status=TransferJobStatus(str(row["status"])),
            lease_owner=str(row["lease_owner"]),
            attempt=int(row["attempt"]),
            checkpoint=checkpoint,
        )

    def _decode_json_field(self, value: object) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            return json.loads(value)
        return value

    def _decode_list(self, value: object) -> list[str]:
        decoded = self._decode_json_field(value)
        if not isinstance(decoded, list):
            raise TypeError(f"Expected list payload for labels, got {type(decoded)!r}.")
        if not all(isinstance(item, str) for item in decoded):
            raise TypeError("Labels payload must contain only strings.")
        return decoded

    def _decode_dict(self, value: object) -> dict[str, Any]:
        decoded = self._decode_json_field(value)
        if not isinstance(decoded, dict):
            raise TypeError(f"Expected dict payload for metadata, got {type(decoded)!r}.")
        if not all(isinstance(key, str) for key in decoded):
            raise TypeError("Metadata payload must contain string keys.")
        return decoded

    def _as_optional_str(self, value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        raise TypeError(f"Expected optional string value, got {type(value)!r}.")


__all__ = ["PostgresDataFlowRepository"]
