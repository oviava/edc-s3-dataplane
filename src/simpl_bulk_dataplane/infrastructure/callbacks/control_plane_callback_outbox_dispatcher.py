"""Background worker for durable control-plane callback delivery."""

from __future__ import annotations

import asyncio
import logging
import random
from contextlib import suppress
from datetime import UTC, datetime, timedelta

from simpl_bulk_dataplane.domain.callbacks import (
    ControlPlaneCallbackEventType,
    PendingControlPlaneCallbackEvent,
)
from simpl_bulk_dataplane.domain.ports import CallbackOutboxRepository
from simpl_bulk_dataplane.infrastructure.control_plane import (
    ControlPlaneClient,
    ControlPlaneClientError,
)

logger = logging.getLogger(__name__)


class ControlPlaneCallbackOutboxDispatcher:
    """Dispatch callback outbox events with retry and exponential backoff."""

    def __init__(
        self,
        outbox_repository: CallbackOutboxRepository,
        control_plane_client: ControlPlaneClient,
        *,
        poll_interval_seconds: float = 1.0,
        batch_size: int = 20,
        lease_seconds: float = 30.0,
        retry_base_delay_seconds: float = 2.0,
        retry_max_delay_seconds: float = 300.0,
        retry_jitter_ratio: float = 0.2,
        max_error_length: int = 2000,
    ) -> None:
        self._outbox_repository = outbox_repository
        self._control_plane_client = control_plane_client
        self._poll_interval_seconds = max(poll_interval_seconds, 0.01)
        self._batch_size = max(batch_size, 1)
        self._lease_seconds = max(lease_seconds, 0.01)
        self._retry_base_delay_seconds = max(retry_base_delay_seconds, 0.001)
        self._retry_max_delay_seconds = max(
            retry_max_delay_seconds,
            self._retry_base_delay_seconds,
        )
        self._retry_jitter_ratio = max(min(retry_jitter_ratio, 1.0), 0.0)
        self._max_error_length = max(max_error_length, 128)

        self._task: asyncio.Task[None] | None = None
        self._wake_event = asyncio.Event()
        self._stopping = asyncio.Event()
        self._lifecycle_lock = asyncio.Lock()

    async def start(self) -> None:
        """Start dispatcher background loop if not already running."""

        async with self._lifecycle_lock:
            task = self._task
            if task is not None and not task.done():
                return

            self._stopping.clear()
            self._wake_event.set()
            self._task = asyncio.create_task(
                self._run_loop(),
                name="control-plane-callback-outbox-dispatcher",
            )

    async def stop(self) -> None:
        """Stop dispatcher background loop."""

        async with self._lifecycle_lock:
            task = self._task
            if task is None:
                return
            self._task = None

            self._stopping.set()
            self._wake_event.set()
            task.cancel()

        with suppress(asyncio.CancelledError):
            await task

    async def notify_new_event(self) -> None:
        """Wake dispatcher so newly queued events can be processed quickly."""

        self._wake_event.set()

    async def dispatch_due_events_once(self) -> int:
        """Process one claimed batch and return claimed event count."""

        events = await self._outbox_repository.claim_due_callback_events(
            limit=self._batch_size,
            lease_seconds=self._lease_seconds,
        )
        if not events:
            return 0

        for event in events:
            await self._dispatch_one_event(event)
        return len(events)

    async def _run_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                processed = await self.dispatch_due_events_once()
            except Exception:
                logger.exception("Control-plane callback outbox loop failed.")
                processed = 0

            if processed > 0:
                continue

            self._wake_event.clear()
            try:
                await asyncio.wait_for(
                    self._wake_event.wait(),
                    timeout=self._poll_interval_seconds,
                )
            except TimeoutError:
                pass

    async def _dispatch_one_event(self, event: PendingControlPlaneCallbackEvent) -> None:
        try:
            await self._send_event(event)
        except ControlPlaneClientError as exc:
            await self._record_failure(event, str(exc))
            return
        except Exception as exc:  # pragma: no cover - defensive fallback
            await self._record_failure(event, f"Unexpected callback error: {exc}")
            return

        await self._outbox_repository.mark_callback_event_sent(event.outbox_id)

    async def _send_event(self, event: PendingControlPlaneCallbackEvent) -> None:
        transfer_id = event.process_id
        message = event.payload
        callback_base_url = self._normalized_callback_base_url(event.callback_address)

        if event.event_type is ControlPlaneCallbackEventType.PREPARED:
            await self._control_plane_client.signal_prepared(
                transfer_id,
                message,
                callback_base_url=callback_base_url,
            )
            return
        if event.event_type is ControlPlaneCallbackEventType.STARTED:
            await self._control_plane_client.signal_started(
                transfer_id,
                message,
                callback_base_url=callback_base_url,
            )
            return
        if event.event_type is ControlPlaneCallbackEventType.COMPLETED:
            await self._control_plane_client.signal_completed(
                transfer_id,
                message,
                callback_base_url=callback_base_url,
            )
            return
        if event.event_type is ControlPlaneCallbackEventType.ERRORED:
            await self._control_plane_client.signal_errored(
                transfer_id,
                message,
                callback_base_url=callback_base_url,
            )
            return

        raise ControlPlaneClientError(f"Unsupported callback event type '{event.event_type}'.")

    async def _record_failure(self, event: PendingControlPlaneCallbackEvent, error: str) -> None:
        next_attempt = self._next_retry_time(event.attempts + 1)
        compact_error = error.strip() or "callback delivery failed"
        compact_error = compact_error[: self._max_error_length]

        await self._outbox_repository.mark_callback_event_failed(
            event.outbox_id,
            error=compact_error,
            next_attempt_at=next_attempt,
        )
        logger.warning(
            "Control-plane callback delivery failed for outbox event %s (attempt %s): %s",
            event.outbox_id,
            event.attempts + 1,
            compact_error,
        )

    def _next_retry_time(self, attempt_number: int) -> datetime:
        base_delay = self._retry_base_delay_seconds * (2 ** max(attempt_number - 1, 0))
        capped_delay = min(base_delay, self._retry_max_delay_seconds)
        if self._retry_jitter_ratio > 0:
            jitter_window = capped_delay * self._retry_jitter_ratio
            jitter = random.uniform(-jitter_window, jitter_window)
            capped_delay = max(capped_delay + jitter, self._retry_base_delay_seconds)

        return datetime.now(tz=UTC) + timedelta(seconds=capped_delay)

    def _normalized_callback_base_url(self, callback_address: str | None) -> str | None:
        if callback_address is None:
            return None

        normalized = callback_address.strip()
        if not normalized:
            return None
        return normalized


__all__ = ["ControlPlaneCallbackOutboxDispatcher"]
