"""Reusable slot-based execution queue for transfer runtimes."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

_DEFAULT_SLOT_ACQUIRE_TIMEOUT_SECONDS = 0.1
_DEFAULT_PAUSE_POLL_INTERVAL_SECONDS = 0.05

QueueStateCallback = Callable[[], Awaitable[None]]


@dataclass(slots=True)
class QueueExecutionControl:
    """Mutable execution controls for one queued runtime job."""

    pause_event: asyncio.Event = field(default_factory=asyncio.Event)
    terminate_event: asyncio.Event = field(default_factory=asyncio.Event)
    slot_acquired: bool = False
    waiting_for_slot: bool = False

    def __post_init__(self) -> None:
        self.pause_event.set()


class SlotBasedExecutionQueue:
    """Manage bounded concurrent execution with wait/queue semantics."""

    def __init__(
        self,
        max_active_executions: int,
        slot_acquire_timeout_seconds: float = _DEFAULT_SLOT_ACQUIRE_TIMEOUT_SECONDS,
        pause_poll_interval_seconds: float = _DEFAULT_PAUSE_POLL_INTERVAL_SECONDS,
    ) -> None:
        self._max_active_executions = max(1, max_active_executions)
        self._slot_acquire_timeout_seconds = max(0.01, slot_acquire_timeout_seconds)
        self._pause_poll_interval_seconds = max(0.01, pause_poll_interval_seconds)
        self._slots = asyncio.Semaphore(self._max_active_executions)

    @property
    def max_active_executions(self) -> int:
        """Return queue capacity for concurrently active executions."""

        return self._max_active_executions

    async def wait_until_active(
        self,
        control: QueueExecutionControl,
        on_queue_state_change: QueueStateCallback | None = None,
    ) -> None:
        """Block until execution is resumed, not terminated, and has an acquired slot."""

        while True:
            if control.terminate_event.is_set():
                self.release(control)
                raise asyncio.CancelledError

            if not control.pause_event.is_set():
                self.release(control)
                await self._set_waiting_for_slot(
                    control,
                    waiting_for_slot=False,
                    on_queue_state_change=on_queue_state_change,
                )
                await asyncio.sleep(self._pause_poll_interval_seconds)
                continue

            if control.slot_acquired:
                await self._set_waiting_for_slot(
                    control,
                    waiting_for_slot=False,
                    on_queue_state_change=on_queue_state_change,
                )
                return

            await self._set_waiting_for_slot(
                control,
                waiting_for_slot=True,
                on_queue_state_change=on_queue_state_change,
            )
            try:
                await asyncio.wait_for(
                    self._slots.acquire(),
                    timeout=self._slot_acquire_timeout_seconds,
                )
            except TimeoutError:
                continue

            if control.terminate_event.is_set() or not control.pause_event.is_set():
                self._slots.release()
                continue

            control.slot_acquired = True
            await self._set_waiting_for_slot(
                control,
                waiting_for_slot=False,
                on_queue_state_change=on_queue_state_change,
            )
            return

    async def mark_not_waiting(
        self,
        control: QueueExecutionControl,
        on_queue_state_change: QueueStateCallback | None = None,
    ) -> None:
        """Clear queued state for callers that explicitly transition out of queueing."""

        await self._set_waiting_for_slot(
            control,
            waiting_for_slot=False,
            on_queue_state_change=on_queue_state_change,
        )

    def release(self, control: QueueExecutionControl) -> None:
        """Release a previously acquired execution slot."""

        if not control.slot_acquired:
            return
        control.slot_acquired = False
        self._slots.release()

    async def _set_waiting_for_slot(
        self,
        control: QueueExecutionControl,
        *,
        waiting_for_slot: bool,
        on_queue_state_change: QueueStateCallback | None,
    ) -> None:
        """Update queued state and notify when the state actually changed."""

        if control.waiting_for_slot == waiting_for_slot:
            return
        control.waiting_for_slot = waiting_for_slot
        if on_queue_state_change is not None:
            await on_queue_state_change()


__all__ = ["QueueExecutionControl", "SlotBasedExecutionQueue"]
