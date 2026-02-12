from __future__ import annotations

import asyncio

import pytest

from simpl_bulk_dataplane.infrastructure.transfers.runtime import (
    QueueExecutionControl,
    SlotBasedExecutionQueue,
)


def test_slot_queue_limits_concurrency_and_marks_waiting_state() -> None:
    queue = SlotBasedExecutionQueue(
        max_active_executions=1,
        slot_acquire_timeout_seconds=0.02,
        pause_poll_interval_seconds=0.01,
    )
    first = QueueExecutionControl()
    second = QueueExecutionControl()
    second_queue_state_changes: list[bool] = []

    async def second_on_change() -> None:
        second_queue_state_changes.append(second.waiting_for_slot)

    async def scenario() -> None:
        await queue.wait_until_active(first)
        assert first.slot_acquired is True

        second_waiter = asyncio.create_task(
            queue.wait_until_active(
                second,
                on_queue_state_change=second_on_change,
            )
        )
        await asyncio.sleep(0.05)
        assert second.waiting_for_slot is True
        assert second.slot_acquired is False

        queue.release(first)
        await asyncio.wait_for(second_waiter, timeout=1.0)
        assert second.slot_acquired is True
        assert second.waiting_for_slot is False

        queue.release(second)

    asyncio.run(scenario())

    assert True in second_queue_state_changes
    assert False in second_queue_state_changes


def test_slot_queue_cancels_waiting_execution_on_terminate() -> None:
    queue = SlotBasedExecutionQueue(
        max_active_executions=1,
        slot_acquire_timeout_seconds=0.02,
        pause_poll_interval_seconds=0.01,
    )
    first = QueueExecutionControl()
    second = QueueExecutionControl()

    async def scenario() -> None:
        await queue.wait_until_active(first)

        second_waiter = asyncio.create_task(queue.wait_until_active(second))
        await asyncio.sleep(0.05)
        assert second.waiting_for_slot is True
        assert second.slot_acquired is False

        second.terminate_event.set()
        with pytest.raises(asyncio.CancelledError):
            await second_waiter

        queue.release(first)

    asyncio.run(scenario())
