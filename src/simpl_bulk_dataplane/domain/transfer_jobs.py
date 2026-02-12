"""Transfer job durability models."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class TransferJobStatus(StrEnum):
    """Durable runtime execution states."""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TERMINATED = "TERMINATED"


TERMINAL_TRANSFER_JOB_STATUSES = frozenset(
    {
        TransferJobStatus.COMPLETED,
        TransferJobStatus.FAILED,
        TransferJobStatus.TERMINATED,
    }
)


@dataclass(slots=True, frozen=True)
class ClaimedTransferJob:
    """A transfer job claimed for recovery/execution on one worker."""

    data_flow_id: str
    status: TransferJobStatus
    lease_owner: str
    attempt: int
    checkpoint: dict[str, Any]


__all__ = [
    "ClaimedTransferJob",
    "TERMINAL_TRANSFER_JOB_STATUSES",
    "TransferJobStatus",
]
