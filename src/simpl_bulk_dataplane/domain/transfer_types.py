"""Transfer type and state helpers."""

from enum import StrEnum

from simpl_bulk_dataplane.domain.errors import UnsupportedTransferTypeError


class DataFlowState(StrEnum):
    """Supported data flow states."""

    UNINITIALIZED = "UNINITIALIZED"
    PREPARING = "PREPARING"
    PREPARED = "PREPARED"
    STARTING = "STARTING"
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    SUSPENDED = "SUSPENDED"
    TERMINATED = "TERMINATED"


class TransferMode(StrEnum):
    """Transfer direction."""

    PUSH = "PUSH"
    PULL = "PULL"


TERMINAL_STATES = frozenset({DataFlowState.COMPLETED, DataFlowState.TERMINATED})


def parse_transfer_mode(transfer_type: str) -> TransferMode:
    """Infer transfer mode from transferType suffix."""

    normalized = transfer_type.strip().upper()
    if normalized.endswith("PUSH"):
        return TransferMode.PUSH
    if normalized.endswith("PULL"):
        return TransferMode.PULL
    raise UnsupportedTransferTypeError(
        f"Unable to infer PUSH/PULL mode from transferType '{transfer_type}'."
    )


def ensure_s3_transfer_type(transfer_type: str) -> None:
    """Ensure transferType targets an S3 flow."""

    if "S3" not in transfer_type.upper():
        raise UnsupportedTransferTypeError(
            f"Only S3 transfer types are supported, got '{transfer_type}'."
        )


__all__ = [
    "DataFlowState",
    "TERMINAL_STATES",
    "TransferMode",
    "ensure_s3_transfer_type",
    "parse_transfer_mode",
]
