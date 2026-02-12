"""Domain entities."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from simpl_bulk_dataplane.domain.signaling_models import DataAddress
from simpl_bulk_dataplane.domain.transfer_types import DataFlowState, TransferMode


@dataclass(slots=True)
class DataFlow:
    """Mutable representation of a transfer lifecycle instance."""

    data_flow_id: str
    process_id: str
    transfer_type: str
    transfer_mode: TransferMode
    participant_id: str
    counter_party_id: str
    dataspace_context: str
    agreement_id: str
    dataset_id: str
    callback_address: str | None
    state: DataFlowState = DataFlowState.INITIALIZED
    data_address: DataAddress | None = None
    labels: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def status_path(self) -> str:
        """Relative status path for asynchronous responses."""

        return f"/dataflows/{self.data_flow_id}/status"


__all__ = ["DataFlow"]
