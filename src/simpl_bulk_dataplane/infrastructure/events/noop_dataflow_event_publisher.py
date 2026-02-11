"""No-op dataflow event publisher."""

from __future__ import annotations

from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.monitoring_models import TransferProgressSnapshot
from simpl_bulk_dataplane.domain.ports import DataFlowEventPublisher


class NoopDataFlowEventPublisher(DataFlowEventPublisher):
    """No-op implementation for environments without event streaming."""

    async def publish_state(
        self,
        data_flow: DataFlow,
        progress: TransferProgressSnapshot | None = None,
    ) -> None:
        _ = (data_flow, progress)

    async def publish_progress(
        self,
        data_flow_id: str,
        progress: TransferProgressSnapshot,
        data_flow: DataFlow | None = None,
    ) -> None:
        _ = (data_flow_id, progress, data_flow)


__all__ = ["NoopDataFlowEventPublisher"]
