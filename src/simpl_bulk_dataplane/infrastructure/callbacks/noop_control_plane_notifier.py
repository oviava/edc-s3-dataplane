"""No-op callback implementation for scaffold runs."""

from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.ports import ControlPlaneNotifier
from simpl_bulk_dataplane.domain.signaling_models import DataFlowResponseMessage


class NoopControlPlaneNotifier(ControlPlaneNotifier):
    """Placeholder callback adapter until HTTP callbacks are implemented."""

    async def notify_prepared(self, data_flow: DataFlow, message: DataFlowResponseMessage) -> None:
        _ = (data_flow, message)

    async def notify_started(self, data_flow: DataFlow, message: DataFlowResponseMessage) -> None:
        _ = (data_flow, message)

    async def notify_completed(self, data_flow: DataFlow, message: DataFlowResponseMessage) -> None:
        _ = (data_flow, message)

    async def notify_terminated(
        self, data_flow: DataFlow, message: DataFlowResponseMessage
    ) -> None:
        _ = (data_flow, message)


__all__ = ["NoopControlPlaneNotifier"]
