"""HTTP callback implementation for control-plane notifications."""

from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.ports import ControlPlaneNotifier
from simpl_bulk_dataplane.domain.signaling_models import DataFlowResponseMessage
from simpl_bulk_dataplane.infrastructure.control_plane import ControlPlaneClient


class HttpControlPlaneNotifier(ControlPlaneNotifier):
    """Notify control-plane transfer callbacks over HTTP."""

    def __init__(self, client: ControlPlaneClient) -> None:
        self._client = client

    async def notify_prepared(self, data_flow: DataFlow, message: DataFlowResponseMessage) -> None:
        await self._client.signal_prepared(data_flow.process_id, message)

    async def notify_started(self, data_flow: DataFlow, message: DataFlowResponseMessage) -> None:
        await self._client.signal_started(data_flow.process_id, message)

    async def notify_completed(self, data_flow: DataFlow, message: DataFlowResponseMessage) -> None:
        await self._client.signal_completed(data_flow.process_id, message)

    async def notify_terminated(
        self, data_flow: DataFlow, message: DataFlowResponseMessage
    ) -> None:
        await self._client.signal_errored(data_flow.process_id, message)


__all__ = ["HttpControlPlaneNotifier"]
