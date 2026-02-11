"""In-memory repository implementation for data flows."""

from __future__ import annotations

import asyncio

from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.ports import DataFlowRepository


class InMemoryDataFlowRepository(DataFlowRepository):
    """Simple repository for local development and tests."""

    def __init__(self) -> None:
        self._by_data_flow_id: dict[str, DataFlow] = {}
        self._process_index: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def get_by_data_flow_id(self, data_flow_id: str) -> DataFlow | None:
        """Return by data flow id."""

        return self._by_data_flow_id.get(data_flow_id)

    async def get_by_process_id(self, process_id: str) -> DataFlow | None:
        """Return by process id."""

        data_flow_id = self._process_index.get(process_id)
        if data_flow_id is None:
            return None
        return self._by_data_flow_id.get(data_flow_id)

    async def upsert(self, data_flow: DataFlow) -> None:
        """Persist entity state."""

        async with self._lock:
            self._by_data_flow_id[data_flow.data_flow_id] = data_flow
            self._process_index[data_flow.process_id] = data_flow.data_flow_id


__all__ = ["InMemoryDataFlowRepository"]
