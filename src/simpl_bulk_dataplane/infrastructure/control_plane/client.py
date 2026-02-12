"""HTTP client for control plane registration and data flow callbacks."""

from __future__ import annotations

from typing import cast
from urllib.parse import quote

import httpx

from simpl_bulk_dataplane.domain.signaling_models import (
    DataFlowResponseMessage,
    DataPlaneRegistrationMessage,
)


class ControlPlaneClientError(RuntimeError):
    """Raised when control-plane calls fail."""


class ControlPlaneClient:
    """Wrapper around control-plane signaling endpoints."""

    def __init__(
        self,
        base_url: str,
        timeout_seconds: float = 10.0,
        transport: httpx.BaseTransport | httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = self._normalize_base_url(base_url)
        self._timeout_seconds = timeout_seconds
        self._transport = transport

    def register_dataplane(self, message: DataPlaneRegistrationMessage) -> None:
        """Call `/dataplanes/register` on control plane."""

        url = self._endpoint("/dataplanes/register")
        sync_transport = cast(httpx.BaseTransport | None, self._transport)
        try:
            with httpx.Client(
                timeout=self._timeout_seconds,
                transport=sync_transport,
            ) as http_client:
                response = http_client.post(
                    url,
                    json=message.model_dump(by_alias=True, exclude_none=True),
                )
        except httpx.HTTPError as exc:
            raise ControlPlaneClientError(f"POST {url} failed: {exc}") from exc
        self._ensure_success(response)

    async def signal_prepared(self, transfer_id: str, message: DataFlowResponseMessage) -> None:
        """Call `/transfers/{transferId}/dataflow/prepared`."""

        await self._signal_dataflow_state(transfer_id, "prepared", message)

    async def signal_started(self, transfer_id: str, message: DataFlowResponseMessage) -> None:
        """Call `/transfers/{transferId}/dataflow/started`."""

        await self._signal_dataflow_state(transfer_id, "started", message)

    async def signal_completed(self, transfer_id: str, message: DataFlowResponseMessage) -> None:
        """Call `/transfers/{transferId}/dataflow/completed`."""

        await self._signal_dataflow_state(transfer_id, "completed", message)

    async def signal_errored(self, transfer_id: str, message: DataFlowResponseMessage) -> None:
        """Call `/transfers/{transferId}/dataflow/errored`."""

        await self._signal_dataflow_state(transfer_id, "errored", message)

    async def _signal_dataflow_state(
        self,
        transfer_id: str,
        state_path: str,
        message: DataFlowResponseMessage,
    ) -> None:
        transfer_id_path = quote(transfer_id, safe="")
        url = self._endpoint(f"/transfers/{transfer_id_path}/dataflow/{state_path}")
        async_transport = cast(httpx.AsyncBaseTransport | None, self._transport)
        try:
            async with httpx.AsyncClient(
                timeout=self._timeout_seconds,
                transport=async_transport,
            ) as http_client:
                response = await http_client.post(
                    url,
                    json=message.model_dump(by_alias=True, exclude_none=True),
                )
        except httpx.HTTPError as exc:
            raise ControlPlaneClientError(f"POST {url} failed: {exc}") from exc
        self._ensure_success(response)

    def _endpoint(self, path: str) -> str:
        return f"{self._base_url}{path}"

    def _ensure_success(self, response: httpx.Response) -> None:
        if response.is_success:
            return
        message = self._detail_from_response(response)
        raise ControlPlaneClientError(
            f"{response.request.method} {response.request.url} failed: "
            f"{response.status_code} {message}"
        )

    def _detail_from_response(self, response: httpx.Response) -> str:
        try:
            payload = response.json()
        except ValueError:
            text = response.text.strip()
            return text or "<no response body>"

        if isinstance(payload, dict):
            detail = payload.get("detail")
            if isinstance(detail, str):
                return detail
        return str(payload)

    def _normalize_base_url(self, base_url: str) -> str:
        normalized = base_url.strip().rstrip("/")
        if not normalized:
            raise ControlPlaneClientError("Control plane endpoint cannot be empty.")
        return normalized


__all__ = ["ControlPlaneClient", "ControlPlaneClientError"]
