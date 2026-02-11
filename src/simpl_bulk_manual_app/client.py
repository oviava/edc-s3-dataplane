"""HTTP client for dataplane signaling and management endpoints."""

from __future__ import annotations

from typing import Any

import httpx


class DataPlaneClientError(RuntimeError):
    """Raised when dataplane calls fail."""


class DataPlaneClient:
    """Small wrapper around HTTP calls used by the manual UI app."""

    def __init__(self, timeout_seconds: float = 20.0) -> None:
        self._http = httpx.AsyncClient(timeout=timeout_seconds)

    async def close(self) -> None:
        """Release underlying HTTP resources."""

        await self._http.aclose()

    async def start_dataflow(
        self,
        dataplane_url: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Call `/dataflows/start`."""

        response = await self._http.post(
            self._endpoint(dataplane_url, "/dataflows/start"),
            json=payload,
        )
        return self._parse_json_response(response)

    async def notify_started(
        self,
        dataplane_url: str,
        data_flow_id: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """Call `/dataflows/{id}/started`."""

        response = await self._http.post(
            self._endpoint(dataplane_url, f"/dataflows/{data_flow_id}/started"),
            json=payload,
        )
        self._ensure_success(response)

    async def suspend_dataflow(
        self,
        dataplane_url: str,
        data_flow_id: str,
        reason: str | None = None,
    ) -> None:
        """Call `/dataflows/{id}/suspend`."""

        body = None if reason is None else {"reason": reason}
        response = await self._http.post(
            self._endpoint(dataplane_url, f"/dataflows/{data_flow_id}/suspend"),
            json=body,
        )
        self._ensure_success(response)

    def _endpoint(self, base_url: str, path: str) -> str:
        clean_base = base_url.strip().rstrip("/")
        if not clean_base:
            raise DataPlaneClientError("Dataplane URL cannot be empty.")
        return f"{clean_base}{path}"

    def _ensure_success(self, response: httpx.Response) -> None:
        if response.is_success:
            return
        message = self._detail_from_response(response)
        raise DataPlaneClientError(
            f"{response.request.method} {response.request.url} failed: "
            f"{response.status_code} {message}"
        )

    def _parse_json_response(self, response: httpx.Response) -> dict[str, Any]:
        self._ensure_success(response)
        try:
            payload = response.json()
        except ValueError as exc:
            raise DataPlaneClientError(
                f"{response.request.method} {response.request.url} returned invalid JSON."
            ) from exc
        if not isinstance(payload, dict):
            raise DataPlaneClientError(
                f"{response.request.method} {response.request.url} returned non-object JSON."
            )
        return payload

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


__all__ = ["DataPlaneClient", "DataPlaneClientError"]
