"""S3-focused transfer executor scaffold."""

from __future__ import annotations

from typing import Any

from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.ports import TransferExecutor
from simpl_bulk_dataplane.domain.signaling_models import (
    DataAddress,
    DataFlowPrepareMessage,
    DataFlowStartedNotificationMessage,
    DataFlowStartMessage,
    EndpointProperty,
)
from simpl_bulk_dataplane.domain.transfer_types import TransferMode


class S3TransferExecutor(TransferExecutor):
    """Transfer port implementation with S3-oriented address generation.

    This scaffold intentionally avoids performing real S3 I/O. It only models
    endpoint addresses and lifecycle hooks so real workers can be plugged in.
    """

    async def prepare(
        self, data_flow: DataFlow, message: DataFlowPrepareMessage
    ) -> DataAddress | None:
        """Prepare transfer artifacts.

        For PUSH transfers, the consumer side should expose where providers can
        deliver data, so we synthesize an S3 destination address from metadata.
        """

        if data_flow.transfer_mode is TransferMode.PUSH:
            return self._build_data_address(message.metadata, target="destination")
        return None

    async def start(self, data_flow: DataFlow, message: DataFlowStartMessage) -> DataAddress | None:
        """Start transfer artifacts.

        For PULL transfers, the provider side should expose where consumers can
        fetch data, so we synthesize an S3 source address from metadata.
        """

        if data_flow.transfer_mode is TransferMode.PULL:
            return self._build_data_address(message.metadata, target="source")
        return None

    async def notify_started(
        self,
        data_flow: DataFlow,
        message: DataFlowStartedNotificationMessage | None,
    ) -> None:
        """Hook for consumer-side started signal."""

        _ = (data_flow, message)

    async def suspend(self, data_flow: DataFlow, reason: str | None) -> None:
        """Hook for transfer suspension."""

        _ = (data_flow, reason)

    async def terminate(self, data_flow: DataFlow, reason: str | None) -> None:
        """Hook for transfer termination."""

        _ = (data_flow, reason)

    async def complete(self, data_flow: DataFlow) -> None:
        """Hook for transfer completion."""

        _ = data_flow

    def _build_data_address(self, metadata: dict[str, Any], target: str) -> DataAddress | None:
        """Construct an S3 DataAddress from metadata.

        Supported keys include:
        - `sourceBucket`, `sourceKey`
        - `destinationBucket`, `destinationKey`
        - fallback `bucketName`, `key`, `region`
        """

        bucket = self._pick_value(metadata, target, "Bucket")
        if bucket is None:
            return None

        key = self._pick_value(metadata, target, "Key")
        endpoint = f"s3://{bucket}" if key is None else f"s3://{bucket}/{key}"

        endpoint_properties: list[EndpointProperty] = [
            EndpointProperty(name="storageType", value="s3"),
        ]
        if region := metadata.get("region"):
            endpoint_properties.append(EndpointProperty(name="region", value=str(region)))

        return DataAddress(
            type_="DataAddress",
            endpoint_type="urn:aws:s3",
            endpoint=endpoint,
            endpoint_properties=endpoint_properties,
        )

    def _pick_value(self, metadata: dict[str, Any], target: str, suffix: str) -> str | None:
        """Read best-effort metadata key variants."""

        primary = f"{target}{suffix}"
        if primary in metadata:
            return str(metadata[primary])

        if suffix == "Bucket":
            for fallback in ("bucketName", "bucket"):
                if fallback in metadata:
                    return str(metadata[fallback])
        if suffix == "Key":
            for fallback in ("objectKey", "key"):
                if fallback in metadata:
                    return str(metadata[fallback])
        return None


__all__ = ["S3TransferExecutor"]
