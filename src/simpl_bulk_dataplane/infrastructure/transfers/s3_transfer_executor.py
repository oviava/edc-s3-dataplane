"""Async S3 transfer executor with suspend/resume and multipart support."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Protocol, cast
from urllib.parse import urlparse

from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.errors import DataFlowValidationError
from simpl_bulk_dataplane.domain.ports import TransferExecutor
from simpl_bulk_dataplane.domain.signaling_models import (
    DataAddress,
    DataFlowPrepareMessage,
    DataFlowStartedNotificationMessage,
    DataFlowStartMessage,
    EndpointProperty,
)
from simpl_bulk_dataplane.domain.transfer_types import TransferMode

_MIN_MULTIPART_SIZE_MB = 5
_DEFAULT_MULTIPART_THRESHOLD_MB = 8
_DEFAULT_MULTIPART_PART_SIZE_MB = 8
_DEFAULT_MULTIPART_CONCURRENCY = 4


class S3Client(Protocol):
    """Subset of S3 client operations used by the transfer executor."""

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        """Return object metadata."""

    def copy_object(self, *, Bucket: str, Key: str, CopySource: dict[str, str]) -> dict[str, Any]:
        """Copy an object without multipart."""

    def create_multipart_upload(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        """Start multipart upload."""

    def upload_part_copy(
        self,
        *,
        Bucket: str,
        Key: str,
        UploadId: str,
        PartNumber: int,
        CopySource: dict[str, str],
        CopySourceRange: str,
    ) -> dict[str, Any]:
        """Copy one multipart segment."""

    def complete_multipart_upload(
        self,
        *,
        Bucket: str,
        Key: str,
        UploadId: str,
        MultipartUpload: dict[str, list[dict[str, str | int]]],
    ) -> dict[str, Any]:
        """Finalize multipart upload."""

    def abort_multipart_upload(self, *, Bucket: str, Key: str, UploadId: str) -> dict[str, Any]:
        """Abort multipart upload."""


@dataclass(slots=True, frozen=True)
class S3ObjectRef:
    """Canonical S3 object location."""

    bucket: str
    key: str
    region: str | None = None


@dataclass(slots=True, frozen=True)
class S3TransferPlan:
    """Resolved runtime configuration for an S3 object transfer."""

    source: S3ObjectRef
    destination: S3ObjectRef
    multipart_threshold_bytes: int
    multipart_part_size_bytes: int
    multipart_concurrency: int
    force_multipart: bool


@dataclass(slots=True)
class TransferSession:
    """Runtime state for one background S3 transfer."""

    data_flow_id: str
    plan: S3TransferPlan
    pause_event: asyncio.Event = field(default_factory=asyncio.Event)
    terminate_event: asyncio.Event = field(default_factory=asyncio.Event)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    task: asyncio.Task[None] | None = None
    upload_id: str | None = None
    uploaded_parts: dict[int, str] = field(default_factory=dict)
    finished: bool = False
    last_error: str | None = None

    def __post_init__(self) -> None:
        self.pause_event.set()


class S3TransferExecutor(TransferExecutor):
    """Transfer adapter that performs async S3 copy operations.

    - `start` triggers provider-push copies asynchronously.
    - `notify_started` triggers consumer-pull copies asynchronously.
    - `suspend`/`start` act as pause/resume for active sessions.
    - Large objects use multipart copy with persisted part tracking in-memory.
    """

    def __init__(
        self,
        default_region: str = "us-east-1",
        multipart_threshold_mb: int = _DEFAULT_MULTIPART_THRESHOLD_MB,
        multipart_part_size_mb: int = _DEFAULT_MULTIPART_PART_SIZE_MB,
        multipart_concurrency: int = _DEFAULT_MULTIPART_CONCURRENCY,
        s3_client_factory: Callable[[str | None], S3Client] | None = None,
    ) -> None:
        self._default_region = default_region
        threshold_mb = max(_MIN_MULTIPART_SIZE_MB, multipart_threshold_mb)
        part_size_mb = max(_MIN_MULTIPART_SIZE_MB, multipart_part_size_mb)
        self._multipart_threshold_bytes = threshold_mb * 1024 * 1024
        self._multipart_part_size_bytes = part_size_mb * 1024 * 1024
        self._multipart_concurrency = max(1, multipart_concurrency)
        self._sessions: dict[str, TransferSession] = {}
        self._sessions_lock = asyncio.Lock()
        self._s3_client_factory = s3_client_factory or self._build_default_s3_client

    async def prepare(
        self, data_flow: DataFlow, message: DataFlowPrepareMessage
    ) -> DataAddress | None:
        """Prepare transfer artifacts and return address hints when needed."""

        if data_flow.transfer_mode is TransferMode.PUSH:
            return self._build_data_address(message.metadata, target="destination")
        return None

    async def start(self, data_flow: DataFlow, message: DataFlowStartMessage) -> DataAddress | None:
        """Start provider-side signaling behavior.

        PUSH: schedule object copy from source to destination asynchronously.
        PULL: return source address for consumer retrieval.
        """

        if data_flow.transfer_mode is TransferMode.PULL:
            return self._build_data_address(message.metadata, target="source")

        if message.data_address is None:
            raise DataFlowValidationError("Push transfers require a destination dataAddress.")

        source = self._resolve_object_ref(
            metadata=message.metadata,
            target="source",
            data_address=None,
        )
        destination = self._resolve_object_ref(
            metadata=message.metadata,
            target="destination",
            data_address=message.data_address,
        )
        plan = self._build_plan(message.metadata, source, destination)
        await self._start_or_resume(data_flow.data_flow_id, plan)
        return None

    async def notify_started(
        self,
        data_flow: DataFlow,
        message: DataFlowStartedNotificationMessage | None,
    ) -> None:
        """Start or resume consumer-side pull execution once source is known."""

        if data_flow.transfer_mode is TransferMode.PUSH:
            return

        address = None if message is None else message.data_address
        if address is None:
            raise DataFlowValidationError(
                "Consumer-pull /started requires dataAddress containing the source S3 object."
            )

        source = self._resolve_object_ref(
            metadata=data_flow.metadata,
            target="source",
            data_address=address,
        )
        destination = self._resolve_object_ref(
            metadata=data_flow.metadata,
            target="destination",
            data_address=None,
        )
        plan = self._build_plan(data_flow.metadata, source, destination)
        await self._start_or_resume(data_flow.data_flow_id, plan)

    async def suspend(self, data_flow: DataFlow, reason: str | None) -> None:
        """Pause active transfer session if present."""

        _ = reason
        session = self._sessions.get(data_flow.data_flow_id)
        if session is None or session.finished:
            return
        session.pause_event.clear()

    async def terminate(self, data_flow: DataFlow, reason: str | None) -> None:
        """Stop transfer and abort multipart state."""

        _ = reason
        session = await self._pop_session(data_flow.data_flow_id)
        if session is None:
            return

        session.terminate_event.set()
        session.pause_event.set()

        if session.task is not None and not session.task.done():
            session.task.cancel()
            with suppress(asyncio.CancelledError):
                await session.task

        if session.upload_id is not None and not session.finished:
            await self._abort_multipart(session)

    async def complete(self, data_flow: DataFlow) -> None:
        """Wait for active task completion when applicable."""

        session = self._sessions.get(data_flow.data_flow_id)
        if session is None:
            return

        if session.task is not None and not session.task.done():
            await session.task

        if session.finished:
            await self._pop_session(data_flow.data_flow_id)

    async def _start_or_resume(self, data_flow_id: str, plan: S3TransferPlan) -> None:
        """Start background task or resume a paused one."""

        async with self._sessions_lock:
            session = self._sessions.get(data_flow_id)
            if session is None:
                session = TransferSession(data_flow_id=data_flow_id, plan=plan)
                self._sessions[data_flow_id] = session
            else:
                session.plan = plan

            session.pause_event.set()
            session.terminate_event.clear()

            if session.finished:
                return

            if session.task is None or session.task.done():
                session.task = asyncio.create_task(
                    self._run_transfer(session),
                    name=f"s3-transfer-{data_flow_id}",
                )

    async def _run_transfer(self, session: TransferSession) -> None:
        """Run one copy workflow in background."""

        async with session.lock:
            try:
                await self._execute_plan(session)
                session.finished = True
                session.last_error = None
            except asyncio.CancelledError:
                session.last_error = "Transfer task cancelled"
                raise
            except Exception as exc:  # noqa: BLE001
                session.last_error = str(exc)

    async def _execute_plan(self, session: TransferSession) -> None:
        """Run copy workflow using multipart when object size requires it."""

        client = self._s3_client_factory(session.plan.destination.region or self._default_region)
        size = await self._get_source_size(client, session.plan.source)
        use_multipart = (
            session.plan.force_multipart
            or size >= session.plan.multipart_threshold_bytes
            or size > session.plan.multipart_part_size_bytes
        )

        if not use_multipart:
            await self._copy_singlepart(client, session)
            return

        await self._copy_multipart(client, session, size)

    async def _copy_singlepart(self, client: S3Client, session: TransferSession) -> None:
        """Copy object using single API call."""

        await self._wait_until_active(session)
        await asyncio.to_thread(
            client.copy_object,
            Bucket=session.plan.destination.bucket,
            Key=session.plan.destination.key,
            CopySource={
                "Bucket": session.plan.source.bucket,
                "Key": session.plan.source.key,
            },
        )

    async def _copy_multipart(self, client: S3Client, session: TransferSession, size: int) -> None:
        """Copy object with multipart upload and resumable part tracking."""

        if session.upload_id is None:
            response = await asyncio.to_thread(
                client.create_multipart_upload,
                Bucket=session.plan.destination.bucket,
                Key=session.plan.destination.key,
            )
            upload_id = response.get("UploadId")
            if not isinstance(upload_id, str) or not upload_id:
                raise RuntimeError("create_multipart_upload did not return UploadId")
            session.upload_id = upload_id

        assert session.upload_id is not None
        part_size = session.plan.multipart_part_size_bytes
        total_parts = math.ceil(size / part_size)
        pending_parts = [
            part_number
            for part_number in range(1, total_parts + 1)
            if part_number not in session.uploaded_parts
        ]

        for index in range(0, len(pending_parts), session.plan.multipart_concurrency):
            await self._wait_until_active(session)
            batch = pending_parts[index : index + session.plan.multipart_concurrency]
            results = await asyncio.gather(
                *[
                    self._copy_part(
                        client=client,
                        session=session,
                        part_number=part_number,
                        part_size=part_size,
                        object_size=size,
                    )
                    for part_number in batch
                ]
            )
            for part_number, etag in results:
                session.uploaded_parts[part_number] = etag

        completed_parts = [
            {"PartNumber": number, "ETag": etag}
            for number, etag in sorted(session.uploaded_parts.items())
        ]
        await self._wait_until_active(session)
        await asyncio.to_thread(
            client.complete_multipart_upload,
            Bucket=session.plan.destination.bucket,
            Key=session.plan.destination.key,
            UploadId=session.upload_id,
            MultipartUpload={"Parts": completed_parts},
        )
        session.upload_id = None

    async def _copy_part(
        self,
        client: S3Client,
        session: TransferSession,
        part_number: int,
        part_size: int,
        object_size: int,
    ) -> tuple[int, str]:
        """Copy one multipart segment and return part number + ETag."""

        assert session.upload_id is not None
        start = (part_number - 1) * part_size
        end = min(object_size - 1, start + part_size - 1)

        response = await asyncio.to_thread(
            client.upload_part_copy,
            Bucket=session.plan.destination.bucket,
            Key=session.plan.destination.key,
            UploadId=session.upload_id,
            PartNumber=part_number,
            CopySource={
                "Bucket": session.plan.source.bucket,
                "Key": session.plan.source.key,
            },
            CopySourceRange=f"bytes={start}-{end}",
        )
        return part_number, self._extract_etag(response)

    async def _get_source_size(self, client: S3Client, source: S3ObjectRef) -> int:
        """Read source object size in bytes."""

        response = await asyncio.to_thread(client.head_object, Bucket=source.bucket, Key=source.key)
        size = response.get("ContentLength")
        if not isinstance(size, int):
            raise RuntimeError("head_object did not return ContentLength")
        return size

    async def _wait_until_active(self, session: TransferSession) -> None:
        """Pause transfer loop while suspended."""

        while True:
            if session.terminate_event.is_set():
                raise asyncio.CancelledError
            if session.pause_event.is_set():
                return
            await asyncio.sleep(0.05)

    async def _abort_multipart(self, session: TransferSession) -> None:
        """Abort in-flight multipart upload on terminate."""

        if session.upload_id is None:
            return

        client = self._s3_client_factory(session.plan.destination.region or self._default_region)
        with suppress(Exception):
            await asyncio.to_thread(
                client.abort_multipart_upload,
                Bucket=session.plan.destination.bucket,
                Key=session.plan.destination.key,
                UploadId=session.upload_id,
            )

    async def _pop_session(self, data_flow_id: str) -> TransferSession | None:
        """Remove a transfer session atomically."""

        async with self._sessions_lock:
            return self._sessions.pop(data_flow_id, None)

    def _build_plan(
        self,
        metadata: dict[str, Any],
        source: S3ObjectRef,
        destination: S3ObjectRef,
    ) -> S3TransferPlan:
        """Map metadata + defaults to transfer settings."""

        threshold_mb = self._read_positive_int(
            metadata,
            key="multipartThresholdMb",
            fallback=self._multipart_threshold_bytes // (1024 * 1024),
        )
        part_size_mb = self._read_positive_int(
            metadata,
            key="multipartPartSizeMb",
            fallback=self._multipart_part_size_bytes // (1024 * 1024),
        )
        part_size_mb = max(_MIN_MULTIPART_SIZE_MB, part_size_mb)
        threshold_mb = max(_MIN_MULTIPART_SIZE_MB, threshold_mb)

        concurrency = self._read_positive_int(
            metadata,
            key="multipartConcurrency",
            fallback=self._multipart_concurrency,
        )

        return S3TransferPlan(
            source=source,
            destination=destination,
            multipart_threshold_bytes=threshold_mb * 1024 * 1024,
            multipart_part_size_bytes=part_size_mb * 1024 * 1024,
            multipart_concurrency=max(1, concurrency),
            force_multipart=bool(metadata.get("forceMultipart") is True),
        )

    def _resolve_object_ref(
        self,
        metadata: dict[str, Any],
        target: str,
        data_address: DataAddress | None,
    ) -> S3ObjectRef:
        """Resolve object location from dataAddress or metadata.

        Priority:
        1. `dataAddress.endpoint` when present and S3-formatted.
        2. metadata keys like `<target>Bucket`/`<target>Key`.
        """

        if data_address is not None:
            return self._parse_data_address(data_address)

        bucket = self._pick_value(metadata, target, "Bucket")
        key = self._pick_value(metadata, target, "Key")
        if bucket is None or key is None:
            raise DataFlowValidationError(
                f"Missing {target} bucket/key metadata for S3 transfer execution."
            )

        return S3ObjectRef(
            bucket=bucket,
            key=key,
            region=self._pick_region(metadata, target),
        )

    def _parse_data_address(self, data_address: DataAddress) -> S3ObjectRef:
        """Convert `DataAddress.endpoint` to bucket/key pair."""

        endpoint = data_address.endpoint
        parsed = urlparse(endpoint)
        if parsed.scheme.lower() != "s3":
            raise DataFlowValidationError(
                f"Unsupported dataAddress endpoint '{endpoint}', expected s3://bucket/key."
            )

        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise DataFlowValidationError(
                f"Invalid S3 endpoint '{endpoint}', expected s3://bucket/key."
            )

        return S3ObjectRef(bucket=bucket, key=key, region=self._region_from_address(data_address))

    def _region_from_address(self, data_address: DataAddress) -> str | None:
        """Extract region from endpoint properties when available."""

        for prop in data_address.endpoint_properties:
            if prop.name.lower() == "region":
                return prop.value
        return None

    def _build_data_address(self, metadata: dict[str, Any], target: str) -> DataAddress | None:
        """Construct an S3 DataAddress from metadata."""

        bucket = self._pick_value(metadata, target, "Bucket")
        if bucket is None:
            return None

        key = self._pick_value(metadata, target, "Key")
        endpoint = f"s3://{bucket}" if key is None else f"s3://{bucket}/{key}"

        endpoint_properties: list[EndpointProperty] = [
            EndpointProperty(name="storageType", value="s3"),
        ]
        if region := self._pick_region(metadata, target):
            endpoint_properties.append(EndpointProperty(name="region", value=region))

        return DataAddress(
            type_="DataAddress",
            endpoint_type="urn:aws:s3",
            endpoint=endpoint,
            endpoint_properties=endpoint_properties,
        )

    def _pick_region(self, metadata: dict[str, Any], target: str) -> str | None:
        """Read best-effort region metadata key variants."""

        key = f"{target}Region"
        if key in metadata:
            return str(metadata[key])
        if "region" in metadata:
            return str(metadata["region"])
        return None

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

    def _read_positive_int(self, metadata: dict[str, Any], key: str, fallback: int) -> int:
        """Parse positive integer metadata values with fallback."""

        raw = metadata.get(key)
        if raw is None:
            return fallback

        try:
            parsed = int(raw)
        except (TypeError, ValueError):
            return fallback
        return fallback if parsed <= 0 else parsed

    def _extract_etag(self, response: dict[str, Any]) -> str:
        """Extract part ETag from upload_part_copy response."""

        copy_part_result = cast(dict[str, Any] | None, response.get("CopyPartResult"))
        if copy_part_result is None:
            raise RuntimeError("upload_part_copy did not return CopyPartResult")

        etag = copy_part_result.get("ETag")
        if not isinstance(etag, str) or not etag:
            raise RuntimeError("upload_part_copy did not return CopyPartResult.ETag")
        return etag

    def _build_default_s3_client(self, region: str | None) -> S3Client:
        """Create a boto3 S3 client lazily to avoid import-time hard dependency."""

        try:
            import boto3  # type: ignore[import-not-found]
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "boto3 is required for S3 transfer execution. Install project dependencies first."
            ) from exc

        resolved_region = region or self._default_region
        client = boto3.client("s3", region_name=resolved_region)
        return cast(S3Client, client)


__all__ = ["S3TransferExecutor"]
