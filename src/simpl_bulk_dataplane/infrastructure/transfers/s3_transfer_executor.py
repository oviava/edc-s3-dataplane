"""Async S3 transfer executor with suspend/resume and multipart support."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Protocol, cast
from urllib.parse import urlparse

from pydantic import ValidationError

from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.errors import DataFlowValidationError
from simpl_bulk_dataplane.domain.monitoring_models import TransferProgressSnapshot
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

    def get_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Range: str | None = None,
    ) -> dict[str, Any]:
        """Fetch an object or byte range."""

    def list_objects_v2(
        self,
        *,
        Bucket: str,
        ContinuationToken: str | None = None,
    ) -> dict[str, Any]:
        """List objects in a bucket."""

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> dict[str, Any]:
        """Upload an object in a single call."""

    def create_multipart_upload(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        """Start multipart upload."""

    def upload_part(
        self,
        *,
        Bucket: str,
        Key: str,
        UploadId: str,
        PartNumber: int,
        Body: bytes,
    ) -> dict[str, Any]:
        """Upload one multipart segment."""

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
    key: str | None
    region: str | None = None
    endpoint_url: str | None = None
    force_path_style: bool = False
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None


@dataclass(slots=True, frozen=True)
class S3TransferPlan:
    """Resolved runtime configuration for an S3 object transfer."""

    source: S3ObjectRef
    destination: S3ObjectRef
    multipart_threshold_bytes: int
    multipart_part_size_bytes: int
    multipart_concurrency: int
    force_multipart: bool


@dataclass(slots=True, frozen=True)
class S3CopyWorkItem:
    """One source object copy step resolved from transfer plan."""

    source_key: str
    destination_key: str
    size_bytes: int


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
    uploaded_part_sizes: dict[int, int] = field(default_factory=dict)
    object_size_bytes: int | None = None
    bytes_transferred: int = 0
    completed_bytes: int = 0
    completed_source_keys: set[str] = field(default_factory=set)
    active_source_key: str | None = None
    active_destination_key: str | None = None
    active_object_size_bytes: int | None = None
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
        s3_client_factory: Callable[
            [str | None, str | None, bool, str | None, str | None, str | None],
            S3Client,
        ]
        | None = None,
        progress_callback: Callable[[str, TransferProgressSnapshot], Awaitable[None]] | None = None,
    ) -> None:
        self._default_region = default_region
        threshold_mb = max(_MIN_MULTIPART_SIZE_MB, multipart_threshold_mb)
        part_size_mb = max(_MIN_MULTIPART_SIZE_MB, multipart_part_size_mb)
        self._multipart_threshold_bytes = threshold_mb * 1024 * 1024
        self._multipart_part_size_bytes = part_size_mb * 1024 * 1024
        self._multipart_concurrency = max(1, multipart_concurrency)
        self._sessions: dict[str, TransferSession] = {}
        self._history: dict[str, TransferProgressSnapshot] = {}
        self._sessions_lock = asyncio.Lock()
        self._s3_client_factory = s3_client_factory or self._build_default_s3_client
        self._progress_callback = progress_callback

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
        await self._emit_progress(data_flow.data_flow_id, self._snapshot_from_session(session))

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

        if session.last_error is None:
            session.last_error = "Transfer terminated by request."
        snapshot = self._snapshot_from_session(session)
        self._remember_snapshot(data_flow.data_flow_id, snapshot)
        await self._emit_progress(data_flow.data_flow_id, snapshot)

    async def complete(self, data_flow: DataFlow) -> None:
        """Wait for active task completion when applicable."""

        session = self._sessions.get(data_flow.data_flow_id)
        if session is None:
            return

        if session.task is not None and not session.task.done():
            await session.task

        if session.finished:
            snapshot = self._snapshot_from_session(session)
            self._remember_snapshot(data_flow.data_flow_id, snapshot)
            await self._emit_progress(data_flow.data_flow_id, snapshot)
            await self._pop_session(data_flow.data_flow_id)

    async def get_progress(self, data_flow_id: str) -> TransferProgressSnapshot | None:
        """Return runtime snapshot for active or recently finished flows."""

        session = self._sessions.get(data_flow_id)
        if session is not None:
            return self._snapshot_from_session(session)
        return self._history.get(data_flow_id)

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
            await self._emit_progress(data_flow_id, self._snapshot_from_session(session))

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
                if session.object_size_bytes is not None:
                    session.bytes_transferred = session.object_size_bytes
                session.last_error = None
            except asyncio.CancelledError:
                session.last_error = "Transfer task cancelled"
                await self._emit_progress(
                    session.data_flow_id,
                    self._snapshot_from_session(session),
                )
                raise
            except Exception as exc:  # noqa: BLE001
                session.last_error = str(exc)
            finally:
                await self._emit_progress(
                    session.data_flow_id,
                    self._snapshot_from_session(session),
                )

    async def _execute_plan(self, session: TransferSession) -> None:
        """Run copy workflow for one object or all objects in a source bucket."""

        source_client = self._s3_client_factory(
            session.plan.source.region or self._default_region,
            session.plan.source.endpoint_url,
            session.plan.source.force_path_style,
            session.plan.source.access_key_id,
            session.plan.source.secret_access_key,
            session.plan.source.session_token,
        )
        destination_client = self._s3_client_factory(
            session.plan.destination.region or self._default_region,
            session.plan.destination.endpoint_url,
            session.plan.destination.force_path_style,
            session.plan.destination.access_key_id,
            session.plan.destination.secret_access_key,
            session.plan.destination.session_token,
        )

        work_items = await self._build_copy_work_items(source_client, session.plan)
        session.object_size_bytes = sum(item.size_bytes for item in work_items)
        size_by_source_key = {item.source_key: item.size_bytes for item in work_items}
        session.completed_bytes = sum(
            size_by_source_key.get(source_key, 0) for source_key in session.completed_source_keys
        )
        session.bytes_transferred = session.completed_bytes + sum(session.uploaded_part_sizes.values())
        await self._emit_progress(session.data_flow_id, self._snapshot_from_session(session))

        for item in work_items:
            if item.source_key in session.completed_source_keys:
                continue
            await self._wait_until_active(session)
            source = self._with_key(session.plan.source, item.source_key)
            destination = self._with_key(session.plan.destination, item.destination_key)
            await self._copy_object(
                source_client=source_client,
                destination_client=destination_client,
                session=session,
                source=source,
                destination=destination,
                size=item.size_bytes,
            )

    async def _copy_object(
        self,
        source_client: S3Client,
        destination_client: S3Client,
        session: TransferSession,
        source: S3ObjectRef,
        destination: S3ObjectRef,
        size: int,
    ) -> None:
        """Copy one object and update aggregate transfer tracking."""

        if source.key is None or destination.key is None:
            raise RuntimeError("Internal transfer plan resolved object without key.")

        self._prepare_active_object_state(
            session=session,
            source_key=source.key,
            destination_key=destination.key,
            size=size,
        )

        use_multipart = (
            session.plan.force_multipart
            or size >= session.plan.multipart_threshold_bytes
            or size > session.plan.multipart_part_size_bytes
        )
        if not use_multipart:
            await self._copy_singlepart(
                source_client=source_client,
                destination_client=destination_client,
                session=session,
                source=source,
                destination=destination,
            )
            self._mark_object_completed(session, source_key=source.key, size=size)
            return

        await self._copy_multipart(
            source_client=source_client,
            destination_client=destination_client,
            session=session,
            source=source,
            destination=destination,
            size=size,
        )
        self._mark_object_completed(session, source_key=source.key, size=size)

    async def _copy_singlepart(
        self,
        source_client: S3Client,
        destination_client: S3Client,
        session: TransferSession,
        source: S3ObjectRef,
        destination: S3ObjectRef,
    ) -> None:
        """Copy object using single API call."""

        if destination.key is None:
            raise RuntimeError("Single-part copy requires destination key.")

        await self._wait_until_active(session)
        source_payload = await self._read_source_payload(
            source_client,
            source,
            part_range=None,
        )
        session.bytes_transferred = session.completed_bytes
        await asyncio.to_thread(
            destination_client.put_object,
            Bucket=destination.bucket,
            Key=destination.key,
            Body=source_payload,
        )
        session.bytes_transferred = session.completed_bytes + len(source_payload)
        await self._emit_progress(session.data_flow_id, self._snapshot_from_session(session))

    async def _copy_multipart(
        self,
        source_client: S3Client,
        destination_client: S3Client,
        session: TransferSession,
        source: S3ObjectRef,
        destination: S3ObjectRef,
        size: int,
    ) -> None:
        """Copy object with multipart upload and resumable part tracking."""

        if destination.key is None:
            raise RuntimeError("Multipart copy requires destination key.")

        if session.upload_id is None:
            response = await asyncio.to_thread(
                destination_client.create_multipart_upload,
                Bucket=destination.bucket,
                Key=destination.key,
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
                        source_client=source_client,
                        destination_client=destination_client,
                        session=session,
                        source=source,
                        destination=destination,
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
            destination_client.complete_multipart_upload,
            Bucket=destination.bucket,
            Key=destination.key,
            UploadId=session.upload_id,
            MultipartUpload={"Parts": completed_parts},
        )
        session.upload_id = None
        session.uploaded_parts.clear()
        session.uploaded_part_sizes.clear()

    async def _copy_part(
        self,
        source_client: S3Client,
        destination_client: S3Client,
        session: TransferSession,
        source: S3ObjectRef,
        destination: S3ObjectRef,
        part_number: int,
        part_size: int,
        object_size: int,
    ) -> tuple[int, str]:
        """Copy one multipart segment and return part number + ETag."""

        if destination.key is None:
            raise RuntimeError("Multipart part copy requires destination key.")

        assert session.upload_id is not None
        start = (part_number - 1) * part_size
        end = min(object_size - 1, start + part_size - 1)

        payload = await self._read_source_payload(
            source_client,
            source,
            part_range=(start, end),
        )
        response = await asyncio.to_thread(
            destination_client.upload_part,
            Bucket=destination.bucket,
            Key=destination.key,
            UploadId=session.upload_id,
            PartNumber=part_number,
            Body=payload,
        )
        session.uploaded_part_sizes[part_number] = len(payload)
        session.bytes_transferred = session.completed_bytes + sum(session.uploaded_part_sizes.values())
        await self._emit_progress(session.data_flow_id, self._snapshot_from_session(session))
        return part_number, self._extract_etag(response)

    async def _build_copy_work_items(
        self,
        source_client: S3Client,
        plan: S3TransferPlan,
    ) -> list[S3CopyWorkItem]:
        """Resolve copy plan to concrete source/destination object pairs."""

        source_key = plan.source.key
        if source_key is not None:
            size = await self._get_source_size(source_client, self._with_key(plan.source, source_key))
            destination_key = self._resolve_destination_key(
                source_key=source_key,
                destination_key=plan.destination.key,
                source_is_bucket=False,
            )
            return [
                S3CopyWorkItem(
                    source_key=source_key,
                    destination_key=destination_key,
                    size_bytes=size,
                )
            ]

        source_objects = await self._list_bucket_objects(source_client, plan.source.bucket)
        work_items = [
            S3CopyWorkItem(
                source_key=source_object_key,
                destination_key=self._resolve_destination_key(
                    source_key=source_object_key,
                    destination_key=plan.destination.key,
                    source_is_bucket=True,
                ),
                size_bytes=size_bytes,
            )
            for source_object_key, size_bytes in source_objects
        ]
        work_items.sort(key=lambda item: item.source_key)
        return work_items

    async def _list_bucket_objects(
        self,
        source_client: S3Client,
        bucket: str,
    ) -> list[tuple[str, int]]:
        """List source bucket objects and their sizes."""

        entries: list[tuple[str, int]] = []
        continuation_token: str | None = None

        while True:
            request_kwargs: dict[str, Any] = {"Bucket": bucket}
            if continuation_token is not None:
                request_kwargs["ContinuationToken"] = continuation_token
            response = await asyncio.to_thread(
                source_client.list_objects_v2,
                **request_kwargs,
            )
            contents = response.get("Contents")
            if isinstance(contents, list):
                for raw_item in contents:
                    if not isinstance(raw_item, dict):
                        continue
                    key = raw_item.get("Key")
                    size = raw_item.get("Size")
                    if not isinstance(key, str) or not key:
                        continue
                    if not isinstance(size, int):
                        size = await self._get_source_size(
                            source_client, self._with_key(S3ObjectRef(bucket=bucket, key=None), key)
                        )
                    entries.append((key, size))

            is_truncated = bool(response.get("IsTruncated"))
            if not is_truncated:
                return entries

            next_token = response.get("NextContinuationToken")
            if not isinstance(next_token, str) or not next_token:
                raise RuntimeError("list_objects_v2 pagination missing NextContinuationToken.")
            continuation_token = next_token

    def _resolve_destination_key(
        self,
        source_key: str,
        destination_key: str | None,
        source_is_bucket: bool,
    ) -> str:
        """Map source key to destination key for object and bucket workflows."""

        if not source_is_bucket:
            return destination_key or source_key
        if destination_key is None:
            return source_key
        normalized_prefix = destination_key.strip("/")
        if not normalized_prefix:
            return source_key
        return f"{normalized_prefix}/{source_key}"

    def _with_key(self, object_ref: S3ObjectRef, key: str) -> S3ObjectRef:
        """Clone object ref while preserving bucket/client configuration."""

        return S3ObjectRef(
            bucket=object_ref.bucket,
            key=key,
            region=object_ref.region,
            endpoint_url=object_ref.endpoint_url,
            force_path_style=object_ref.force_path_style,
            access_key_id=object_ref.access_key_id,
            secret_access_key=object_ref.secret_access_key,
            session_token=object_ref.session_token,
        )

    def _prepare_active_object_state(
        self,
        session: TransferSession,
        source_key: str,
        destination_key: str,
        size: int,
    ) -> None:
        """Initialize per-object state, preserving multipart checkpoints for the same key."""

        same_object = (
            session.active_source_key == source_key
            and session.active_destination_key == destination_key
            and session.active_object_size_bytes == size
        )
        if same_object:
            return

        session.active_source_key = source_key
        session.active_destination_key = destination_key
        session.active_object_size_bytes = size
        session.upload_id = None
        session.uploaded_parts.clear()
        session.uploaded_part_sizes.clear()
        session.bytes_transferred = session.completed_bytes

    def _mark_object_completed(
        self,
        session: TransferSession,
        source_key: str,
        size: int,
    ) -> None:
        """Finalize one object copy and reset per-object state."""

        session.completed_source_keys.add(source_key)
        session.completed_bytes += size
        session.bytes_transferred = session.completed_bytes
        session.active_source_key = None
        session.active_destination_key = None
        session.active_object_size_bytes = None
        session.upload_id = None
        session.uploaded_parts.clear()
        session.uploaded_part_sizes.clear()

    async def _get_source_size(self, client: S3Client, source: S3ObjectRef) -> int:
        """Read source object size in bytes."""

        if source.key is None:
            raise RuntimeError("Source key is required for head_object.")
        response = await asyncio.to_thread(client.head_object, Bucket=source.bucket, Key=source.key)
        size = response.get("ContentLength")
        if not isinstance(size, int):
            raise RuntimeError("head_object did not return ContentLength")
        return size

    async def _read_source_payload(
        self,
        client: S3Client,
        source: S3ObjectRef,
        part_range: tuple[int, int] | None,
    ) -> bytes:
        """Read source bytes for single-part or one multipart segment."""

        if source.key is None:
            raise RuntimeError("Source key is required for get_object.")
        range_header: str | None = None
        if part_range is not None:
            start, end = part_range
            range_header = f"bytes={start}-{end}"

        response = await asyncio.to_thread(
            client.get_object,
            Bucket=source.bucket,
            Key=source.key,
            Range=range_header,
        )
        body = response.get("Body")
        if body is None:
            raise RuntimeError("get_object did not return Body")
        payload = await asyncio.to_thread(self._read_stream_body, body)
        if not isinstance(payload, bytes):
            raise RuntimeError("get_object Body did not resolve to bytes")
        return payload

    def _read_stream_body(self, body: Any) -> bytes:
        """Normalize S3 response body content to bytes."""

        if isinstance(body, bytes):
            return body
        if hasattr(body, "read"):
            payload = body.read()
            if isinstance(payload, bytes):
                return payload
            raise RuntimeError("S3 response body .read() did not return bytes")
        raise RuntimeError("S3 response body is not readable")

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

        if session.upload_id is None or session.active_destination_key is None:
            return

        client = self._s3_client_factory(
            session.plan.destination.region or self._default_region,
            session.plan.destination.endpoint_url,
            session.plan.destination.force_path_style,
            session.plan.destination.access_key_id,
            session.plan.destination.secret_access_key,
            session.plan.destination.session_token,
        )
        with suppress(Exception):
            await asyncio.to_thread(
                client.abort_multipart_upload,
                Bucket=session.plan.destination.bucket,
                Key=session.active_destination_key,
                UploadId=session.upload_id,
            )

    async def _pop_session(self, data_flow_id: str) -> TransferSession | None:
        """Remove a transfer session atomically."""

        async with self._sessions_lock:
            return self._sessions.pop(data_flow_id, None)

    def _snapshot_from_session(self, session: TransferSession) -> TransferProgressSnapshot:
        """Build a read-safe progress payload from runtime state."""

        task_running = session.task is not None and not session.task.done() and not session.finished
        return TransferProgressSnapshot(
            bytes_total=session.object_size_bytes,
            bytes_transferred=session.bytes_transferred,
            running=task_running,
            paused=(task_running and not session.pause_event.is_set()),
            finished=session.finished,
            last_error=session.last_error,
        )

    def _remember_snapshot(self, data_flow_id: str, snapshot: TransferProgressSnapshot) -> None:
        """Store latest snapshot for completed/terminated flows."""

        self._history[data_flow_id] = snapshot

    async def _emit_progress(self, data_flow_id: str, snapshot: TransferProgressSnapshot) -> None:
        """Emit progress callbacks in best-effort mode."""

        if self._progress_callback is None:
            return
        with suppress(Exception):
            await self._progress_callback(data_flow_id, snapshot)

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
        """Resolve S3 location from dataAddress or metadata.

        Priority:
        1. `dataAddress.endpoint` when present and S3-formatted (`s3://bucket[/key]`).
        2. metadata keys like `<target>Bucket`/`<target>Key`.
        """

        if data_address is not None:
            return self._parse_data_address(data_address)

        metadata_address = self._pick_data_address(metadata, target)
        if metadata_address is not None:
            return self._parse_data_address(metadata_address)

        bucket = self._pick_value(metadata, target, "Bucket")
        key = self._pick_value(metadata, target, "Key")
        if bucket is None:
            raise DataFlowValidationError(
                f"Missing {target} bucket metadata for S3 transfer execution."
            )

        access_key_id = self._pick_access_key_id(metadata, target)
        secret_access_key = self._pick_secret_access_key(metadata, target)
        session_token = self._pick_session_token(metadata, target)
        self._validate_static_credentials(
            access_key_id,
            secret_access_key,
            source=f"metadata[{target}]",
        )

        return S3ObjectRef(
            bucket=bucket,
            key=key,
            region=self._pick_region(metadata, target),
            endpoint_url=self._pick_endpoint_url(metadata, target),
            force_path_style=self._pick_force_path_style(metadata, target),
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
        )

    def _parse_data_address(self, data_address: DataAddress) -> S3ObjectRef:
        """Convert `DataAddress.endpoint` to bucket/key pair with optional key."""

        endpoint = data_address.endpoint
        parsed = urlparse(endpoint)
        scheme = parsed.scheme.lower()
        endpoint_url = self._endpoint_url_from_address(data_address)

        if scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path.lstrip("/") or None
        elif scheme in {"http", "https"}:
            path_parts = parsed.path.lstrip("/").split("/", 1)
            if len(path_parts) == 1:
                bucket = path_parts[0]
                key = None
            elif len(path_parts) == 2:
                bucket, key = path_parts
                key = key or None
            else:
                raise DataFlowValidationError(
                    f"Invalid HTTP S3 endpoint '{endpoint}', expected http(s)://host/bucket[/key]."
                )
            endpoint_url = endpoint_url or f"{parsed.scheme}://{parsed.netloc}"
        else:
            raise DataFlowValidationError(
                f"Unsupported dataAddress endpoint '{endpoint}', expected s3://bucket[/key] "
                "or http(s)://host/bucket[/key]."
            )

        if not bucket:
            raise DataFlowValidationError(
                f"Invalid S3 endpoint '{endpoint}', expected s3://bucket[/key]."
            )

        access_key_id, secret_access_key, session_token = self._credentials_from_address(
            data_address
        )

        return S3ObjectRef(
            bucket=bucket,
            key=key,
            region=self._region_from_address(data_address),
            endpoint_url=endpoint_url,
            force_path_style=self._force_path_style_from_address(data_address),
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
        )

    def _region_from_address(self, data_address: DataAddress) -> str | None:
        """Extract region from endpoint properties when available."""

        for prop in data_address.endpoint_properties:
            if prop.name.lower() == "region":
                return prop.value
        return None

    def _endpoint_url_from_address(self, data_address: DataAddress) -> str | None:
        """Extract endpoint URL override from endpoint properties."""

        for prop in data_address.endpoint_properties:
            if prop.name.lower() in {"endpointurl", "s3endpointurl"}:
                return prop.value
        return None

    def _force_path_style_from_address(self, data_address: DataAddress) -> bool:
        """Extract path-style flag from endpoint properties."""

        for prop in data_address.endpoint_properties:
            if prop.name.lower() == "forcepathstyle":
                return self._coerce_bool(prop.value)
        return False

    def _credentials_from_address(
        self,
        data_address: DataAddress,
    ) -> tuple[str | None, str | None, str | None]:
        """Extract static AWS credentials from endpoint properties when provided."""

        property_map = {
            prop.name.lower(): prop.value for prop in data_address.endpoint_properties
        }
        access_key_id = property_map.get("accesskeyid") or property_map.get("awsaccesskeyid")
        secret_access_key = property_map.get("secretaccesskey") or property_map.get(
            "awssecretaccesskey"
        )
        session_token = property_map.get("sessiontoken") or property_map.get("awssessiontoken")

        self._validate_static_credentials(
            access_key_id,
            secret_access_key,
            source="DataAddress.endpointProperties",
        )
        return access_key_id, secret_access_key, session_token

    def _validate_static_credentials(
        self,
        access_key_id: str | None,
        secret_access_key: str | None,
        source: str,
    ) -> None:
        """Ensure static credentials are either fully provided or omitted."""

        if (access_key_id is None) == (secret_access_key is None):
            return
        raise DataFlowValidationError(
            f"{source} must include both accessKeyId and secretAccessKey when using "
            "static credentials."
        )

    def _build_data_address(self, metadata: dict[str, Any], target: str) -> DataAddress | None:
        """Construct an S3 DataAddress from metadata."""

        metadata_address = self._pick_data_address(metadata, target)
        if metadata_address is not None:
            return metadata_address

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
        if endpoint_url := self._pick_endpoint_url(metadata, target):
            endpoint_properties.append(
                EndpointProperty(name="endpointUrl", value=endpoint_url),
            )
        if self._pick_force_path_style(metadata, target):
            endpoint_properties.append(
                EndpointProperty(name="forcePathStyle", value="true"),
            )
        if access_key_id := self._pick_access_key_id(metadata, target):
            endpoint_properties.append(
                EndpointProperty(name="accessKeyId", value=access_key_id),
            )
        if secret_access_key := self._pick_secret_access_key(metadata, target):
            endpoint_properties.append(
                EndpointProperty(name="secretAccessKey", value=secret_access_key),
            )
        if session_token := self._pick_session_token(metadata, target):
            endpoint_properties.append(
                EndpointProperty(name="sessionToken", value=session_token),
            )

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

    def _pick_endpoint_url(self, metadata: dict[str, Any], target: str) -> str | None:
        """Read best-effort endpoint URL metadata key variants."""

        for key in (f"{target}EndpointUrl", f"{target}S3EndpointUrl"):
            if key in metadata:
                return str(metadata[key])
        if "endpointUrl" in metadata:
            return str(metadata["endpointUrl"])
        return None

    def _pick_force_path_style(self, metadata: dict[str, Any], target: str) -> bool:
        """Read best-effort path-style setting from metadata."""

        for key in (f"{target}ForcePathStyle", "forcePathStyle"):
            if key in metadata:
                return self._coerce_bool(metadata[key])
        return False

    def _pick_access_key_id(self, metadata: dict[str, Any], target: str) -> str | None:
        """Read best-effort access key metadata key variants."""

        for key in (f"{target}AccessKeyId", "accessKeyId", "awsAccessKeyId"):
            if key in metadata:
                return str(metadata[key])
        return None

    def _pick_secret_access_key(self, metadata: dict[str, Any], target: str) -> str | None:
        """Read best-effort secret key metadata key variants."""

        for key in (f"{target}SecretAccessKey", "secretAccessKey", "awsSecretAccessKey"):
            if key in metadata:
                return str(metadata[key])
        return None

    def _pick_session_token(self, metadata: dict[str, Any], target: str) -> str | None:
        """Read best-effort session token metadata key variants."""

        for key in (f"{target}SessionToken", "sessionToken", "awsSessionToken"):
            if key in metadata:
                return str(metadata[key])
        return None

    def _pick_data_address(
        self,
        metadata: dict[str, Any],
        target: str,
    ) -> DataAddress | None:
        """Read an optional target-specific DataAddress from metadata."""

        key = f"{target}DataAddress"
        raw = metadata.get(key)
        if raw is None:
            return None
        if isinstance(raw, DataAddress):
            return raw
        if isinstance(raw, dict):
            try:
                return DataAddress.model_validate(raw)
            except ValidationError as exc:
                raise DataFlowValidationError(
                    f"Invalid metadata['{key}'] DataAddress payload: {exc}"
                ) from exc
        raise DataFlowValidationError(
            f"metadata['{key}'] must be an object matching DataAddress."
        )

    def _pick_value(self, metadata: dict[str, Any], target: str, suffix: str) -> str | None:
        """Read best-effort metadata key variants."""

        primary = f"{target}{suffix}"
        if primary in metadata:
            return self._optional_str(metadata[primary])

        if suffix == "Bucket":
            for fallback in ("bucketName", "bucket"):
                if fallback in metadata:
                    return self._optional_str(metadata[fallback])
        if suffix == "Key":
            for fallback in ("objectKey", "key"):
                if fallback in metadata:
                    return self._optional_str(metadata[fallback])
        return None

    def _optional_str(self, raw: object) -> str | None:
        """Normalize metadata values to optional non-empty strings."""

        if raw is None:
            return None
        text = str(raw).strip()
        return text or None

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

    def _coerce_bool(self, raw: object) -> bool:
        """Convert flexible values to boolean."""

        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            return raw.strip().lower() in {"1", "true", "yes", "on"}
        if isinstance(raw, int):
            return raw != 0
        return False

    def _extract_etag(self, response: dict[str, Any]) -> str:
        """Extract part ETag from multipart upload response."""

        etag = response.get("ETag")
        if not isinstance(etag, str):
            copy_part_result = cast(dict[str, Any] | None, response.get("CopyPartResult"))
            if copy_part_result is not None:
                etag = copy_part_result.get("ETag")
        if not isinstance(etag, str) or not etag:
            raise RuntimeError("Multipart part upload did not return ETag")
        return etag

    def _build_default_s3_client(
        self,
        region: str | None,
        endpoint_url: str | None,
        force_path_style: bool,
        access_key_id: str | None,
        secret_access_key: str | None,
        session_token: str | None,
    ) -> S3Client:
        """Create a boto3 S3 client lazily to avoid import-time hard dependency."""

        try:
            import boto3  # type: ignore[import-not-found]
            from botocore.config import Config
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "boto3 is required for S3 transfer execution. Install project dependencies first."
            ) from exc

        resolved_region = region or self._default_region
        client_kwargs: dict[str, object] = {"region_name": resolved_region}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        if force_path_style:
            client_kwargs["config"] = Config(s3={"addressing_style": "path"})
        if access_key_id is not None:
            client_kwargs["aws_access_key_id"] = access_key_id
        if secret_access_key is not None:
            client_kwargs["aws_secret_access_key"] = secret_access_key
        if session_token is not None:
            client_kwargs["aws_session_token"] = session_token

        client = boto3.client("s3", **client_kwargs)
        return cast(S3Client, client)


__all__ = ["S3TransferExecutor"]
