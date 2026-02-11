"""Async S3 transfer executor with suspend/resume and multipart support."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Protocol, cast
from urllib.parse import urlparse

from pydantic import ValidationError

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

    def get_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Range: str | None = None,
    ) -> dict[str, Any]:
        """Fetch an object or byte range."""

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
    key: str
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
        s3_client_factory: Callable[
            [str | None, str | None, bool, str | None, str | None, str | None],
            S3Client,
        ]
        | None = None,
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

        size = await self._get_source_size(source_client, session.plan.source)
        use_multipart = (
            session.plan.force_multipart
            or size >= session.plan.multipart_threshold_bytes
            or size > session.plan.multipart_part_size_bytes
        )

        if not use_multipart:
            await self._copy_singlepart(source_client, destination_client, session)
            return

        await self._copy_multipart(source_client, destination_client, session, size)

    async def _copy_singlepart(
        self,
        source_client: S3Client,
        destination_client: S3Client,
        session: TransferSession,
    ) -> None:
        """Copy object using single API call."""

        await self._wait_until_active(session)
        source_payload = await self._read_source_payload(
            source_client,
            session.plan.source,
            part_range=None,
        )
        await asyncio.to_thread(
            destination_client.put_object,
            Bucket=session.plan.destination.bucket,
            Key=session.plan.destination.key,
            Body=source_payload,
        )

    async def _copy_multipart(
        self,
        source_client: S3Client,
        destination_client: S3Client,
        session: TransferSession,
        size: int,
    ) -> None:
        """Copy object with multipart upload and resumable part tracking."""

        if session.upload_id is None:
            response = await asyncio.to_thread(
                destination_client.create_multipart_upload,
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
                        source_client=source_client,
                        destination_client=destination_client,
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
            destination_client.complete_multipart_upload,
            Bucket=session.plan.destination.bucket,
            Key=session.plan.destination.key,
            UploadId=session.upload_id,
            MultipartUpload={"Parts": completed_parts},
        )
        session.upload_id = None

    async def _copy_part(
        self,
        source_client: S3Client,
        destination_client: S3Client,
        session: TransferSession,
        part_number: int,
        part_size: int,
        object_size: int,
    ) -> tuple[int, str]:
        """Copy one multipart segment and return part number + ETag."""

        assert session.upload_id is not None
        start = (part_number - 1) * part_size
        end = min(object_size - 1, start + part_size - 1)

        payload = await self._read_source_payload(
            source_client,
            session.plan.source,
            part_range=(start, end),
        )
        response = await asyncio.to_thread(
            destination_client.upload_part,
            Bucket=session.plan.destination.bucket,
            Key=session.plan.destination.key,
            UploadId=session.upload_id,
            PartNumber=part_number,
            Body=payload,
        )
        return part_number, self._extract_etag(response)

    async def _get_source_size(self, client: S3Client, source: S3ObjectRef) -> int:
        """Read source object size in bytes."""

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

        if session.upload_id is None:
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

        metadata_address = self._pick_data_address(metadata, target)
        if metadata_address is not None:
            return self._parse_data_address(metadata_address)

        bucket = self._pick_value(metadata, target, "Bucket")
        key = self._pick_value(metadata, target, "Key")
        if bucket is None or key is None:
            raise DataFlowValidationError(
                f"Missing {target} bucket/key metadata for S3 transfer execution."
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
        """Convert `DataAddress.endpoint` to bucket/key pair."""

        endpoint = data_address.endpoint
        parsed = urlparse(endpoint)
        scheme = parsed.scheme.lower()
        endpoint_url = self._endpoint_url_from_address(data_address)

        if scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
        elif scheme in {"http", "https"}:
            path_parts = parsed.path.lstrip("/").split("/", 1)
            if len(path_parts) != 2:
                raise DataFlowValidationError(
                    f"Invalid HTTP S3 endpoint '{endpoint}', expected http(s)://host/bucket/key."
                )
            bucket, key = path_parts
            endpoint_url = endpoint_url or f"{parsed.scheme}://{parsed.netloc}"
        else:
            raise DataFlowValidationError(
                f"Unsupported dataAddress endpoint '{endpoint}', expected s3://bucket/key "
                "or http(s)://host/bucket/key."
            )

        if not bucket or not key:
            raise DataFlowValidationError(
                f"Invalid S3 endpoint '{endpoint}', expected s3://bucket/key."
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
