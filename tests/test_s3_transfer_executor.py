from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.signaling_models import (
    DataAddress,
    DataFlowStartMessage,
    EndpointProperty,
)
from simpl_bulk_dataplane.domain.transfer_types import DataFlowState, TransferMode
from simpl_bulk_dataplane.infrastructure.transfers import S3TransferExecutor


class FakeS3Client:
    """Thread-safe fake S3 client used by transfer executor tests."""

    def __init__(
        self,
        object_sizes: dict[tuple[str, str], int],
        part_delay_seconds: float = 0.0,
        put_delay_seconds: float = 0.0,
    ) -> None:
        self._objects = {
            object_ref: b"x" * size for object_ref, size in object_sizes.items()
        }
        self._part_delay_seconds = part_delay_seconds
        self._put_delay_seconds = put_delay_seconds
        self._upload_counter = 0
        self._multipart_uploads: dict[str, tuple[str, str, dict[int, bytes]]] = {}
        self._lock = threading.Lock()
        self.put_calls = 0
        self.copy_calls = 0
        self.get_calls = 0
        self.multipart_upload_calls = 0
        self.upload_part_calls = 0
        self.upload_part_copy_calls = 0
        self.uploaded_part_numbers: list[int] = []
        self.complete_calls = 0
        self.abort_calls = 0
        self.list_calls = 0

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        return {"ContentLength": len(self._objects[(Bucket, Key)])}

    def get_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Range: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            self.get_calls += 1
            payload = self._objects[(Bucket, Key)]

        if Range is None:
            return {"Body": payload}

        prefix = "bytes="
        if not Range.startswith(prefix):
            raise ValueError(f"Unsupported range: {Range}")
        start_str, end_str = Range[len(prefix) :].split("-", 1)
        start = int(start_str)
        end = int(end_str)
        return {"Body": payload[start : end + 1]}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> dict[str, Any]:
        if self._put_delay_seconds:
            time.sleep(self._put_delay_seconds)
        with self._lock:
            self.put_calls += 1
            self._objects[(Bucket, Key)] = Body
        return {"ETag": "etag-single-part"}

    def copy_object(
        self,
        *,
        Bucket: str,
        Key: str,
        CopySource: dict[str, str],
    ) -> dict[str, Any]:
        if self._put_delay_seconds:
            time.sleep(self._put_delay_seconds)
        source_bucket, source_key = self._parse_copy_source(CopySource)
        with self._lock:
            self.copy_calls += 1
            self._objects[(Bucket, Key)] = self._objects[(source_bucket, source_key)]
        return {"CopyObjectResult": {"ETag": "etag-copy"}}

    def list_objects_v2(
        self,
        *,
        Bucket: str,
        ContinuationToken: str | None = None,
    ) -> dict[str, Any]:
        page_size = 2
        with self._lock:
            self.list_calls += 1
            keys = sorted(key for bucket, key in self._objects if bucket == Bucket)

        start = 0 if ContinuationToken is None else int(ContinuationToken)
        page = keys[start : start + page_size]
        contents = [
            {"Key": key, "Size": len(self._objects[(Bucket, key)])}
            for key in page
        ]
        next_index = start + page_size
        is_truncated = next_index < len(keys)
        response: dict[str, Any] = {
            "Contents": contents,
            "IsTruncated": is_truncated,
        }
        if is_truncated:
            response["NextContinuationToken"] = str(next_index)
        return response

    def create_multipart_upload(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        with self._lock:
            self.multipart_upload_calls += 1
            self._upload_counter += 1
            upload_id = f"upload-{self._upload_counter}"
            self._multipart_uploads[upload_id] = (Bucket, Key, {})
        return {"UploadId": upload_id}

    def upload_part(
        self,
        *,
        Bucket: str,
        Key: str,
        UploadId: str,
        PartNumber: int,
        Body: bytes,
    ) -> dict[str, Any]:
        if self._part_delay_seconds:
            time.sleep(self._part_delay_seconds)
        with self._lock:
            self.upload_part_calls += 1
            upload_bucket, upload_key, parts = self._multipart_uploads[UploadId]
            assert upload_bucket == Bucket
            assert upload_key == Key
            parts[PartNumber] = Body
            self.uploaded_part_numbers.append(PartNumber)
        return {"ETag": f"etag-{PartNumber}"}

    def upload_part_copy(
        self,
        *,
        Bucket: str,
        Key: str,
        UploadId: str,
        PartNumber: int,
        CopySource: dict[str, str],
        CopySourceRange: str | None = None,
    ) -> dict[str, Any]:
        if self._part_delay_seconds:
            time.sleep(self._part_delay_seconds)
        source_bucket, source_key = self._parse_copy_source(CopySource)
        payload = self._objects[(source_bucket, source_key)]
        if CopySourceRange is not None:
            prefix = "bytes="
            if not CopySourceRange.startswith(prefix):
                raise ValueError(f"Unsupported copy range: {CopySourceRange}")
            start_str, end_str = CopySourceRange[len(prefix) :].split("-", 1)
            start = int(start_str)
            end = int(end_str)
            payload = payload[start : end + 1]
        with self._lock:
            self.upload_part_copy_calls += 1
            upload_bucket, upload_key, parts = self._multipart_uploads[UploadId]
            assert upload_bucket == Bucket
            assert upload_key == Key
            parts[PartNumber] = payload
            self.uploaded_part_numbers.append(PartNumber)
        return {"CopyPartResult": {"ETag": f"etag-{PartNumber}"}}

    def complete_multipart_upload(
        self,
        *,
        Bucket: str,
        Key: str,
        UploadId: str,
        MultipartUpload: dict[str, list[dict[str, str | int]]],
    ) -> dict[str, Any]:
        with self._lock:
            self.complete_calls += 1
            upload_bucket, upload_key, parts = self._multipart_uploads.pop(UploadId)
            assert upload_bucket == Bucket
            assert upload_key == Key
            ordered_parts = [parts[part["PartNumber"]] for part in MultipartUpload["Parts"]]
            self._objects[(Bucket, Key)] = b"".join(ordered_parts)
        return {}

    def abort_multipart_upload(self, *, Bucket: str, Key: str, UploadId: str) -> dict[str, Any]:
        with self._lock:
            self.abort_calls += 1
            self._multipart_uploads.pop(UploadId, None)
        return {}

    def _parse_copy_source(self, copy_source: dict[str, str]) -> tuple[str, str]:
        source_bucket = copy_source["Bucket"]
        source_key = copy_source["Key"]
        return source_bucket, source_key


def _build_data_flow(process_id: str) -> DataFlow:
    return DataFlow(
        data_flow_id=f"flow-{process_id}",
        process_id=process_id,
        transfer_type="com.test.s3-PUSH",
        transfer_mode=TransferMode.PUSH,
        participant_id="did:web:provider",
        counter_party_id="did:web:consumer",
        dataspace_context="context-1",
        agreement_id="agreement-1",
        dataset_id="dataset-1",
        callback_address="https://example.com/callback",
        state=DataFlowState.PREPARED,
        metadata={
            "sourceBucket": "src-bucket",
            "sourceKey": "source.bin",
        },
    )


def _build_start_message(process_id: str) -> DataFlowStartMessage:
    return DataFlowStartMessage(
        message_id=f"msg-{process_id}",
        participant_id="did:web:provider",
        counter_party_id="did:web:consumer",
        dataspace_context="context-1",
        process_id=process_id,
        agreement_id="agreement-1",
        dataset_id="dataset-1",
        callback_address="https://example.com/callback",
        transfer_type="com.test.s3-PUSH",
        metadata={
            "sourceBucket": "src-bucket",
            "sourceKey": "source.bin",
        },
        data_address=DataAddress(
            type_="DataAddress",
            endpoint_type="urn:aws:s3",
            endpoint="s3://dst-bucket/result.bin",
            endpoint_properties=[],
        ),
    )


def test_push_start_uses_multipart_copy_async() -> None:
    fake_client = FakeS3Client({("src-bucket", "source.bin"): 13 * 1024 * 1024})
    executor = S3TransferExecutor(
        multipart_threshold_mb=5,
        multipart_part_size_mb=5,
        s3_client_factory=lambda *_: fake_client,
    )

    data_flow = _build_data_flow("multipart")
    message = _build_start_message("multipart")

    async def scenario() -> None:
        await executor.start(data_flow, message)
        await executor.complete(data_flow)

    asyncio.run(scenario())

    assert fake_client.put_calls == 0
    assert fake_client.upload_part_calls == 0
    assert fake_client.upload_part_copy_calls == 3
    assert fake_client.multipart_upload_calls == 1
    assert sorted(fake_client.uploaded_part_numbers) == [1, 2, 3]
    assert fake_client.complete_calls == 1


def test_push_start_emits_completion_callback_after_successful_copy() -> None:
    fake_client = FakeS3Client({("src-bucket", "source.bin"): 2 * 1024 * 1024})
    completions: list[str] = []
    completion_event = asyncio.Event()

    async def completion_callback(data_flow_id: str) -> None:
        completions.append(data_flow_id)
        completion_event.set()

    executor = S3TransferExecutor(
        multipart_threshold_mb=5,
        multipart_part_size_mb=5,
        s3_client_factory=lambda *_: fake_client,
        completion_callback=completion_callback,
    )

    data_flow = _build_data_flow("callback")
    message = _build_start_message("callback")

    async def scenario() -> None:
        await executor.start(data_flow, message)
        await asyncio.wait_for(completion_event.wait(), timeout=2.0)
        await executor.complete(data_flow)

    asyncio.run(scenario())

    assert completions == [data_flow.data_flow_id]
    assert fake_client.copy_calls == 1
    assert fake_client.put_calls == 0


def test_suspend_then_resume_continues_multipart_transfer() -> None:
    fake_client = FakeS3Client(
        {("src-bucket", "source.bin"): 16 * 1024 * 1024},
        part_delay_seconds=0.05,
    )
    executor = S3TransferExecutor(
        multipart_threshold_mb=5,
        multipart_part_size_mb=5,
        s3_client_factory=lambda *_: fake_client,
    )

    data_flow = _build_data_flow("resume")
    message = _build_start_message("resume")

    async def scenario() -> None:
        await executor.start(data_flow, message)
        await asyncio.sleep(0.01)
        await executor.suspend(data_flow, "pause")

        await asyncio.sleep(0.08)
        count_during_pause = len(fake_client.uploaded_part_numbers)
        await asyncio.sleep(0.08)
        assert len(fake_client.uploaded_part_numbers) == count_during_pause

        await executor.start(data_flow, message)
        await executor.complete(data_flow)

    asyncio.run(scenario())

    assert fake_client.complete_calls == 1
    assert sorted(fake_client.uploaded_part_numbers) == [1, 2, 3, 4]


def test_start_uses_dataaddress_credentials_for_s3_clients() -> None:
    fake_client = FakeS3Client({("src-bucket", "source.bin"): 1 * 1024 * 1024})
    client_calls: list[tuple[str | None, str | None, bool, str | None, str | None, str | None]] = []

    def factory(
        region: str | None,
        endpoint_url: str | None,
        force_path_style: bool,
        access_key_id: str | None,
        secret_access_key: str | None,
        session_token: str | None,
    ) -> FakeS3Client:
        client_calls.append(
            (
                region,
                endpoint_url,
                force_path_style,
                access_key_id,
                secret_access_key,
                session_token,
            )
        )
        return fake_client

    executor = S3TransferExecutor(
        multipart_threshold_mb=8,
        multipart_part_size_mb=8,
        s3_client_factory=factory,
    )

    data_flow = _build_data_flow("credentials")
    message = _build_start_message("credentials").model_copy(
        update={
            "metadata": {
                "sourceDataAddress": {
                    "@type": "DataAddress",
                    "endpointType": "urn:aws:s3",
                    "endpoint": "s3://src-bucket/source.bin",
                    "endpointProperties": [
                        {"name": "region", "value": "us-east-1"},
                        {"name": "endpointUrl", "value": "http://source-minio:9000"},
                        {"name": "forcePathStyle", "value": "true"},
                        {"name": "accessKeyId", "value": "src-ak"},
                        {"name": "secretAccessKey", "value": "src-sk"},
                    ],
                }
            },
            "data_address": DataAddress(
                type_="DataAddress",
                endpoint_type="urn:aws:s3",
                endpoint="s3://dst-bucket/result.bin",
                endpoint_properties=[
                    EndpointProperty(name="region", value="us-east-1"),
                    EndpointProperty(name="endpointUrl", value="http://dest-minio:9000"),
                    EndpointProperty(name="forcePathStyle", value="true"),
                    EndpointProperty(name="accessKeyId", value="dst-ak"),
                    EndpointProperty(name="secretAccessKey", value="dst-sk"),
                ],
            ),
        }
    )

    async def scenario() -> None:
        await executor.start(data_flow, message)
        await executor.complete(data_flow)

    asyncio.run(scenario())

    assert len(client_calls) >= 2
    source_call, destination_call = client_calls[0], client_calls[1]

    assert source_call[1] == "http://source-minio:9000"
    assert source_call[2] is True
    assert source_call[3] == "src-ak"
    assert source_call[4] == "src-sk"

    assert destination_call[1] == "http://dest-minio:9000"
    assert destination_call[2] is True
    assert destination_call[3] == "dst-ak"
    assert destination_call[4] == "dst-sk"
    assert fake_client.put_calls == 1
    assert fake_client.copy_calls == 0


def test_multipart_falls_back_to_stream_copy_when_endpoints_differ() -> None:
    fake_client = FakeS3Client({("src-bucket", "source.bin"): 13 * 1024 * 1024})
    executor = S3TransferExecutor(
        multipart_threshold_mb=5,
        multipart_part_size_mb=5,
        s3_client_factory=lambda *_: fake_client,
    )

    data_flow = _build_data_flow("multipart-fallback")
    message = _build_start_message("multipart-fallback").model_copy(
        update={
            "metadata": {
                "sourceBucket": "src-bucket",
                "sourceKey": "source.bin",
                "sourceEndpointUrl": "http://source-minio:9000",
            },
            "data_address": DataAddress(
                type_="DataAddress",
                endpoint_type="urn:aws:s3",
                endpoint="s3://dst-bucket/result.bin",
                endpoint_properties=[
                    EndpointProperty(name="endpointUrl", value="http://dest-minio:9000"),
                ],
            ),
        }
    )

    async def scenario() -> None:
        await executor.start(data_flow, message)
        await executor.complete(data_flow)

    asyncio.run(scenario())

    assert fake_client.upload_part_calls == 3
    assert fake_client.upload_part_copy_calls == 0


def test_bucket_to_bucket_without_source_key_supports_suspend_resume() -> None:
    fake_client = FakeS3Client(
        {
            ("src-bucket", "0-large.bin"): 16 * 1024 * 1024,
            ("src-bucket", "1-small.txt"): 512 * 1024,
            ("src-bucket", "2-small.txt"): 256 * 1024,
        },
        part_delay_seconds=0.05,
    )
    executor = S3TransferExecutor(
        multipart_threshold_mb=5,
        multipart_part_size_mb=5,
        s3_client_factory=lambda *_: fake_client,
    )

    data_flow = _build_data_flow("bucket-resume")
    data_flow.metadata = {
        "sourceBucket": "src-bucket",
        "destinationBucket": "dst-bucket",
    }
    message = _build_start_message("bucket-resume").model_copy(
        update={
            "metadata": {
                "sourceBucket": "src-bucket",
                "destinationBucket": "dst-bucket",
            },
            "data_address": DataAddress(
                type_="DataAddress",
                endpoint_type="urn:aws:s3",
                endpoint="s3://dst-bucket",
                endpoint_properties=[],
            ),
        }
    )

    async def scenario() -> None:
        await executor.start(data_flow, message)
        await asyncio.sleep(0.01)
        await executor.suspend(data_flow, "pause")

        await asyncio.sleep(0.08)
        count_during_pause = len(fake_client.uploaded_part_numbers)
        await asyncio.sleep(0.08)
        assert len(fake_client.uploaded_part_numbers) == count_during_pause

        await executor.start(data_flow, message)
        await executor.complete(data_flow)

    asyncio.run(scenario())

    assert fake_client.list_calls >= 1
    assert fake_client.complete_calls == 1
    assert fake_client._objects[("dst-bucket", "0-large.bin")] == fake_client._objects[
        ("src-bucket", "0-large.bin")
    ]
    assert fake_client._objects[("dst-bucket", "1-small.txt")] == fake_client._objects[
        ("src-bucket", "1-small.txt")
    ]
    assert fake_client._objects[("dst-bucket", "2-small.txt")] == fake_client._objects[
        ("src-bucket", "2-small.txt")
    ]


def test_bucket_to_bucket_uses_destination_key_prefix() -> None:
    fake_client = FakeS3Client(
        {
            ("src-bucket", "a/file-1.txt"): 64 * 1024,
            ("src-bucket", "b/file-2.txt"): 32 * 1024,
        }
    )
    executor = S3TransferExecutor(
        multipart_threshold_mb=5,
        multipart_part_size_mb=5,
        s3_client_factory=lambda *_: fake_client,
    )

    data_flow = _build_data_flow("bucket-prefix")
    data_flow.metadata = {
        "sourceBucket": "src-bucket",
        "destinationBucket": "dst-bucket",
    }
    message = _build_start_message("bucket-prefix").model_copy(
        update={
            "metadata": {
                "sourceBucket": "src-bucket",
                "destinationBucket": "dst-bucket",
            },
            "data_address": DataAddress(
                type_="DataAddress",
                endpoint_type="urn:aws:s3",
                endpoint="s3://dst-bucket/archive",
                endpoint_properties=[],
            ),
        }
    )

    async def scenario() -> None:
        await executor.start(data_flow, message)
        await executor.complete(data_flow)

    asyncio.run(scenario())

    assert fake_client._objects[("dst-bucket", "archive/a/file-1.txt")] == fake_client._objects[
        ("src-bucket", "a/file-1.txt")
    ]
    assert fake_client._objects[("dst-bucket", "archive/b/file-2.txt")] == fake_client._objects[
        ("src-bucket", "b/file-2.txt")
    ]


def test_start_queues_when_max_active_dataflows_limit_is_reached() -> None:
    fake_client = FakeS3Client(
        {("src-bucket", "source.bin"): 2 * 1024 * 1024},
        put_delay_seconds=0.3,
    )
    executor = S3TransferExecutor(
        multipart_threshold_mb=8,
        multipart_part_size_mb=8,
        max_active_dataflows=1,
        s3_client_factory=lambda *_: fake_client,
    )

    flow_a = _build_data_flow("queue-a")
    message_a = _build_start_message("queue-a").model_copy(
        update={
            "data_address": DataAddress(
                type_="DataAddress",
                endpoint_type="urn:aws:s3",
                endpoint="s3://dst-bucket/result-a.bin",
                endpoint_properties=[],
            )
        }
    )
    flow_b = _build_data_flow("queue-b")
    message_b = _build_start_message("queue-b").model_copy(
        update={
            "data_address": DataAddress(
                type_="DataAddress",
                endpoint_type="urn:aws:s3",
                endpoint="s3://dst-bucket/result-b.bin",
                endpoint_properties=[],
            )
        }
    )

    async def scenario() -> None:
        await executor.start(flow_a, message_a)
        await executor.start(flow_b, message_b)
        await asyncio.sleep(0.05)

        progress_a = await executor.get_progress(flow_a.data_flow_id)
        progress_b = await executor.get_progress(flow_b.data_flow_id)
        assert progress_a is not None
        assert progress_b is not None
        assert progress_a.running is True
        assert progress_a.queued is False
        assert progress_b.running is False
        assert progress_b.queued is True

        await executor.complete(flow_a)
        await executor.complete(flow_b)

    asyncio.run(scenario())
