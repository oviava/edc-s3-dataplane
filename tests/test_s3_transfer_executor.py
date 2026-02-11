from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

from simpl_bulk_dataplane.domain.entities import DataFlow
from simpl_bulk_dataplane.domain.signaling_models import DataAddress, DataFlowStartMessage
from simpl_bulk_dataplane.domain.transfer_types import DataFlowState, TransferMode
from simpl_bulk_dataplane.infrastructure.transfers import S3TransferExecutor


class FakeS3Client:
    """Thread-safe fake S3 client used by transfer executor tests."""

    def __init__(
        self, object_sizes: dict[tuple[str, str], int], part_delay_seconds: float = 0.0
    ) -> None:
        self._object_sizes = dict(object_sizes)
        self._part_delay_seconds = part_delay_seconds
        self._upload_counter = 0
        self._lock = threading.Lock()
        self.copy_calls = 0
        self.multipart_upload_calls = 0
        self.uploaded_part_numbers: list[int] = []
        self.complete_calls = 0
        self.abort_calls = 0

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        return {"ContentLength": self._object_sizes[(Bucket, Key)]}

    def copy_object(self, *, Bucket: str, Key: str, CopySource: dict[str, str]) -> dict[str, Any]:
        with self._lock:
            self.copy_calls += 1
            source = (CopySource["Bucket"], CopySource["Key"])
            self._object_sizes[(Bucket, Key)] = self._object_sizes[source]
        return {}

    def create_multipart_upload(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        _ = (Bucket, Key)
        with self._lock:
            self.multipart_upload_calls += 1
            self._upload_counter += 1
            upload_id = f"upload-{self._upload_counter}"
        return {"UploadId": upload_id}

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
        _ = (Bucket, Key, UploadId, CopySource, CopySourceRange)
        if self._part_delay_seconds:
            time.sleep(self._part_delay_seconds)
        with self._lock:
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
        _ = (Bucket, Key, UploadId, MultipartUpload)
        with self._lock:
            self.complete_calls += 1
        return {}

    def abort_multipart_upload(self, *, Bucket: str, Key: str, UploadId: str) -> dict[str, Any]:
        _ = (Bucket, Key, UploadId)
        with self._lock:
            self.abort_calls += 1
        return {}


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
        s3_client_factory=lambda _: fake_client,
    )

    data_flow = _build_data_flow("multipart")
    message = _build_start_message("multipart")

    async def scenario() -> None:
        await executor.start(data_flow, message)
        await executor.complete(data_flow)

    asyncio.run(scenario())

    assert fake_client.copy_calls == 0
    assert fake_client.multipart_upload_calls == 1
    assert sorted(fake_client.uploaded_part_numbers) == [1, 2, 3]
    assert fake_client.complete_calls == 1


def test_suspend_then_resume_continues_multipart_transfer() -> None:
    fake_client = FakeS3Client(
        {("src-bucket", "source.bin"): 16 * 1024 * 1024},
        part_delay_seconds=0.05,
    )
    executor = S3TransferExecutor(
        multipart_threshold_mb=5,
        multipart_part_size_mb=5,
        s3_client_factory=lambda _: fake_client,
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
