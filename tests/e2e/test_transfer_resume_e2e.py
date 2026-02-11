from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = PROJECT_ROOT / "tests" / "e2e" / "docker-compose.yml"

DATAPLANE_A_URL = "http://127.0.0.1:18081"
DATAPLANE_B_URL = "http://127.0.0.1:18082"

MINIO_A_EXTERNAL = "http://127.0.0.1:19000"
MINIO_B_EXTERNAL = "http://127.0.0.1:19010"

MINIO_A_INTERNAL = "http://minio-a:9000"
MINIO_B_INTERNAL = "http://minio-b:9000"

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

SOURCE_BUCKET = "source-bucket"
DEST_BUCKET = "destination-bucket"
SOURCE_KEY = "payload-100mb.bin"
DEST_KEY = "payload-100mb-copy.bin"
RETURN_KEY = "payload-100mb-return.bin"
PAYLOAD_SIZE_BYTES = 100 * 1024 * 1024


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _compose_command() -> list[str]:
    return ["docker", "compose", "-f", str(COMPOSE_FILE)]


def _run_compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    command = [*_compose_command(), *args]
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    if check and completed.returncode != 0:
        raise RuntimeError(
            "docker compose command failed:\n"
            f"cmd: {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return completed


def _wait_for_http(url: str, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=2.0)
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(1.0)
    raise AssertionError(f"Timed out waiting for URL: {url}")


def _wait_for_state(
    dataplane_url: str,
    data_flow_id: str,
    state: str,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    status_url = f"{dataplane_url}/dataflows/{data_flow_id}/status"
    while time.monotonic() < deadline:
        response = httpx.get(status_url, timeout=5.0)
        if response.status_code == 200 and response.json().get("state") == state:
            return
        time.sleep(0.5)
    raise AssertionError(f"Timed out waiting for state '{state}' on flow {data_flow_id}.")


def _wait_for_object(
    client: Any,
    bucket: str,
    key: str,
    expected_size: int,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            response = client.head_object(Bucket=bucket, Key=key)
            if response.get("ContentLength") == expected_size:
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1.0)
    raise AssertionError(f"Timed out waiting for object s3://{bucket}/{key}.")


def _post_json(url: str, payload: dict[str, object]) -> dict[str, object]:
    response = httpx.post(url, json=payload, timeout=30.0)
    assert response.status_code == 200, response.text
    data = response.json()
    assert isinstance(data, dict)
    return data


def _s3_client(endpoint_url: str) -> Any:
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def _ensure_bucket(client: Any, bucket: str) -> None:
    try:
        client.create_bucket(Bucket=bucket)
    except Exception as exc:  # noqa: BLE001
        error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if error_code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise


def _object_exists(client: Any, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:  # noqa: BLE001
        return False


def _object_sha256(client: Any, bucket: str, key: str) -> str:
    response = client.get_object(Bucket=bucket, Key=key)
    payload = response["Body"].read()
    return hashlib.sha256(payload).hexdigest()


def _build_payload() -> bytes:
    block = b"0123456789abcdef" * 4096
    repeats = PAYLOAD_SIZE_BYTES // len(block)
    remainder = PAYLOAD_SIZE_BYTES % len(block)
    return block * repeats + block[:remainder]


def _data_address(bucket: str, key: str, endpoint_url: str) -> dict[str, object]:
    return {
        "@type": "DataAddress",
        "endpointType": "urn:aws:s3",
        "endpoint": f"s3://{bucket}/{key}",
        "endpointProperties": [
            {"name": "storageType", "value": "s3"},
            {"name": "region", "value": "us-east-1"},
            {"name": "endpointUrl", "value": endpoint_url},
            {"name": "forcePathStyle", "value": "true"},
            {"name": "accessKeyId", "value": MINIO_ACCESS_KEY},
            {"name": "secretAccessKey", "value": MINIO_SECRET_KEY},
        ],
    }


def _start_payload(
    *,
    process_id: str,
    source_data_address: dict[str, object],
    destination_data_address: dict[str, object] | None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "messageId": f"msg-{uuid4()}",
        "participantId": "did:web:provider",
        "counterPartyId": "did:web:consumer",
        "dataspaceContext": "context-1",
        "processId": process_id,
        "agreementId": "agreement-1",
        "datasetId": "dataset-1",
        "callbackAddress": "https://example.com/callback",
        "transferType": "com.test.s3-PUSH",
        "metadata": {
            "sourceDataAddress": source_data_address,
            "forceMultipart": True,
            "multipartThresholdMb": 5,
            "multipartPartSizeMb": 5,
            "multipartConcurrency": 1,
        },
    }
    if destination_data_address is not None:
        payload["dataAddress"] = destination_data_address
    return payload


@pytest.fixture(scope="module")
def docker_stack() -> None:
    if os.getenv("RUN_DOCKER_E2E") != "1":
        pytest.skip("Set RUN_DOCKER_E2E=1 to run Docker e2e tests.")
    if not _docker_available():
        pytest.skip("Docker is not available on this machine.")
    try:
        import boto3  # noqa: F401
        import botocore.config  # noqa: F401
    except ModuleNotFoundError:
        pytest.skip("boto3/botocore are required to run Docker e2e tests.")

    _run_compose("up", "-d", "--build")
    try:
        _wait_for_http(f"{DATAPLANE_A_URL}/healthz", timeout_seconds=120.0)
        _wait_for_http(f"{DATAPLANE_B_URL}/healthz", timeout_seconds=120.0)
        yield
    finally:
        _run_compose("down", "-v", "--remove-orphans", check=False)


@pytest.mark.e2e
def test_transfer_100mb_with_suspend_resume_across_two_dataplanes(docker_stack: None) -> None:
    _ = docker_stack

    source_client = _s3_client(MINIO_A_EXTERNAL)
    destination_client = _s3_client(MINIO_B_EXTERNAL)

    _ensure_bucket(source_client, SOURCE_BUCKET)
    _ensure_bucket(source_client, DEST_BUCKET)
    _ensure_bucket(destination_client, SOURCE_BUCKET)
    _ensure_bucket(destination_client, DEST_BUCKET)

    payload = _build_payload()
    expected_sha = hashlib.sha256(payload).hexdigest()
    source_client.put_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY, Body=payload)

    process_id = f"proc-a-{uuid4()}"
    source_address_a = _data_address(SOURCE_BUCKET, SOURCE_KEY, MINIO_A_INTERNAL)
    destination_address_a = _data_address(DEST_BUCKET, DEST_KEY, MINIO_B_INTERNAL)
    start_response = _post_json(
        f"{DATAPLANE_A_URL}/dataflows/start",
        _start_payload(
            process_id=process_id,
            source_data_address=source_address_a,
            destination_data_address=destination_address_a,
        ),
    )

    data_flow_id_a = str(start_response["dataFlowId"])
    suspend_response = httpx.post(
        f"{DATAPLANE_A_URL}/dataflows/{data_flow_id_a}/suspend",
        json={"reason": "pause-for-resume-e2e"},
        timeout=30.0,
    )
    assert suspend_response.status_code == 200, suspend_response.text
    _wait_for_state(DATAPLANE_A_URL, data_flow_id_a, "SUSPENDED", timeout_seconds=30.0)

    time.sleep(1.0)
    assert not _object_exists(destination_client, DEST_BUCKET, DEST_KEY)

    resumed_response = _post_json(
        f"{DATAPLANE_A_URL}/dataflows/start",
        _start_payload(
            process_id=process_id,
            source_data_address=source_address_a,
            destination_data_address=None,
        ),
    )
    assert str(resumed_response["dataFlowId"]) == data_flow_id_a
    _wait_for_object(
        destination_client,
        DEST_BUCKET,
        DEST_KEY,
        expected_size=PAYLOAD_SIZE_BYTES,
        timeout_seconds=180.0,
    )
    assert _object_sha256(destination_client, DEST_BUCKET, DEST_KEY) == expected_sha

    completed_response = httpx.post(
        f"{DATAPLANE_A_URL}/dataflows/{data_flow_id_a}/completed",
        timeout=30.0,
    )
    assert completed_response.status_code == 200, completed_response.text
    _wait_for_state(DATAPLANE_A_URL, data_flow_id_a, "COMPLETED", timeout_seconds=30.0)

    process_id_b = f"proc-b-{uuid4()}"
    source_address_b = _data_address(DEST_BUCKET, DEST_KEY, MINIO_B_INTERNAL)
    destination_address_b = _data_address(SOURCE_BUCKET, RETURN_KEY, MINIO_A_INTERNAL)
    start_response_b = _post_json(
        f"{DATAPLANE_B_URL}/dataflows/start",
        _start_payload(
            process_id=process_id_b,
            source_data_address=source_address_b,
            destination_data_address=destination_address_b,
        ),
    )

    data_flow_id_b = str(start_response_b["dataFlowId"])
    _wait_for_object(
        source_client,
        SOURCE_BUCKET,
        RETURN_KEY,
        expected_size=PAYLOAD_SIZE_BYTES,
        timeout_seconds=180.0,
    )
    assert _object_sha256(source_client, SOURCE_BUCKET, RETURN_KEY) == expected_sha

    completed_response_b = httpx.post(
        f"{DATAPLANE_B_URL}/dataflows/{data_flow_id_b}/completed",
        timeout=30.0,
    )
    assert completed_response_b.status_code == 200, completed_response_b.text
    _wait_for_state(DATAPLANE_B_URL, data_flow_id_b, "COMPLETED", timeout_seconds=30.0)
