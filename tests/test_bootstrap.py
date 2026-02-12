from __future__ import annotations

import pytest
from pydantic import ValidationError

from simpl_bulk_dataplane.bootstrap import build_dataflow_service
from simpl_bulk_dataplane.config import RepositoryBackend, Settings
from simpl_bulk_dataplane.infrastructure.callbacks import (
    HttpControlPlaneNotifier,
    NoopControlPlaneNotifier,
)
from simpl_bulk_dataplane.infrastructure.control_plane import (
    ControlPlaneClient,
    ControlPlaneClientError,
)
from simpl_bulk_dataplane.infrastructure.repositories import (
    InMemoryDataFlowRepository,
    PostgresDataFlowRepository,
)
from simpl_bulk_dataplane.infrastructure.transfers import S3TransferExecutor


def test_build_dataflow_service_uses_in_memory_repository_by_default() -> None:
    settings = Settings(repository_backend=RepositoryBackend.IN_MEMORY)
    service = build_dataflow_service(settings)

    assert isinstance(service._repository, InMemoryDataFlowRepository)


def test_build_dataflow_service_uses_postgres_repository_when_configured() -> None:
    settings = Settings(
        repository_backend=RepositoryBackend.POSTGRES,
        postgres_dsn="postgresql://simpl:simpl@localhost:5432/simpl_dataplane",
    )
    service = build_dataflow_service(settings)

    assert isinstance(service._repository, PostgresDataFlowRepository)


def test_settings_require_postgres_dsn_when_backend_is_postgres() -> None:
    with pytest.raises(ValidationError):
        Settings(repository_backend=RepositoryBackend.POSTGRES)


def test_settings_require_mqtt_host_when_mqtt_events_are_enabled() -> None:
    with pytest.raises(ValidationError):
        Settings(dataflow_events_mqtt_enabled=True)


def test_settings_require_positive_max_active_dataflows() -> None:
    with pytest.raises(ValidationError):
        Settings(s3_max_active_dataflows=0)


def test_settings_require_positive_s3_max_pool_connections() -> None:
    with pytest.raises(ValidationError):
        Settings(s3_max_pool_connections=0)


def test_settings_require_multipart_concurrency_not_exceed_pool() -> None:
    with pytest.raises(ValidationError):
        Settings(s3_multipart_concurrency=8, s3_max_pool_connections=4)


def test_build_dataflow_service_uses_http_notifier_when_registration_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _register_ok(self: ControlPlaneClient, _message: object) -> None:
        return None

    monkeypatch.setattr(ControlPlaneClient, "register_dataplane", _register_ok)

    settings = Settings(
        control_plane_registration_enabled=True,
        control_plane_endpoint="https://controlplane.example.com/signaling/v1",
        dataplane_public_url="https://dataplane.example.com/signaling/v1",
    )
    service = build_dataflow_service(settings)

    assert isinstance(service._control_plane_notifier, HttpControlPlaneNotifier)


def test_build_dataflow_service_falls_back_to_noop_notifier_when_registration_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _register_fail(self: ControlPlaneClient, _message: object) -> None:
        raise ControlPlaneClientError("registration failed")

    monkeypatch.setattr(ControlPlaneClient, "register_dataplane", _register_fail)

    settings = Settings(
        control_plane_registration_enabled=True,
        control_plane_endpoint="https://controlplane.example.com/signaling/v1",
        dataplane_public_url="https://dataplane.example.com/signaling/v1",
    )
    service = build_dataflow_service(settings)

    assert isinstance(service._control_plane_notifier, NoopControlPlaneNotifier)


def test_build_dataflow_service_applies_max_active_dataflow_limit() -> None:
    settings = Settings(s3_max_active_dataflows=2)
    service = build_dataflow_service(settings)

    assert isinstance(service._transfer_executor, S3TransferExecutor)
    assert service._transfer_executor.max_active_dataflows == 2
