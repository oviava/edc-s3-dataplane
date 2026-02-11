from __future__ import annotations

import pytest
from pydantic import ValidationError

from simpl_bulk_dataplane.bootstrap import build_dataflow_service
from simpl_bulk_dataplane.config import RepositoryBackend, Settings
from simpl_bulk_dataplane.infrastructure.repositories import (
    InMemoryDataFlowRepository,
    PostgresDataFlowRepository,
)


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
