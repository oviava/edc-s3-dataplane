"""Application bootstrap/wiring."""

from simpl_bulk_dataplane.application.services import DataFlowService
from simpl_bulk_dataplane.config import RepositoryBackend, Settings
from simpl_bulk_dataplane.domain.ports import DataFlowRepository
from simpl_bulk_dataplane.infrastructure.callbacks import NoopControlPlaneNotifier
from simpl_bulk_dataplane.infrastructure.repositories import (
    InMemoryDataFlowRepository,
    PostgresDataFlowRepository,
)
from simpl_bulk_dataplane.infrastructure.transfers import S3TransferExecutor


def _build_repository(settings: Settings) -> DataFlowRepository:
    if settings.repository_backend == RepositoryBackend.POSTGRES:
        if settings.postgres_dsn is None:
            raise ValueError(
                "SIMPL_DP_POSTGRES_DSN is required when SIMPL_DP_REPOSITORY_BACKEND=postgres."
            )
        return PostgresDataFlowRepository(
            dsn=settings.postgres_dsn,
            min_pool_size=settings.postgres_pool_min_size,
            max_pool_size=settings.postgres_pool_max_size,
        )
    return InMemoryDataFlowRepository()


def build_dataflow_service(settings: Settings) -> DataFlowService:
    """Compose service graph."""

    return DataFlowService(
        dataplane_id=settings.dataplane_id,
        repository=_build_repository(settings),
        transfer_executor=S3TransferExecutor(
            default_region=settings.aws_region,
            multipart_threshold_mb=settings.s3_multipart_threshold_mb,
            multipart_part_size_mb=settings.s3_multipart_part_size_mb,
            multipart_concurrency=settings.s3_multipart_concurrency,
        ),
        control_plane_notifier=NoopControlPlaneNotifier(),
    )


__all__ = ["build_dataflow_service"]
