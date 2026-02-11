"""Application bootstrap/wiring."""

from simpl_bulk_dataplane.application.services import DataFlowService
from simpl_bulk_dataplane.config import Settings
from simpl_bulk_dataplane.infrastructure.callbacks import NoopControlPlaneNotifier
from simpl_bulk_dataplane.infrastructure.repositories import InMemoryDataFlowRepository
from simpl_bulk_dataplane.infrastructure.transfers import S3TransferExecutor


def build_dataflow_service(settings: Settings) -> DataFlowService:
    """Compose service graph."""

    return DataFlowService(
        dataplane_id=settings.dataplane_id,
        repository=InMemoryDataFlowRepository(),
        transfer_executor=S3TransferExecutor(
            default_region=settings.aws_region,
            multipart_threshold_mb=settings.s3_multipart_threshold_mb,
            multipart_part_size_mb=settings.s3_multipart_part_size_mb,
            multipart_concurrency=settings.s3_multipart_concurrency,
        ),
        control_plane_notifier=NoopControlPlaneNotifier(),
    )


__all__ = ["build_dataflow_service"]
