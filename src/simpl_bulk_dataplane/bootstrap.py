"""Application bootstrap/wiring."""

from simpl_bulk_dataplane.application.services import DataFlowService
from simpl_bulk_dataplane.config import RepositoryBackend, Settings
from simpl_bulk_dataplane.domain.monitoring_models import TransferProgressSnapshot
from simpl_bulk_dataplane.domain.ports import DataFlowEventPublisher, DataFlowRepository
from simpl_bulk_dataplane.infrastructure.callbacks import NoopControlPlaneNotifier
from simpl_bulk_dataplane.infrastructure.events import (
    MqttDataFlowEventPublisher,
    NoopDataFlowEventPublisher,
)
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


def _build_dataflow_event_publisher(settings: Settings) -> DataFlowEventPublisher:
    if settings.dataflow_events_mqtt_enabled:
        if settings.dataflow_events_mqtt_host is None:
            raise ValueError(
                "SIMPL_DP_DATAFLOW_EVENTS_MQTT_HOST is required when "
                "SIMPL_DP_DATAFLOW_EVENTS_MQTT_ENABLED=true."
            )
        return MqttDataFlowEventPublisher(
            dataplane_id=settings.dataplane_id,
            broker_host=settings.dataflow_events_mqtt_host,
            broker_port=settings.dataflow_events_mqtt_port,
            topic_prefix=settings.dataflow_events_mqtt_topic_prefix,
            qos=settings.dataflow_events_mqtt_qos,
            username=settings.dataflow_events_mqtt_username,
            password=settings.dataflow_events_mqtt_password,
            dataplane_public_url=settings.dataplane_public_url,
        )
    return NoopDataFlowEventPublisher()


def build_dataflow_service(settings: Settings) -> DataFlowService:
    """Compose service graph."""

    event_publisher = _build_dataflow_event_publisher(settings)

    async def progress_callback(
        data_flow_id: str,
        progress: TransferProgressSnapshot,
    ) -> None:
        await event_publisher.publish_progress(
            data_flow_id=data_flow_id,
            progress=progress,
        )

    return DataFlowService(
        dataplane_id=settings.dataplane_id,
        repository=_build_repository(settings),
        transfer_executor=S3TransferExecutor(
            default_region=settings.aws_region,
            multipart_threshold_mb=settings.s3_multipart_threshold_mb,
            multipart_part_size_mb=settings.s3_multipart_part_size_mb,
            multipart_concurrency=settings.s3_multipart_concurrency,
            progress_callback=progress_callback,
        ),
        control_plane_notifier=NoopControlPlaneNotifier(),
        dataflow_event_publisher=event_publisher,
    )


__all__ = ["build_dataflow_service"]
