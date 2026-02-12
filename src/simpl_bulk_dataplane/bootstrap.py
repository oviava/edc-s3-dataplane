"""Application bootstrap/wiring."""

import logging
from dataclasses import dataclass

from simpl_bulk_dataplane.application.services import DataFlowService
from simpl_bulk_dataplane.config import RepositoryBackend, Settings
from simpl_bulk_dataplane.domain.monitoring_models import TransferProgressSnapshot
from simpl_bulk_dataplane.domain.ports import (
    CallbackOutboxRepository,
    ControlPlaneNotifier,
    DataFlowEventPublisher,
    DataFlowRepository,
    TransferJobRepository,
)
from simpl_bulk_dataplane.domain.signaling_models import DataPlaneRegistrationMessage
from simpl_bulk_dataplane.infrastructure.callbacks import (
    ControlPlaneCallbackOutboxDispatcher,
    HttpControlPlaneNotifier,
    NoopControlPlaneNotifier,
)
from simpl_bulk_dataplane.infrastructure.control_plane import (
    ControlPlaneClient,
    ControlPlaneClientError,
)
from simpl_bulk_dataplane.infrastructure.events import (
    MqttDataFlowEventPublisher,
    NoopDataFlowEventPublisher,
)
from simpl_bulk_dataplane.infrastructure.repositories import (
    InMemoryDataFlowRepository,
    PostgresDataFlowRepository,
)
from simpl_bulk_dataplane.infrastructure.transfers import S3TransferExecutor

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class _ControlPlaneCallbackWiring:
    notifier: ControlPlaneNotifier
    client: ControlPlaneClient | None


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


def _dataplane_signaling_endpoint(settings: Settings) -> str | None:
    """Build externally reachable signaling endpoint from settings."""

    if settings.dataplane_public_url is None:
        return None

    base_url = settings.dataplane_public_url.strip().rstrip("/")
    if not base_url:
        return None

    api_prefix = settings.api_prefix.strip()
    if not api_prefix:
        return base_url

    normalized_prefix = api_prefix if api_prefix.startswith("/") else f"/{api_prefix}"
    normalized_prefix = normalized_prefix.rstrip("/")
    return f"{base_url}{normalized_prefix}"


def _build_control_plane_notifier(settings: Settings) -> _ControlPlaneCallbackWiring:
    if not settings.control_plane_registration_enabled:
        return _ControlPlaneCallbackWiring(notifier=NoopControlPlaneNotifier(), client=None)

    if settings.control_plane_endpoint is None:
        logger.warning(
            "Control-plane registration enabled but SIMPL_DP_CONTROL_PLANE_ENDPOINT is missing. "
            "Falling back to noop callbacks."
        )
        return _ControlPlaneCallbackWiring(notifier=NoopControlPlaneNotifier(), client=None)

    dataplane_endpoint = _dataplane_signaling_endpoint(settings)
    if dataplane_endpoint is None:
        logger.warning(
            "Control-plane registration enabled but SIMPL_DP_DATAPLANE_PUBLIC_URL is missing. "
            "Falling back to noop callbacks."
        )
        return _ControlPlaneCallbackWiring(notifier=NoopControlPlaneNotifier(), client=None)

    registration = DataPlaneRegistrationMessage(
        dataplane_id=settings.dataplane_id,
        name=settings.control_plane_registration_name or settings.app_name,
        description=settings.control_plane_registration_description,
        endpoint=dataplane_endpoint,
        transfer_types=settings.control_plane_registration_transfer_types,
        authorization=settings.control_plane_registration_authorization,
        labels=settings.control_plane_registration_labels,
    )
    try:
        client = ControlPlaneClient(
            base_url=settings.control_plane_endpoint,
            timeout_seconds=settings.control_plane_timeout_seconds,
        )
        client.register_dataplane(registration)
    except ControlPlaneClientError as exc:
        logger.warning(
            "Failed to register dataplane '%s' at control plane '%s': %s. "
            "Falling back to noop callbacks.",
            settings.dataplane_id,
            settings.control_plane_endpoint,
            exc,
        )
        return _ControlPlaneCallbackWiring(notifier=NoopControlPlaneNotifier(), client=None)

    logger.info(
        "Registered dataplane '%s' at control plane '%s'.",
        settings.dataplane_id,
        settings.control_plane_endpoint,
    )
    return _ControlPlaneCallbackWiring(
        notifier=HttpControlPlaneNotifier(client),
        client=client,
    )


def _build_callback_outbox_dispatcher(
    repository: DataFlowRepository,
    control_plane_client: ControlPlaneClient | None,
) -> ControlPlaneCallbackOutboxDispatcher | None:
    if control_plane_client is None:
        return None
    if not isinstance(repository, CallbackOutboxRepository):
        return None
    return ControlPlaneCallbackOutboxDispatcher(
        outbox_repository=repository,
        control_plane_client=control_plane_client,
    )


def build_dataflow_service(settings: Settings) -> DataFlowService:
    """Compose service graph."""

    repository = _build_repository(settings)
    control_plane_wiring = _build_control_plane_notifier(settings)
    event_publisher = _build_dataflow_event_publisher(settings)

    async def progress_callback(
        data_flow_id: str,
        progress: TransferProgressSnapshot,
    ) -> None:
        await event_publisher.publish_progress(
            data_flow_id=data_flow_id,
            progress=progress,
        )

    callback_outbox_dispatcher = _build_callback_outbox_dispatcher(
        repository=repository,
        control_plane_client=control_plane_wiring.client,
    )

    return DataFlowService(
        dataplane_id=settings.dataplane_id,
        repository=repository,
        transfer_executor=S3TransferExecutor(
            default_region=settings.aws_region,
            multipart_threshold_mb=settings.s3_multipart_threshold_mb,
            multipart_part_size_mb=settings.s3_multipart_part_size_mb,
            multipart_concurrency=settings.s3_multipart_concurrency,
            max_active_dataflows=settings.s3_max_active_dataflows,
            max_pool_connections=settings.s3_max_pool_connections,
            prefer_server_side_copy=settings.s3_prefer_server_side_copy,
            progress_callback=progress_callback,
        ),
        control_plane_notifier=control_plane_wiring.notifier,
        dataflow_event_publisher=event_publisher,
        callback_outbox_dispatcher=callback_outbox_dispatcher,
        transfer_job_repository=(
            repository if isinstance(repository, TransferJobRepository) else None
        ),
        transfer_job_recovery_poll_seconds=settings.transfer_job_recovery_poll_seconds,
        transfer_job_recovery_batch_size=settings.transfer_job_recovery_batch_size,
        transfer_job_lease_seconds=settings.transfer_job_lease_seconds,
        transfer_job_heartbeat_seconds=settings.transfer_job_heartbeat_seconds,
    )


__all__ = ["build_dataflow_service"]
