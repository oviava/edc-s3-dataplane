"""Application settings."""

from enum import StrEnum
from typing import Any

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RepositoryBackend(StrEnum):
    """Available persistence adapters for data flow state."""

    IN_MEMORY = "in_memory"
    POSTGRES = "postgres"


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables."""

    app_name: str = "Simpl Bulk Dataplane"
    api_prefix: str = ""
    dataplane_id: str = "dataplane-local"
    dataplane_public_url: str | None = None
    host: str = "0.0.0.0"
    port: int = 8080
    aws_region: str = "us-east-1"
    s3_multipart_threshold_mb: int = 8
    s3_multipart_part_size_mb: int = 8
    s3_multipart_concurrency: int = 4
    s3_max_active_dataflows: int = 4
    s3_max_pool_connections: int = 16
    s3_prefer_server_side_copy: bool = True
    transfer_job_recovery_poll_seconds: float = 1.0
    transfer_job_recovery_batch_size: int = 20
    transfer_job_lease_seconds: float = 30.0
    transfer_job_heartbeat_seconds: float = 10.0
    repository_backend: RepositoryBackend = RepositoryBackend.IN_MEMORY
    postgres_dsn: str | None = None
    postgres_pool_min_size: int = 1
    postgres_pool_max_size: int = 10
    dataflow_events_mqtt_enabled: bool = False
    dataflow_events_mqtt_host: str | None = None
    dataflow_events_mqtt_port: int = 1883
    dataflow_events_mqtt_username: str | None = None
    dataflow_events_mqtt_password: str | None = None
    dataflow_events_mqtt_topic_prefix: str = "simpl/dataplane"
    dataflow_events_mqtt_qos: int = 0
    control_plane_registration_enabled: bool = False
    control_plane_endpoint: str | None = None
    control_plane_timeout_seconds: float = 10.0
    control_plane_registration_name: str | None = None
    control_plane_registration_description: str | None = None
    control_plane_registration_transfer_types: list[str] = Field(
        default_factory=lambda: ["com.test.s3-PUSH", "com.test.s3-PULL"]
    )
    control_plane_registration_labels: list[str] = Field(default_factory=list)
    control_plane_registration_authorization: list[dict[str, Any]] = Field(
        default_factory=list
    )

    @field_validator(
        "control_plane_registration_transfer_types",
        "control_plane_registration_labels",
        mode="before",
    )
    @classmethod
    def parse_csv_list(cls, value: object) -> object:
        """Support comma-separated env var values in addition to JSON arrays."""

        if not isinstance(value, str):
            return value
        return [item.strip() for item in value.split(",") if item.strip()]

    @model_validator(mode="after")
    def validate_repository_settings(self) -> "Settings":
        """Ensure backend-specific settings are valid."""

        if self.repository_backend == RepositoryBackend.POSTGRES and not self.postgres_dsn:
            raise ValueError(
                "SIMPL_DP_POSTGRES_DSN is required when SIMPL_DP_REPOSITORY_BACKEND=postgres."
            )
        if self.postgres_pool_min_size < 1:
            raise ValueError("SIMPL_DP_POSTGRES_POOL_MIN_SIZE must be >= 1.")
        if self.postgres_pool_max_size < self.postgres_pool_min_size:
            raise ValueError(
                "SIMPL_DP_POSTGRES_POOL_MAX_SIZE must be >= SIMPL_DP_POSTGRES_POOL_MIN_SIZE."
            )
        if self.dataflow_events_mqtt_enabled and not self.dataflow_events_mqtt_host:
            raise ValueError(
                "SIMPL_DP_DATAFLOW_EVENTS_MQTT_HOST is required when "
                "SIMPL_DP_DATAFLOW_EVENTS_MQTT_ENABLED=true."
            )
        if self.dataflow_events_mqtt_port < 1:
            raise ValueError("SIMPL_DP_DATAFLOW_EVENTS_MQTT_PORT must be >= 1.")
        if self.dataflow_events_mqtt_qos not in {0, 1, 2}:
            raise ValueError("SIMPL_DP_DATAFLOW_EVENTS_MQTT_QOS must be one of 0, 1, 2.")
        if self.control_plane_timeout_seconds <= 0:
            raise ValueError("SIMPL_DP_CONTROL_PLANE_TIMEOUT_SECONDS must be > 0.")
        if self.s3_max_active_dataflows < 1:
            raise ValueError("SIMPL_DP_S3_MAX_ACTIVE_DATAFLOWS must be >= 1.")
        if self.s3_multipart_concurrency < 1:
            raise ValueError("SIMPL_DP_S3_MULTIPART_CONCURRENCY must be >= 1.")
        if self.s3_max_pool_connections < 1:
            raise ValueError("SIMPL_DP_S3_MAX_POOL_CONNECTIONS must be >= 1.")
        if self.s3_multipart_concurrency > self.s3_max_pool_connections:
            raise ValueError(
                "SIMPL_DP_S3_MULTIPART_CONCURRENCY must be <= "
                "SIMPL_DP_S3_MAX_POOL_CONNECTIONS."
            )
        if self.transfer_job_recovery_poll_seconds <= 0:
            raise ValueError("SIMPL_DP_TRANSFER_JOB_RECOVERY_POLL_SECONDS must be > 0.")
        if self.transfer_job_recovery_batch_size < 1:
            raise ValueError("SIMPL_DP_TRANSFER_JOB_RECOVERY_BATCH_SIZE must be >= 1.")
        if self.transfer_job_lease_seconds <= 0:
            raise ValueError("SIMPL_DP_TRANSFER_JOB_LEASE_SECONDS must be > 0.")
        if self.transfer_job_heartbeat_seconds <= 0:
            raise ValueError("SIMPL_DP_TRANSFER_JOB_HEARTBEAT_SECONDS must be > 0.")
        if self.transfer_job_heartbeat_seconds > self.transfer_job_lease_seconds:
            raise ValueError(
                "SIMPL_DP_TRANSFER_JOB_HEARTBEAT_SECONDS must be <= "
                "SIMPL_DP_TRANSFER_JOB_LEASE_SECONDS."
            )
        return self

    model_config = SettingsConfigDict(env_prefix="SIMPL_DP_", extra="ignore")


__all__ = ["RepositoryBackend", "Settings"]
