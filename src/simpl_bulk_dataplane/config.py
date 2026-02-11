"""Application settings."""

from enum import StrEnum

from pydantic import model_validator
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
    host: str = "0.0.0.0"
    port: int = 8080
    aws_region: str = "us-east-1"
    s3_multipart_threshold_mb: int = 8
    s3_multipart_part_size_mb: int = 8
    s3_multipart_concurrency: int = 4
    repository_backend: RepositoryBackend = RepositoryBackend.IN_MEMORY
    postgres_dsn: str | None = None
    postgres_pool_min_size: int = 1
    postgres_pool_max_size: int = 10

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
        return self

    model_config = SettingsConfigDict(env_prefix="SIMPL_DP_", extra="ignore")


__all__ = ["RepositoryBackend", "Settings"]
