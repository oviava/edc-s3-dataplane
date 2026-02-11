"""Application settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


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

    model_config = SettingsConfigDict(env_prefix="SIMPL_DP_", extra="ignore")


__all__ = ["Settings"]
