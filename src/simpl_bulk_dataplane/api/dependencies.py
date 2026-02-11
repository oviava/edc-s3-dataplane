"""Dependency providers for FastAPI routes."""

from functools import lru_cache

from simpl_bulk_dataplane.application.services import DataFlowService
from simpl_bulk_dataplane.bootstrap import build_dataflow_service
from simpl_bulk_dataplane.config import Settings


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return singleton settings."""

    return Settings()


@lru_cache(maxsize=1)
def get_dataflow_service() -> DataFlowService:
    """Return singleton service graph."""

    return build_dataflow_service(get_settings())


__all__ = ["get_dataflow_service", "get_settings"]
