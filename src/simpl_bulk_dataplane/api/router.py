"""Top-level API router composition."""

from fastapi import APIRouter

from simpl_bulk_dataplane.api.routes import health_router, management_router, signaling_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(management_router)
api_router.include_router(signaling_router)

__all__ = ["api_router"]
