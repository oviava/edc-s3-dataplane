"""Route modules public API."""

from simpl_bulk_dataplane.api.routes.health import router as health_router
from simpl_bulk_dataplane.api.routes.signaling import router as signaling_router

__all__ = ["health_router", "signaling_router"]
