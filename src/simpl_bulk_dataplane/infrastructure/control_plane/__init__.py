"""Control-plane infrastructure adapters."""

from simpl_bulk_dataplane.infrastructure.control_plane.client import (
    ControlPlaneClient,
    ControlPlaneClientError,
)

__all__ = ["ControlPlaneClient", "ControlPlaneClientError"]
