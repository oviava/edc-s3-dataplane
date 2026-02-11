"""Health check routes."""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness probe."""

    return {"status": "ok"}


__all__ = ["router"]
