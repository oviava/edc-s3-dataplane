"""FastAPI application entrypoint."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from simpl_bulk_dataplane import __version__
from simpl_bulk_dataplane.api import api_router
from simpl_bulk_dataplane.api.dependencies import get_dataflow_service, get_settings


def create_app() -> FastAPI:
    """Build FastAPI application."""

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        """Build singleton dependencies at startup."""

        get_dataflow_service()
        yield

    settings = get_settings()
    app = FastAPI(
        title=settings.app_name,
        version=__version__,
        lifespan=lifespan,
    )
    app.include_router(api_router, prefix=settings.api_prefix)
    return app


app = create_app()


def run() -> None:
    """Run local development server."""

    settings = get_settings()
    uvicorn.run(
        "simpl_bulk_dataplane.main:app",
        host=settings.host,
        port=settings.port,
        reload=False,
    )


__all__ = ["app", "create_app", "run"]
