import logging

from fastapi import FastAPI
from app.core.lifespan import lifespan
from app.api.routers.health import router as health_router
from app.core.config import settings
from app.core.logging import configure_logging


def create_app() -> FastAPI:
    configure_logging()

    logger = logging.getLogger(__name__)

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        lifespan=lifespan,
    )

    app.include_router(health_router)

    return app


app = create_app()