from fastapi import FastAPI

from app.api.routers.database import router as database_router
from app.api.routers.health import router as health_router
from app.api.routers.tasks import router as tasks_router
from app.api.routers.workers import router as workers_router
from app.core.config import settings
from app.core.lifespan import lifespan
from app.core.logging import configure_logging


def create_app() -> FastAPI:
    configure_logging()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        lifespan=lifespan,
    )

    # Routers keep API concerns modular as the application grows.
    app.include_router(health_router)
    app.include_router(database_router)
    app.include_router(tasks_router)
    app.include_router(workers_router)

    return app


app = create_app()
