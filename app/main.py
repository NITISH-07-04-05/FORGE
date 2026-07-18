from fastapi import FastAPI

from app.api.routers.health import router as health_router

app = FastAPI(
    title="FORGE",
    version="0.1.0",
)

app.include_router(health_router)