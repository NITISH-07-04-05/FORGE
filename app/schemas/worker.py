from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class WorkerStatusResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    worker_id: str
    alive: bool
    heartbeat_ttl_seconds: int


class WorkerListResponse(BaseModel):
    workers: list[WorkerStatusResponse]
