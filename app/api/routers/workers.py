from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from redis.exceptions import RedisError

from app.execution.heartbeat import WorkerHeartbeat
from app.execution.worker_registry import WorkerRegistry, WorkerStatus
from app.schemas.worker import WorkerListResponse, WorkerStatusResponse

router = APIRouter(prefix="/workers", tags=["Workers"])


def get_worker_heartbeat() -> WorkerHeartbeat:
    return WorkerHeartbeat()


def get_worker_registry(
    heartbeat_manager: Annotated[WorkerHeartbeat, Depends(get_worker_heartbeat)],
) -> WorkerRegistry:
    return WorkerRegistry(heartbeat_manager=heartbeat_manager)


@router.get("", response_model=WorkerListResponse)
def list_workers(
    worker_registry: Annotated[WorkerRegistry, Depends(get_worker_registry)],
) -> WorkerListResponse:
    try:
        workers = worker_registry.list_workers()
    except RedisError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Worker registry is temporarily unavailable.",
        ) from exc

    return WorkerListResponse(
        workers=[WorkerStatusResponse.model_validate(worker) for worker in workers]
    )
