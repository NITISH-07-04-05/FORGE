from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.dependencies import get_db
from app.queue.redis_queue import RedisQueue
from app.repositories.task_repository import TaskRepository
from app.schemas.task import TaskCreate, TaskResponse
from app.services.task_service import TaskDispatchError, TaskService

router = APIRouter(prefix="/tasks", tags=["Tasks"])


def get_task_repository(db: Annotated[Session, Depends(get_db)]) -> TaskRepository:
    # Repositories stay focused on persistence and share the request-scoped session.
    return TaskRepository(db)


def get_redis_queue() -> RedisQueue:
    return RedisQueue()


def get_task_service(
    task_repository: Annotated[TaskRepository, Depends(get_task_repository)],
    queue: Annotated[RedisQueue, Depends(get_redis_queue)],
) -> TaskService:
    # The router depends on the application service instead of embedding workflow logic.
    return TaskService(task_repository, queue)


@router.post("", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
def create_task(
    task_in: TaskCreate,
    db: Annotated[Session, Depends(get_db)],
    task_service: Annotated[TaskService, Depends(get_task_service)],
) -> TaskResponse:
    task = task_service.create_task(
        task_type=task_in.task_type,
        payload=task_in.payload,
    )

    # Commit first so the worker cannot race ahead of the visible task row.
    db.commit()

    try:
        task_service.enqueue_task(task.id)
        return task
    except TaskDispatchError as exc:
        task_service.mark_dispatch_failed(task, str(exc))
        db.commit()
        db.refresh(task)
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@router.get("/{task_id}", response_model=TaskResponse)
def get_task(
    task_id: UUID,
    task_service: Annotated[TaskService, Depends(get_task_service)],
) -> TaskResponse:
    task = task_service.get_task(task_id)

    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

    return task


@router.get("", response_model=list[TaskResponse])
def list_tasks(
    task_service: Annotated[TaskService, Depends(get_task_service)],
    limit: Annotated[int, Query(ge=1)] = 100,
) -> list[TaskResponse]:
    return task_service.list_tasks(limit=limit)
