from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.dependencies import get_db
from app.repositories.task_repository import TaskRepository
from app.schemas.task import TaskCreate, TaskResponse
from app.services.task_service import TaskService

router = APIRouter(prefix="/tasks", tags=["Tasks"])


def get_task_repository(db: Annotated[Session, Depends(get_db)]) -> TaskRepository:
    # Repositories stay focused on persistence and share the request-scoped session.
    return TaskRepository(db)


def get_task_service(
    task_repository: Annotated[TaskRepository, Depends(get_task_repository)],
) -> TaskService:
    # The router depends on the application service instead of embedding workflow logic.
    return TaskService(task_repository)


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
    # The API owns the transaction boundary for this request lifecycle.
    db.commit()
    db.refresh(task)
    return task


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
