from __future__ import annotations

from typing import Any
from uuid import UUID

from app.models.task import Task
from app.models.task_status import TaskStatus
from app.repositories.task_repository import TaskRepository


class TaskService:
    """Application-layer workflow for creating and retrieving tasks."""

    def __init__(self, task_repository: TaskRepository) -> None:
        # The service depends on persistence abstractions, not session management.
        self._task_repository = task_repository

    def create_task(self, task_type: str, payload: dict[str, Any]) -> Task:
        # The service owns task initialization so future queue dispatch can plug in here.
        task = Task(
            task_type=task_type,
            status=TaskStatus.PENDING,
            payload=dict(payload),
        )
        return self._task_repository.create(task)

    def get_task(self, task_id: UUID) -> Task | None:
        return self._task_repository.get(task_id)

    def list_tasks(self, limit: int = 100) -> list[Task]:
        return self._task_repository.list(limit=limit)
