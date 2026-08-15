from __future__ import annotations

from typing import Any
from uuid import UUID

from app.models.task import Task
from app.models.task_status import TaskStatus
from app.queue.redis_queue import RedisQueue
from app.repositories.task_repository import TaskRepository


class TaskDispatchError(RuntimeError):
    """Raised when a task cannot be handed off to the worker queue."""


class TaskNotRecoverableError(RuntimeError):
    """Raised when a recovery is attempted on a task that is not DEAD_LETTERED."""


class TaskService:
    """Application-layer workflow for creating and retrieving tasks."""

    def __init__(
        self,
        task_repository: TaskRepository,
        queue: RedisQueue,
    ) -> None:
        # The service depends on persistence abstractions, not session management.
        self._task_repository = task_repository
        self._queue = queue

    def create_task(
        self,
        task_type: str,
        payload: dict[str, Any],
        max_retries: int = 0,
    ) -> Task:
        # The service owns task initialization; the caller controls commit and enqueue ordering.
        task = Task(
            task_type=task_type,
            status=TaskStatus.PENDING,
            max_retries=max_retries,
            retry_count=0,
            payload=dict(payload),
        )
        self._task_repository.create(task)
        return task

    def enqueue_task(self, task_id: UUID) -> None:
        try:
            self._queue.enqueue(task_id)
        except Exception as exc:
            raise TaskDispatchError("Task could not be enqueued after it was committed.") from exc

    def mark_dispatch_failed(self, task: Task, error_message: str) -> Task:
        task.mark_failed(error_message)
        return self._task_repository.update(task)

    def get_task(self, task_id: UUID) -> Task | None:
        return self._task_repository.get(task_id)

    def list_tasks(self, limit: int = 100) -> list[Task]:
        return self._task_repository.list(limit=limit)

    def list_dead_lettered(self, limit: int = 100) -> list[Task]:
        return self._task_repository.list_dead_lettered(limit=limit)

    def recover_task(self, task_id: UUID) -> Task:
        """Recover a dead-lettered task back to PENDING for re-execution.

        Acquires a row-level lock (FOR UPDATE) to prevent concurrent recoveries from racing.
        Raises TaskNotRecoverableError if the task is not DEAD_LETTERED, which
        covers both non-existent tasks and duplicate concurrent recovery attempts.
        """
        task = self._task_repository.get_for_update(task_id)

        if task is None or task.status != TaskStatus.DEAD_LETTERED:
            raise TaskNotRecoverableError(
                f"Task {task_id} is not in DEAD_LETTERED state and cannot be recovered."
            )

        task.recover()
        self._task_repository.update(task)
        return task
