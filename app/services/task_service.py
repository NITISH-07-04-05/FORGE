from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from app.models.task import Task
from app.models.task_priority import TaskPriority
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
        priority: TaskPriority = TaskPriority.NORMAL,
        max_retries: int = 0,
        scheduled_at: datetime | None = None,
        delay_seconds: int | None = None,
    ) -> Task:
        now = datetime.now(timezone.utc)
        target_scheduled_at: datetime | None = None

        if delay_seconds is not None:
            if delay_seconds > 0:
                target_scheduled_at = now + timedelta(seconds=delay_seconds)
            else:
                target_scheduled_at = None
        elif scheduled_at is not None:
            if scheduled_at.tzinfo is None:
                target_scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)
            else:
                target_scheduled_at = scheduled_at

        if target_scheduled_at is not None and target_scheduled_at > now:
            status = TaskStatus.SCHEDULED
        else:
            status = TaskStatus.PENDING

        # The service owns task initialization; the caller controls commit and enqueue ordering.
        task = Task(
            task_type=task_type,
            status=status,
            priority=priority,
            max_retries=max_retries,
            retry_count=0,
            scheduled_at=target_scheduled_at,
            payload=dict(payload),
        )
        self._task_repository.create(task)
        return task

    def enqueue_task(self, task_id: UUID, priority: TaskPriority = TaskPriority.NORMAL) -> None:
        try:
            self._queue.enqueue(task_id, priority=priority)
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
