from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy.orm import Session

from app.execution.registry import ExecutionRegistry
from app.models.task import Task
from app.queue.redis_queue import RedisQueue
from app.repositories.task_repository import TaskRepository


class Worker:
    """Small orchestration loop that bridges the queue, registry, and persistence layer."""

    def __init__(
        self,
        queue: RedisQueue,
        task_repository: TaskRepository,
        registry: ExecutionRegistry,
        session: Session,
    ) -> None:
        self._queue = queue
        self._task_repository = task_repository
        self._registry = registry
        # The worker owns transaction boundaries for task execution state changes.
        self._session = session
        self._running = False

    def run(self) -> None:
        self._running = True

        while self._running:
            task_id = self._queue.dequeue()

            if task_id is None:
                continue

            self._process_task(task_id)

    def stop(self) -> None:
        self._running = False

    def _process_task(self, task_id: UUID) -> None:
        task = self._load_task(task_id)

        if task is None:
            return

        started_at: datetime | None = None

        try:
            started_at = self._mark_running(task)
            handler = self._registry.get(task.task_type)
            handler.execute(task.payload)
            self._mark_completed(task)
            self._session.commit()
        except Exception as exc:
            self._session.rollback()
            self._mark_failed(task_id, str(exc), started_at=started_at)

    def _load_task(self, task_id: UUID) -> Task | None:
        return self._task_repository.get(task_id)

    def _mark_running(self, task: Task) -> datetime:
        started_at = task.mark_running()
        self._task_repository.update(task)
        return started_at

    def _mark_completed(self, task: Task) -> None:
        task.mark_completed()
        self._task_repository.update(task)

    def _mark_failed(
        self,
        task_id: UUID,
        error_message: str,
        started_at: datetime | None,
    ) -> None:
        # Failure is recorded in a fresh transaction after rollback so the worker keeps moving.
        task = self._load_task(task_id)

        if task is None:
            return

        if started_at is None:
            return

        # Replaying RUNNING in-memory preserves the allowed lifecycle after a rollback.
        task.mark_running(at=started_at)
        task.mark_failed(error_message)

        try:
            self._task_repository.update(task)
            self._session.commit()
        except Exception:
            self._session.rollback()
