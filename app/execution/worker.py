from __future__ import annotations

from datetime import datetime
import logging
from uuid import UUID

from sqlalchemy.orm import Session

from app.execution.registry import ExecutionRegistry
from app.models.task import Task
from app.models.task_status import TaskStatus
from app.queue.redis_queue import RedisQueue
from app.repositories.task_repository import TaskRepository

logger = logging.getLogger(__name__)


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
            self.process_next_task()

    def stop(self) -> None:
        self._running = False

    def process_next_task(self) -> bool:
        task_id = self._queue.dequeue()

        if task_id is None:
            return False

        self._process_task(task_id)
        return True

    def _process_task(self, task_id: UUID) -> None:
        task = self._load_task(task_id)

        if task is None:
            logger.warning("Skipping queued task %s because it no longer exists.", task_id)
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
            logger.exception("Task %s failed during execution.", task_id)
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
            logger.error(
                "Unable to record failure for task %s because RUNNING was never persisted.",
                task_id,
            )
            return

        # Real database rollbacks restore PENDING, but in-memory tests may still hold RUNNING.
        if task.status == TaskStatus.PENDING:
            task.mark_running(at=started_at)
        task.mark_failed(error_message)

        try:
            self._task_repository.update(task)
            self._session.commit()
        except Exception:
            self._session.rollback()
            logger.exception("Failed to persist FAILED state for task %s.", task_id)
