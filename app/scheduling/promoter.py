from __future__ import annotations

from datetime import datetime, timezone
import logging

from sqlalchemy.orm import Session

from app.queue.redis_queue import RedisQueue
from app.repositories.task_repository import TaskRepository

logger = logging.getLogger(__name__)


class TaskPromoter:
    """Orchestrates promoting due SCHEDULED tasks to PENDING and enqueuing them to the ready Redis queue."""

    def __init__(
        self,
        queue: RedisQueue,
        task_repository: TaskRepository,
        session: Session,
    ) -> None:
        self._queue = queue
        self._task_repository = task_repository
        self._session = session

    def promote_due_tasks(self, at: datetime | None = None, limit: int = 100) -> int:
        """Scan and promote all due SCHEDULED tasks whose scheduled_at <= at."""
        eval_time = at or datetime.now(timezone.utc)
        if eval_time.tzinfo is None:
            eval_time = eval_time.replace(tzinfo=timezone.utc)

        due_tasks = self._task_repository.list_scheduled_due(at=eval_time, limit=limit)
        promoted_count = 0

        for task in due_tasks:
            try:
                task.mark_pending()
                self._task_repository.update(task)
                self._session.commit()
            except Exception:
                self._session.rollback()
                logger.exception("Failed to update status to PENDING for task %s.", task.id)
                continue

            try:
                self._queue.enqueue(task.id, priority=task.priority)
                promoted_count += 1
            except Exception as exc:
                logger.exception("Failed to enqueue promoted task %s to Redis queue.", task.id)
                try:
                    task.mark_failed(f"Dispatch failed during promotion: {exc}")
                    self._task_repository.update(task)
                    self._session.commit()
                except Exception:
                    self._session.rollback()
                    logger.exception("Failed to record dispatch failure for task %s.", task.id)

        return promoted_count
