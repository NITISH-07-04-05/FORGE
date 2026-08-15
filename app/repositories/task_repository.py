from __future__ import annotations

from uuid import UUID

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.task import Task
from app.models.task_status import TaskStatus


class TaskRepository:
    """Persistence helper for Task entities within a caller-managed transaction."""

    def __init__(self, session: Session) -> None:
        # The service layer owns the session lifecycle and transaction boundary.
        self._session = session

    def create(self, task: Task) -> Task:
        # Flush so database-generated values are available without committing.
        self._session.add(task)
        self._session.flush()
        return task

    def get(self, task_id: UUID) -> Task | None:
        statement = select(Task).where(Task.id == task_id)
        return self._session.scalar(statement)

    def get_for_update(self, task_id: UUID) -> Task | None:
        statement = select(Task).where(Task.id == task_id).with_for_update()
        return self._session.scalar(statement)

    def list(self, limit: int = 100) -> list[Task]:
        statement = select(Task).order_by(Task.created_at.desc()).limit(limit)
        return list(self._session.scalars(statement))

    def list_retry_waiting_due(self, at: datetime, limit: int = 100) -> list[Task]:
        statement = (
            select(Task)
            .where(
                Task.status == TaskStatus.RETRY_WAITING,
                Task.next_retry_at.is_not(None),
                Task.retry_enqueued_at.is_(None),
                Task.next_retry_at <= at,
            )
            .order_by(Task.next_retry_at.asc(), Task.created_at.asc())
            .limit(limit)
        )
        return list(self._session.scalars(statement))

    def list_dead_lettered(self, limit: int = 100) -> list[Task]:
        statement = (
            select(Task)
            .where(Task.status == TaskStatus.DEAD_LETTERED)
            .order_by(Task.completed_at.desc())
            .limit(limit)
        )
        return list(self._session.scalars(statement))

    def update(self, task: Task) -> Task:
        # The task is expected to be attached to the current session before flushing.
        self._session.flush()
        return task

    def delete(self, task: Task) -> None:
        self._session.delete(task)
        self._session.flush()
