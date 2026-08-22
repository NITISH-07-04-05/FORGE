from __future__ import annotations

from uuid import UUID

from datetime import datetime

from sqlalchemy.exc import IntegrityError
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

    def create_or_get_by_idempotency_key(self, task: Task) -> tuple[Task, bool]:
        self._session.add(task)
        try:
            self._session.flush()
        except IntegrityError:
            self._session.rollback()
            existing = self.get_by_idempotency_key(task.idempotency_key)
            if existing is None:
                raise
            return existing, False
        return task, True

    def get(self, task_id: UUID) -> Task | None:
        statement = select(Task).where(Task.id == task_id)
        return self._session.scalar(statement)

    def get_for_update(self, task_id: UUID) -> Task | None:
        statement = select(Task).where(Task.id == task_id).with_for_update()
        return self._session.scalar(statement)

    def get_by_idempotency_key(self, idempotency_key: str | None) -> Task | None:
        if idempotency_key is None:
            return None
        statement = select(Task).where(Task.idempotency_key == idempotency_key)
        return self._session.scalar(statement)

    def list(self, limit: int = 100) -> list[Task]:
        statement = select(Task).order_by(Task.created_at.desc()).limit(limit)
        return list(self._session.scalars(statement))

    def list_scheduled_due(
        self,
        at: datetime,
        limit: int = 100,
        for_update: bool = True,
        skip_locked: bool = True,
    ) -> list[Task]:
        statement = (
            select(Task)
            .where(
                Task.status == TaskStatus.SCHEDULED,
                Task.scheduled_at.is_not(None),
                Task.scheduled_at <= at,
            )
            .order_by(Task.scheduled_at.asc(), Task.created_at.asc())
            .limit(limit)
        )
        if for_update:
            statement = statement.with_for_update(skip_locked=skip_locked)
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
