from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import JSON, DateTime, Enum as SqlEnum, Integer, String, Text, Uuid, func, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.task_exceptions import InvalidTaskStateTransition
from app.models.task_status import TaskStatus


task_status_type = SqlEnum(
    TaskStatus,
    name="task_status",
    native_enum=False,
    create_constraint=False,
    validate_strings=True,
    length=50,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


ALLOWED_TASK_STATUS_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.RUNNING, TaskStatus.FAILED}),
    TaskStatus.RUNNING: frozenset({TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.RETRY_WAITING}),
    TaskStatus.RETRY_WAITING: frozenset({TaskStatus.RUNNING, TaskStatus.FAILED}),
    TaskStatus.COMPLETED: frozenset(),
    TaskStatus.FAILED: frozenset(),
}


class Task(Base):
    __tablename__ = "tasks"

    # UUID keys are generated in application code so tasks can be referenced before commit.
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    task_type: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[TaskStatus] = mapped_column(
        task_status_type,
        nullable=False,
        default=TaskStatus.PENDING,
        server_default=text(f"'{TaskStatus.PENDING.value}'"),
    )
    max_retries: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    retry_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    retry_enqueued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # A JSON default keeps the task payload predictable even when callers omit it.
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        default=dict,
        server_default=text("'{}'::json"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    def _transition_to(self, target_status: TaskStatus) -> None:
        allowed_statuses = ALLOWED_TASK_STATUS_TRANSITIONS[self.status]

        if target_status not in allowed_statuses:
            raise InvalidTaskStateTransition(self.status, target_status)

        self.status = target_status

    def mark_running(self, at: datetime | None = None) -> datetime:
        """Centralize running-state transitions so timestamps stay consistent."""
        started_at = at or _utcnow()
        self._transition_to(TaskStatus.RUNNING)
        self.started_at = self.started_at or started_at
        self.completed_at = None
        self.error_message = None
        self.next_retry_at = None
        self.retry_enqueued_at = None
        return self.started_at

    def mark_completed(self, at: datetime | None = None) -> datetime:
        """Mark a task as finished successfully and stamp its completion time."""
        completed_at = at or _utcnow()
        self._transition_to(TaskStatus.COMPLETED)
        self.completed_at = completed_at
        self.error_message = None
        self.next_retry_at = None
        self.retry_enqueued_at = None
        return completed_at

    def mark_retry_waiting(
        self,
        error_message: str,
        next_retry_at: datetime,
        at: datetime | None = None,
    ) -> datetime:
        """Move a failed execution into the delayed retry state."""
        completed_at = at or _utcnow()
        self._transition_to(TaskStatus.RETRY_WAITING)
        self.retry_count += 1
        self.completed_at = completed_at
        self.error_message = error_message
        self.next_retry_at = next_retry_at
        self.retry_enqueued_at = None
        return completed_at

    def mark_failed(
        self,
        error_message: str,
        at: datetime | None = None,
        started_at: datetime | None = None,
    ) -> datetime:
        """Record failure metadata in one place so workers stay orchestration-only."""
        completed_at = at or _utcnow()
        self._transition_to(TaskStatus.FAILED)
        self.started_at = self.started_at or started_at
        self.completed_at = completed_at
        self.error_message = error_message
        self.next_retry_at = None
        self.retry_enqueued_at = None
        return completed_at
