from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.models.task_priority import TaskPriority


class TaskCreate(BaseModel):
    task_type: str
    payload: dict[str, Any]
    priority: TaskPriority = TaskPriority.NORMAL
    max_retries: int = Field(default=0, ge=0)
    scheduled_at: datetime | None = None
    delay_seconds: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_exclusive_schedule_and_delay(self) -> TaskCreate:
        if self.scheduled_at is not None and self.delay_seconds is not None:
            raise ValueError("Cannot provide both 'scheduled_at' and 'delay_seconds'")
        return self


class TaskResponse(BaseModel):
    # API responses can be built directly from ORM instances returned by SQLAlchemy.
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    task_type: str
    status: str
    priority: TaskPriority
    payload: dict[str, Any]
    max_retries: int
    retry_count: int
    next_retry_at: datetime | None
    scheduled_at: datetime | None = None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None
