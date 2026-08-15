from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.models.task_priority import TaskPriority


class TaskCreate(BaseModel):
    task_type: str
    payload: dict[str, Any]
    priority: TaskPriority = TaskPriority.NORMAL
    max_retries: int = Field(default=0, ge=0)
    scheduled_at: datetime | None = None


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
