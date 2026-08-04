from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class TaskCreate(BaseModel):
    task_type: str
    payload: dict[str, Any]


class TaskResponse(BaseModel):
    # API responses can be built directly from ORM instances returned by SQLAlchemy.
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    task_type: str
    status: str
    payload: dict[str, Any]
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None
