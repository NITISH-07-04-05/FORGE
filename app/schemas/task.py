from __future__ import annotations

from collections.abc import Mapping
from hashlib import sha256
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator, field_validator

from app.models.task_priority import TaskPriority


class TaskCreate(BaseModel):
    task_type: str
    payload: dict[str, Any]
    priority: TaskPriority = TaskPriority.NORMAL
    max_retries: int = Field(default=0, ge=0)
    scheduled_at: datetime | None = None
    delay_seconds: int | None = Field(default=None, ge=0)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=255)

    @model_validator(mode="after")
    def validate_exclusive_schedule_and_delay(self) -> TaskCreate:
        if self.scheduled_at is not None and self.delay_seconds is not None:
            raise ValueError("Cannot provide both 'scheduled_at' and 'delay_seconds'")
        return self

    @field_validator("idempotency_key")
    @classmethod
    def strip_idempotency_key(cls, value: str | None) -> str | None:
        if value is None:
            return None

        stripped = value.strip()
        if not stripped:
            raise ValueError("idempotency_key cannot be blank")
        return stripped

    def fingerprint(self) -> str:
        normalized: dict[str, Any] = {
            "task_type": self.task_type,
            "payload": self.payload,
            "priority": self.priority.value,
            "max_retries": self.max_retries,
            "scheduled_at": self.scheduled_at.isoformat() if self.scheduled_at is not None else None,
            "delay_seconds": self.delay_seconds,
        }
        return sha256(_stable_json(normalized).encode("utf-8")).hexdigest()


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


def _stable_json(value: Mapping[str, Any]) -> str:
    import json

    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
