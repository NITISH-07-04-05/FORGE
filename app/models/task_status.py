from __future__ import annotations

from enum import Enum


class TaskStatus(str, Enum):
    """Canonical task lifecycle states shared across the application."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRY_WAITING = "RETRY_WAITING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
