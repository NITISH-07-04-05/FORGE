from __future__ import annotations

from enum import Enum


class TaskPriority(str, Enum):
    """Canonical task execution priority levels."""

    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"
