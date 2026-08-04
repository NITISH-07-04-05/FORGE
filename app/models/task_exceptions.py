from __future__ import annotations

from app.models.task_status import TaskStatus


class InvalidTaskStateTransition(ValueError):
    """Raised when code attempts to move a task outside the allowed lifecycle."""

    def __init__(self, current_status: TaskStatus, target_status: TaskStatus) -> None:
        self.current_status = current_status
        self.target_status = target_status
        super().__init__(
            f"Cannot transition task from {current_status.value} to {target_status.value}."
        )
