from __future__ import annotations

from typing import Any

from app.execution.registry import TaskHandler


class FailTaskHandler(TaskHandler):
    """Intentional failure handler used to verify worker error handling in V1."""

    def execute(self, payload: dict[str, Any]) -> None:
        raise RuntimeError("intentional failure")
