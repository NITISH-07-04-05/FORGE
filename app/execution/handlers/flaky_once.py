from __future__ import annotations

from typing import Any

from app.execution.registry import TaskHandler


class FlakyOnceTaskHandler(TaskHandler):
    """Demo handler that fails once per unique payload key and then succeeds."""

    def __init__(self) -> None:
        self._failed_keys: set[str] = set()

    def execute(self, payload: dict[str, Any]) -> None:
        key = str(payload.get("retry_key") or payload.get("message") or "default")

        if key not in self._failed_keys:
            self._failed_keys.add(key)
            raise RuntimeError("transient failure")
