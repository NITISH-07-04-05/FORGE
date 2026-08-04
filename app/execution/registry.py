from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping


class UnknownTaskTypeError(LookupError):
    """Raised when no handler has been registered for a task type."""

    def __init__(self, task_type: str) -> None:
        self.task_type = task_type
        super().__init__(f"No task handler registered for task type '{task_type}'.")


class TaskHandler(ABC):
    """Abstract execution contract so workers stay decoupled from concrete tasks."""

    @abstractmethod
    def execute(self, payload: dict[str, Any]) -> None:
        """Run the task with the provided payload."""


class DummyTaskHandler(TaskHandler):
    """Simple handler for tests and local wiring before real handlers exist."""

    def __init__(self) -> None:
        self.executed_payloads: list[dict[str, Any]] = []

    def execute(self, payload: dict[str, Any]) -> None:
        # Storing a shallow copy makes test assertions stable across later mutations.
        self.executed_payloads.append(dict(payload))


class ExecutionRegistry:
    """Central registry that resolves task types to execution handlers."""

    def __init__(self, handlers: Mapping[str, TaskHandler] | None = None) -> None:
        # Accepting initial registrations makes startup-time auto-registration easy later.
        self._handlers: dict[str, TaskHandler] = {}

        if handlers is not None:
            for task_type, handler in handlers.items():
                self.register(task_type, handler)

    def register(self, task_type: str, handler: TaskHandler) -> None:
        self._handlers[task_type] = handler

    def get(self, task_type: str) -> TaskHandler:
        try:
            return self._handlers[task_type]
        except KeyError as exc:
            raise UnknownTaskTypeError(task_type) from exc
