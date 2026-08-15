from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from app.execution.handlers import EchoTaskHandler
from app.execution.registry import ExecutionRegistry, TaskHandler
from app.execution.worker import Worker
from app.models.task import Task
from app.models.task_status import TaskStatus


class FakeQueue:
    def __init__(self, task_ids: list[UUID] | None = None) -> None:
        self.task_ids = list(task_ids or [])

    def dequeue(self) -> UUID | None:
        if not self.task_ids:
            return None
        return self.task_ids.pop(0)


class FakeSession:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class InMemoryTaskRepository:
    def __init__(self, tasks: list[Task] | None = None) -> None:
        self.tasks = {task.id: task for task in tasks or []}

    def get(self, task_id: UUID) -> Task | None:
        return self.tasks.get(task_id)

    def update(self, task: Task) -> Task:
        self.tasks[task.id] = task
        return task


class FailingTaskHandler(TaskHandler):
    def execute(self, payload: dict[str, object]) -> None:
        raise RuntimeError("boom")


def make_task(task_type: str, payload: dict[str, object]) -> Task:
    task = Task(
        id=uuid4(),
        task_type=task_type,
        status=TaskStatus.PENDING,
        payload=payload,
        created_at=datetime.now(timezone.utc),
    )
    return task


def test_worker_completes_echo_task_and_executes_handler(caplog: pytest.LogCaptureFixture) -> None:
    task = make_task("echo", {"message": "Hello FORGE"})
    session = FakeSession()
    worker = Worker(
        queue=FakeQueue([task.id]),
        task_repository=InMemoryTaskRepository([task]),
        registry=ExecutionRegistry({"echo": EchoTaskHandler()}),
        session=session,
    )

    with caplog.at_level(logging.INFO):
        processed = worker.process_next_task()

    assert processed is True
    assert task.status == TaskStatus.COMPLETED
    assert task.started_at is not None
    assert task.completed_at is not None
    assert "Echo task executed" in caplog.text


def test_worker_marks_failing_handler_as_failed() -> None:
    task = make_task("boom", {"message": "Hello FORGE"})
    session = FakeSession()
    worker = Worker(
        queue=FakeQueue([task.id]),
        task_repository=InMemoryTaskRepository([task]),
        registry=ExecutionRegistry({"boom": FailingTaskHandler()}),
        session=session,
    )

    processed = worker.process_next_task()

    assert processed is True
    assert task.status == TaskStatus.FAILED
    assert task.error_message == "boom"
    assert session.rollbacks == 1


def test_unknown_task_type_fails_cleanly_and_worker_keeps_processing() -> None:
    unknown_task = make_task("missing", {"message": "first"})
    echo_task = make_task("echo", {"message": "second"})
    session = FakeSession()
    repository = InMemoryTaskRepository([unknown_task, echo_task])
    worker = Worker(
        queue=FakeQueue([unknown_task.id, echo_task.id]),
        task_repository=repository,
        registry=ExecutionRegistry({"echo": EchoTaskHandler()}),
        session=session,
    )

    assert worker.process_next_task() is True
    assert unknown_task.status == TaskStatus.FAILED
    assert "No task handler registered" in (unknown_task.error_message or "")

    assert worker.process_next_task() is True
    assert echo_task.status == TaskStatus.COMPLETED
