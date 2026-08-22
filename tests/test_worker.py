from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from app.execution.handlers import EchoTaskHandler
from app.execution.lease import TaskLeaseManager
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


class FakeLeaseManager:
    def __init__(self, acquire_result: bool = True) -> None:
        self.acquire_result = acquire_result
        self.owners: dict[UUID, str] = {}
        self.released: list[tuple[UUID, str]] = []
        self.acquired: list[tuple[UUID, str, int]] = []

    def acquire(self, task_id: UUID, worker_id: str, ttl: int) -> bool:
        self.acquired.append((task_id, worker_id, ttl))
        if not self.acquire_result or task_id in self.owners:
            return False
        self.owners[task_id] = worker_id
        return True

    def is_owner(self, task_id: UUID, worker_id: str) -> bool:
        return self.owners.get(task_id) == worker_id

    def release(self, task_id: UUID, worker_id: str) -> bool:
        self.released.append((task_id, worker_id))
        if self.owners.get(task_id) == worker_id:
            del self.owners[task_id]
            return True
        return False


def make_task(task_type: str, payload: dict[str, object]) -> Task:
    task = Task(
        id=uuid4(),
        task_type=task_type,
        status=TaskStatus.PENDING,
        max_retries=0,
        retry_count=0,
        next_retry_at=None,
        retry_enqueued_at=None,
        payload=payload,
        created_at=datetime.now(timezone.utc),
    )
    return task


def make_worker(
    task: Task,
    *,
    handler: TaskHandler,
    lease_manager: FakeLeaseManager | None = None,
) -> tuple[Worker, FakeSession]:
    session = FakeSession()
    worker = Worker(
        queue=FakeQueue([task.id]),
        task_repository=InMemoryTaskRepository([task]),
        registry=ExecutionRegistry({task.task_type: handler}),
        session=session,
        lease_manager=lease_manager,
    )
    return worker, session


def test_worker_completes_echo_task_and_executes_handler(caplog: pytest.LogCaptureFixture) -> None:
    task = make_task("echo", {"message": "Hello FORGE"})
    lease_manager = FakeLeaseManager()
    worker, session = make_worker(task, handler=EchoTaskHandler(), lease_manager=lease_manager)

    with caplog.at_level(logging.INFO):
        processed = worker.process_next_task()

    assert processed is True
    assert task.status == TaskStatus.COMPLETED
    assert task.started_at is not None
    assert task.completed_at is not None
    assert lease_manager.released == [(task.id, worker.worker_id)]
    assert "Echo task executed" in caplog.text


def test_worker_marks_failing_handler_as_failed() -> None:
    task = make_task("boom", {"message": "Hello FORGE"})
    lease_manager = FakeLeaseManager()
    worker, session = make_worker(task, handler=FailingTaskHandler(), lease_manager=lease_manager)

    processed = worker.process_next_task()

    assert processed is True
    assert task.status == TaskStatus.FAILED
    assert task.error_message == "boom"
    assert session.rollbacks == 1
    assert lease_manager.released == [(task.id, worker.worker_id)]


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


def test_worker_skips_task_when_lease_is_already_held() -> None:
    task = make_task("echo", {"message": "Hello FORGE"})
    lease_manager = FakeLeaseManager(acquire_result=False)
    worker, session = make_worker(task, handler=EchoTaskHandler(), lease_manager=lease_manager)

    processed = worker.process_next_task()

    assert processed is True
    assert task.status == TaskStatus.PENDING
    assert session.commits == 0
    assert session.rollbacks == 0
    assert lease_manager.released == []


def test_worker_releases_lease_after_retryable_failure() -> None:
    task = make_task("boom", {"message": "Hello FORGE"})
    task.max_retries = 1
    lease_manager = FakeLeaseManager()
    worker, session = make_worker(task, handler=FailingTaskHandler(), lease_manager=lease_manager)

    processed = worker.process_next_task()

    assert processed is True
    assert task.status == TaskStatus.RETRY_WAITING
    assert session.commits == 1
    assert lease_manager.released == [(task.id, worker.worker_id)]


def test_lost_lease_prevents_stale_worker_from_finalizing_state() -> None:
    task = make_task("boom", {"message": "Hello FORGE"})
    lease_manager = FakeLeaseManager()

    class LeaseDroppingHandler(TaskHandler):
        def execute(self, payload: dict[str, object]) -> None:
            lease_manager.owners.pop(task.id, None)
            raise RuntimeError("boom")

    worker, session = make_worker(task, handler=LeaseDroppingHandler(), lease_manager=lease_manager)

    processed = worker.process_next_task()

    assert processed is True
    assert task.status == TaskStatus.RUNNING
    assert task.completed_at is None
    assert session.rollbacks == 1
    assert lease_manager.released == [(task.id, worker.worker_id)]


def test_retry_and_dlq_behaviour_still_works_with_leases() -> None:
    retry_task = make_task("boom", {"message": "retry"})
    retry_task.max_retries = 1
    dlq_task = make_task("boom", {"message": "dlq"})
    dlq_task.max_retries = 0
    lease_manager = FakeLeaseManager()
    session = FakeSession()
    repo = InMemoryTaskRepository([retry_task, dlq_task])
    queue = FakeQueue([retry_task.id, dlq_task.id])
    worker = Worker(
        queue=queue,
        task_repository=repo,
        registry=ExecutionRegistry({"boom": FailingTaskHandler()}),
        session=session,
        lease_manager=lease_manager,
    )

    assert worker.process_next_task() is True
    assert retry_task.status == TaskStatus.RETRY_WAITING
    assert worker.process_next_task() is True
    assert dlq_task.status == TaskStatus.FAILED
