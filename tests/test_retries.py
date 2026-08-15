from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from app.execution.registry import ExecutionRegistry, TaskHandler
from app.execution.worker import Worker
from app.execution.handlers import EchoTaskHandler
from app.models.task import Task
from app.models.task_status import TaskStatus


class RetryQueue:
    def __init__(self, ready_task_ids: list[UUID] | None = None) -> None:
        self.ready_task_ids = list(ready_task_ids or [])
        self.scheduled: dict[UUID, datetime] = {}

    def enqueue(self, task_id: UUID) -> None:
        self.ready_task_ids.append(task_id)

    def enqueue_delayed(self, task_id: UUID, run_at: datetime) -> None:
        self.scheduled[task_id] = run_at

    def dequeue(self) -> UUID | None:
        now = datetime.now(timezone.utc)
        due_task_ids = [task_id for task_id, run_at in self.scheduled.items() if run_at <= now]

        for task_id in due_task_ids:
            self.ready_task_ids.append(task_id)
            del self.scheduled[task_id]

        if not self.ready_task_ids:
            return None

        return self.ready_task_ids.pop(0)


class RetrySession:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class RetryRepository:
    def __init__(self, tasks: list[Task] | None = None) -> None:
        self.tasks = {task.id: task for task in tasks or []}

    def get(self, task_id: UUID) -> Task | None:
        return self.tasks.get(task_id)

    def update(self, task: Task) -> Task:
        self.tasks[task.id] = task
        return task

    def list_retry_waiting_due(self, at: datetime, limit: int = 100) -> list[Task]:
        due_tasks = [
            task
            for task in self.tasks.values()
            if task.status == TaskStatus.RETRY_WAITING
            and task.next_retry_at is not None
            and task.retry_enqueued_at is None
            and task.next_retry_at <= at
        ]
        due_tasks.sort(key=lambda task: (task.next_retry_at, task.created_at))
        return due_tasks[:limit]


class AlwaysFailTaskHandler(TaskHandler):
    def execute(self, payload: dict[str, object]) -> None:
        raise RuntimeError("boom")


class FailOnceTaskHandler(TaskHandler):
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, payload: dict[str, object]) -> None:
        self.calls += 1

        if self.calls == 1:
            raise RuntimeError("boom")


def make_task(
    task_type: str,
    *,
    status: TaskStatus = TaskStatus.PENDING,
    max_retries: int = 0,
    retry_count: int = 0,
    next_retry_at: datetime | None = None,
    retry_enqueued_at: datetime | None = None,
) -> Task:
    return Task(
        id=uuid4(),
        task_type=task_type,
        status=status,
        max_retries=max_retries,
        retry_count=retry_count,
        next_retry_at=next_retry_at,
        retry_enqueued_at=retry_enqueued_at,
        payload={"message": "payload"},
        created_at=datetime.now(timezone.utc),
    )


def build_worker(
    tasks: list[Task],
    handlers: dict[str, TaskHandler],
    *,
    ready_task_ids: list[UUID] | None = None,
    retry_base_delay_seconds: int = 1,
) -> tuple[Worker, RetryRepository, RetryQueue, RetrySession]:
    queue = RetryQueue(ready_task_ids=ready_task_ids)
    repository = RetryRepository(tasks)
    session = RetrySession()
    worker = Worker(
        queue=queue,
        task_repository=repository,
        registry=ExecutionRegistry(handlers),
        session=session,
        retry_base_delay_seconds=retry_base_delay_seconds,
    )
    return worker, repository, queue, session


def test_no_retries_fail_permanently() -> None:
    task = make_task("boom", max_retries=0)
    worker, _, queue, _ = build_worker(
        [task],
        {"boom": AlwaysFailTaskHandler()},
        ready_task_ids=[task.id],
        retry_base_delay_seconds=0,
    )

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.FAILED
    assert task.retry_count == 0
    assert task.next_retry_at is None
    assert queue.scheduled == {}


def test_one_retry_then_failure() -> None:
    task = make_task("boom", max_retries=1)
    worker, _, queue, _ = build_worker(
        [task],
        {"boom": AlwaysFailTaskHandler()},
        ready_task_ids=[task.id],
        retry_base_delay_seconds=0,
    )

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.RETRY_WAITING
    assert task.retry_count == 1
    assert task.next_retry_at is not None
    assert task.id in queue.scheduled

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.FAILED
    assert task.retry_count == 1
    assert queue.scheduled == {}


def test_successful_retry_completes_on_second_attempt() -> None:
    task = make_task("flaky", max_retries=1)
    handler = FailOnceTaskHandler()
    worker, _, queue, _ = build_worker(
        [task],
        {"flaky": handler},
        ready_task_ids=[task.id],
        retry_base_delay_seconds=0,
    )

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.RETRY_WAITING
    assert task.retry_count == 1
    assert task.id in queue.scheduled

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.COMPLETED
    assert task.retry_count == 1
    assert handler.calls == 2


def test_retry_exhaustion_becomes_terminal_failure() -> None:
    task = make_task("boom", max_retries=2)
    worker, _, queue, _ = build_worker(
        [task],
        {"boom": AlwaysFailTaskHandler()},
        ready_task_ids=[task.id],
        retry_base_delay_seconds=0,
    )

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.RETRY_WAITING
    assert task.retry_count == 1

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.RETRY_WAITING
    assert task.retry_count == 2

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.FAILED
    assert task.retry_count == 2
    assert queue.scheduled == {}


def test_retry_backoff_uses_configured_base_delay() -> None:
    task = make_task("boom", max_retries=1)
    worker, _, queue, _ = build_worker(
        [task],
        {"boom": AlwaysFailTaskHandler()},
        ready_task_ids=[task.id],
        retry_base_delay_seconds=2,
    )

    before = datetime.now(timezone.utc)

    assert worker.process_next_task() is True
    scheduled_at = queue.scheduled[task.id]

    assert task.status == TaskStatus.RETRY_WAITING
    assert 1.5 <= (scheduled_at - before).total_seconds() <= 2.5


def test_worker_keeps_processing_after_repeated_failure() -> None:
    failing_task = make_task("boom", max_retries=1)
    echo_task = make_task("echo", max_retries=0)
    worker, _, queue, _ = build_worker(
        [failing_task, echo_task],
        {"boom": AlwaysFailTaskHandler(), "echo": EchoTaskHandler()},
        ready_task_ids=[failing_task.id],
        retry_base_delay_seconds=0,
    )

    assert worker.process_next_task() is True
    assert failing_task.status == TaskStatus.RETRY_WAITING

    assert worker.process_next_task() is True
    assert failing_task.status == TaskStatus.FAILED

    queue.enqueue(echo_task.id)

    assert worker.process_next_task() is True
    assert echo_task.status == TaskStatus.COMPLETED


def test_due_retry_rows_are_scheduled_by_worker_scan() -> None:
    due_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    task = make_task(
        "boom",
        status=TaskStatus.RETRY_WAITING,
        max_retries=3,
        retry_count=1,
        next_retry_at=due_at,
        retry_enqueued_at=None,
    )
    worker, repository, queue, _ = build_worker(
        [task],
        {"boom": AlwaysFailTaskHandler()},
        retry_base_delay_seconds=0,
    )

    worker._schedule_due_retries()

    assert queue.scheduled[task.id] == due_at
    assert repository.tasks[task.id].retry_enqueued_at == due_at
