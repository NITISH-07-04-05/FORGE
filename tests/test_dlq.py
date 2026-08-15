from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from app.api.routers.tasks import get_db, get_redis_queue, get_task_repository
from app.execution.registry import ExecutionRegistry, TaskHandler
from app.execution.worker import Worker
from app.execution.handlers import EchoTaskHandler
from app.main import create_app
from app.models.task import Task
from app.models.task_status import TaskStatus
from app.services.task_service import TaskNotRecoverableError


# ---------------------------------------------------------------------------
# Shared test doubles (reuse the same in-memory pattern as test_retries.py)
# ---------------------------------------------------------------------------


class DLQQueue:
    def __init__(self, ready_task_ids: list[UUID] | None = None) -> None:
        self.ready_task_ids = list(ready_task_ids or [])
        self.scheduled: dict[UUID, datetime] = {}
        self.enqueued: list[UUID] = []

    def enqueue(self, task_id: UUID) -> None:
        self.ready_task_ids.append(task_id)
        self.enqueued.append(task_id)

    def enqueue_delayed(self, task_id: UUID, run_at: datetime) -> None:
        self.scheduled[task_id] = run_at

    def dequeue(self) -> UUID | None:
        now = datetime.now(timezone.utc)
        due = [tid for tid, run_at in self.scheduled.items() if run_at <= now]
        for tid in due:
            self.ready_task_ids.append(tid)
            del self.scheduled[tid]
        if not self.ready_task_ids:
            return None
        return self.ready_task_ids.pop(0)


class DLQSession:
    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


class DLQRepository:
    def __init__(self, tasks: list[Task] | None = None) -> None:
        self.tasks: dict[UUID, Task] = {task.id: task for task in (tasks or [])}

    def get(self, task_id: UUID) -> Task | None:
        return self.tasks.get(task_id)

    def get_for_update(self, task_id: UUID) -> Task | None:
        return self.tasks.get(task_id)

    def update(self, task: Task) -> Task:
        self.tasks[task.id] = task
        return task

    def list_retry_waiting_due(self, at: datetime, limit: int = 100) -> list[Task]:
        due = [
            t for t in self.tasks.values()
            if t.status == TaskStatus.RETRY_WAITING
            and t.next_retry_at is not None
            and t.retry_enqueued_at is None
            and t.next_retry_at <= at
        ]
        due.sort(key=lambda t: (t.next_retry_at, t.created_at))
        return due[:limit]

    def list_dead_lettered(self, limit: int = 100) -> list[Task]:
        dl = [t for t in self.tasks.values() if t.status == TaskStatus.DEAD_LETTERED]
        dl.sort(key=lambda t: t.completed_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return dl[:limit]


class AlwaysFailHandler(TaskHandler):
    def execute(self, payload: dict[str, object]) -> None:
        raise RuntimeError("boom")


def make_task(
    task_type: str,
    *,
    status: TaskStatus = TaskStatus.PENDING,
    max_retries: int = 0,
    retry_count: int = 0,
) -> Task:
    return Task(
        id=uuid4(),
        task_type=task_type,
        status=status,
        max_retries=max_retries,
        retry_count=retry_count,
        payload={"x": 1},
        created_at=datetime.now(timezone.utc),
    )


def build_worker(
    tasks: list[Task],
    handlers: dict[str, TaskHandler],
    *,
    ready_task_ids: list[UUID] | None = None,
) -> tuple[Worker, DLQRepository, DLQQueue, DLQSession]:
    queue = DLQQueue(ready_task_ids=ready_task_ids)
    repo = DLQRepository(tasks)
    session = DLQSession()
    worker = Worker(
        queue=queue,
        task_repository=repo,
        registry=ExecutionRegistry(handlers),
        session=session,
        retry_base_delay_seconds=0,
    )
    return worker, repo, queue, session


# ---------------------------------------------------------------------------
# Worker-level unit tests
# ---------------------------------------------------------------------------


def test_retry_exhaustion_with_retries_configured_becomes_dead_lettered() -> None:
    """A task with max_retries=2 that always fails enters DEAD_LETTERED after 3 attempts."""
    task = make_task("boom", max_retries=2)
    worker, repo, queue, _ = build_worker(
        [task], {"boom": AlwaysFailHandler()}, ready_task_ids=[task.id]
    )

    # Attempt 1 → RETRY_WAITING (retry_count=1)
    assert worker.process_next_task() is True
    assert task.status == TaskStatus.RETRY_WAITING
    assert task.retry_count == 1

    # Attempt 2 → RETRY_WAITING (retry_count=2)
    assert worker.process_next_task() is True
    assert task.status == TaskStatus.RETRY_WAITING
    assert task.retry_count == 2

    # Attempt 3 → retry_count == max_retries → DEAD_LETTERED
    assert worker.process_next_task() is True
    assert task.status == TaskStatus.DEAD_LETTERED
    assert task.retry_count == 2


def test_zero_retry_failure_remains_failed_not_dead_lettered() -> None:
    """A task with max_retries=0 that fails goes to FAILED, not DEAD_LETTERED (V1 behavior)."""
    task = make_task("boom", max_retries=0)
    worker, _, _, _ = build_worker(
        [task], {"boom": AlwaysFailHandler()}, ready_task_ids=[task.id]
    )

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.FAILED
    assert task.retry_count == 0


def test_dead_lettered_task_preserves_error_and_timestamps() -> None:
    """A dead-lettered task retains error_message, retry_count, and completed_at."""
    task = make_task("boom", max_retries=1)
    worker, _, _, _ = build_worker(
        [task], {"boom": AlwaysFailHandler()}, ready_task_ids=[task.id]
    )

    worker.process_next_task()  # → RETRY_WAITING
    worker.process_next_task()  # → DEAD_LETTERED

    assert task.status == TaskStatus.DEAD_LETTERED
    assert task.error_message == "boom"
    assert task.retry_count == 1
    assert task.completed_at is not None
    assert task.next_retry_at is None
    assert task.retry_enqueued_at is None


def test_dead_lettered_task_not_in_delayed_queue() -> None:
    """After dead-lettering, no delayed retry entry remains in the queue."""
    task = make_task("boom", max_retries=1)
    worker, _, queue, _ = build_worker(
        [task], {"boom": AlwaysFailHandler()}, ready_task_ids=[task.id]
    )

    worker.process_next_task()  # → RETRY_WAITING (scheduled in queue)
    assert task.id in queue.scheduled

    worker.process_next_task()  # → DEAD_LETTERED (consumed from queue)
    assert task.id not in queue.scheduled
    assert task.id not in queue.ready_task_ids


def test_worker_continues_processing_after_dead_lettering() -> None:
    """Dead-lettering one task must not stop the worker from processing the next."""
    failing = make_task("boom", max_retries=1)
    success = make_task("echo", max_retries=0)
    worker, _, queue, _ = build_worker(
        [failing, success],
        {"boom": AlwaysFailHandler(), "echo": EchoTaskHandler()},
        ready_task_ids=[failing.id],
    )

    worker.process_next_task()  # → RETRY_WAITING
    worker.process_next_task()  # → DEAD_LETTERED

    queue.enqueue(success.id)
    assert worker.process_next_task() is True
    assert success.status == TaskStatus.COMPLETED


# ---------------------------------------------------------------------------
# Task model unit tests — recover()
# ---------------------------------------------------------------------------


def test_recover_resets_task_to_pending_clean_slate() -> None:
    """recover() transitions DEAD_LETTERED → PENDING and zeroes all runtime state."""
    task = make_task("boom", max_retries=2, retry_count=2)
    # Manually put the task in DEAD_LETTERED without going through the worker.
    task.status = TaskStatus.RUNNING  # satisfy transition guard
    task.mark_dead_lettered("boom", started_at=datetime.now(timezone.utc))

    assert task.status == TaskStatus.DEAD_LETTERED

    task.recover()

    assert task.status == TaskStatus.PENDING
    assert task.retry_count == 0
    assert task.started_at is None
    assert task.completed_at is None
    assert task.error_message is None
    assert task.next_retry_at is None
    assert task.retry_enqueued_at is None
    # max_retries is preserved so the operator retains the original retry budget.
    assert task.max_retries == 2


# ---------------------------------------------------------------------------
# API-level unit tests (in-memory, same pattern as test_tasks_api.py)
# ---------------------------------------------------------------------------


class FakeSession:
    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def refresh(self, task: Task) -> None:
        pass

    def close(self) -> None:
        pass


class FakeQueue:
    def __init__(self) -> None:
        self.enqueued: list[UUID] = []

    def enqueue(self, task_id: UUID) -> None:
        self.enqueued.append(task_id)


class InMemoryRepo:
    def __init__(self, tasks: list[Task] | None = None) -> None:
        self.tasks: dict[UUID, Task] = {t.id: t for t in (tasks or [])}

    def create(self, task: Task) -> Task:
        task.id = task.id or uuid4()
        task.max_retries = task.max_retries or 0
        task.retry_count = task.retry_count or 0
        task.created_at = task.created_at or datetime.now(timezone.utc)
        self.tasks[task.id] = task
        return task

    def get(self, task_id: UUID) -> Task | None:
        return self.tasks.get(task_id)

    def get_for_update(self, task_id: UUID) -> Task | None:
        return self.tasks.get(task_id)

    def list(self, limit: int = 100) -> list[Task]:
        return list(self.tasks.values())[:limit]

    def list_dead_lettered(self, limit: int = 100) -> list[Task]:
        return [t for t in self.tasks.values() if t.status == TaskStatus.DEAD_LETTERED][:limit]

    def list_retry_waiting_due(self, at: datetime, limit: int = 100) -> list[Task]:
        return []

    def update(self, task: Task) -> Task:
        self.tasks[task.id] = task
        return task


def _dead_lettered_task(repo: InMemoryRepo | None = None) -> Task:
    task = Task(
        id=uuid4(),
        task_type="boom",
        status=TaskStatus.RUNNING,
        max_retries=1,
        retry_count=1,
        payload={},
        created_at=datetime.now(timezone.utc),
    )
    task.mark_dead_lettered("exhausted")
    if repo is not None:
        repo.tasks[task.id] = task
    return task


def build_client(
    tasks: list[Task] | None = None,
) -> tuple[TestClient, InMemoryRepo, FakeQueue]:
    app = create_app()
    repo = InMemoryRepo(tasks)
    queue = FakeQueue()
    session = FakeSession()

    def override_db():
        yield session

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_task_repository] = lambda: repo
    app.dependency_overrides[get_redis_queue] = lambda: queue
    return TestClient(app), repo, queue


def test_list_dead_lettered_returns_only_dead_lettered_tasks() -> None:
    """GET /tasks/dead-lettered filters to only DEAD_LETTERED tasks."""
    dl = Task(id=uuid4(), task_type="boom", status=TaskStatus.DEAD_LETTERED, max_retries=1,
              retry_count=1, payload={}, created_at=datetime.now(timezone.utc))
    ok = Task(id=uuid4(), task_type="echo", status=TaskStatus.COMPLETED, max_retries=0,
              retry_count=0, payload={}, created_at=datetime.now(timezone.utc))

    client, repo, _ = build_client([dl, ok])
    response = client.get("/tasks/dead-lettered")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["id"] == str(dl.id)
    assert body[0]["status"] == "DEAD_LETTERED"


def test_get_dead_lettered_task_by_id() -> None:
    """GET /tasks/{id} returns the full task record for a DEAD_LETTERED task."""
    dl = Task(id=uuid4(), task_type="boom", status=TaskStatus.DEAD_LETTERED, max_retries=1,
              retry_count=1, payload={}, created_at=datetime.now(timezone.utc))

    client, repo, _ = build_client([dl])
    response = client.get(f"/tasks/{dl.id}")

    assert response.status_code == 200
    assert response.json()["status"] == "DEAD_LETTERED"


def test_recover_dead_lettered_task_re_enqueues() -> None:
    """POST /tasks/{id}/recover transitions to PENDING and enqueues the task."""
    repo_store: InMemoryRepo
    client, repo_store, queue = build_client()
    task = _dead_lettered_task(repo_store)

    response = client.post(f"/tasks/{task.id}/recover")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "PENDING"
    assert body["retry_count"] == 0
    assert task.id in queue.enqueued


def test_recover_non_dead_lettered_task_returns_409() -> None:
    """POST /tasks/{id}/recover returns 409 when task is not DEAD_LETTERED."""
    failed = Task(id=uuid4(), task_type="boom", status=TaskStatus.FAILED, max_retries=0,
                  retry_count=0, payload={}, created_at=datetime.now(timezone.utc))

    client, _, _ = build_client([failed])
    response = client.post(f"/tasks/{failed.id}/recover")

    assert response.status_code == 409


def test_recover_missing_task_returns_409() -> None:
    """POST /tasks/{id}/recover returns 409 for an unknown task ID (treated as not recoverable)."""
    client, _, _ = build_client()
    response = client.post(f"/tasks/{uuid4()}/recover")

    assert response.status_code == 409


def test_duplicate_recovery_attempt_returns_409() -> None:
    """The second recovery attempt on an already-recovered task returns 409."""
    client, repo_store, queue = build_client()
    task = _dead_lettered_task(repo_store)

    first = client.post(f"/tasks/{task.id}/recover")
    assert first.status_code == 200

    # Task is now PENDING — a second recovery attempt must be rejected.
    second = client.post(f"/tasks/{task.id}/recover")
    assert second.status_code == 409


def test_recover_enqueue_failure_marks_task_failed() -> None:
    """If Redis enqueue fails during recovery, the task is marked FAILED and 503 is returned."""
    class FailingQueue:
        def enqueue(self, task_id: UUID) -> None:
            raise RuntimeError("Redis connection error")

    app = create_app()
    repo = InMemoryRepo()
    task = _dead_lettered_task(repo)
    queue = FailingQueue()
    session = FakeSession()

    def override_db():
        yield session

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_task_repository] = lambda: repo
    app.dependency_overrides[get_redis_queue] = lambda: queue

    client = TestClient(app)
    response = client.post(f"/tasks/{task.id}/recover")

    assert response.status_code == 503
    assert task.status == TaskStatus.FAILED
    assert "Task could not be enqueued after it was committed." in (task.error_message or "")
