from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from threading import Lock
from uuid import UUID, uuid4

from fastapi.testclient import TestClient
import pytest

from app.api.routers.tasks import get_db, get_redis_queue, get_task_repository
from app.main import create_app
from app.models.task import Task
from app.models.task_priority import TaskPriority
from app.models.task_status import TaskStatus
from app.scheduling.promoter import TaskPromoter


class FakeSession:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def refresh(self, task: Task) -> None:
        pass

    def close(self) -> None:
        pass


class FakeQueue:
    def __init__(self, fail_on_enqueue: bool = False) -> None:
        self.enqueued: list[tuple[UUID, TaskPriority]] = []
        self.fail_on_enqueue = fail_on_enqueue

    def enqueue(self, task_id: UUID, priority: TaskPriority = TaskPriority.NORMAL) -> None:
        if self.fail_on_enqueue:
            raise RuntimeError("Redis connection lost")
        self.enqueued.append((task_id, priority))


class InMemoryTaskRepository:
    def __init__(self) -> None:
        self.tasks: dict[UUID, Task] = {}
        self.locked_task_ids: set[UUID] = set()
        self._lock = Lock()

    def create(self, task: Task) -> Task:
        task.id = task.id or uuid4()
        task.status = task.status or TaskStatus.PENDING
        task.priority = task.priority or TaskPriority.NORMAL
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

    def list_scheduled_due(
        self,
        at: datetime,
        limit: int = 100,
        for_update: bool = True,
        skip_locked: bool = True,
    ) -> list[Task]:
        with self._lock:
            matching: list[Task] = []
            for task in self.tasks.values():
                if (
                    task.status == TaskStatus.SCHEDULED
                    and task.scheduled_at is not None
                    and task.scheduled_at <= at
                ):
                    if skip_locked and task.id in self.locked_task_ids:
                        continue
                    matching.append(task)
                    if for_update:
                        self.locked_task_ids.add(task.id)

            matching.sort(key=lambda t: (t.scheduled_at or at, t.created_at))
            return matching[:limit]

    def update(self, task: Task) -> Task:
        self.tasks[task.id] = task
        # Releasing simulated lock once status changes away from SCHEDULED
        if task.status != TaskStatus.SCHEDULED and task.id in self.locked_task_ids:
            self.locked_task_ids.discard(task.id)
        return task


def build_client() -> tuple[TestClient, InMemoryTaskRepository, FakeQueue]:
    app = create_app()
    repo = InMemoryTaskRepository()
    queue = FakeQueue()
    session = FakeSession()

    def override_db() -> Generator[FakeSession, None, None]:
        yield session

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_task_repository] = lambda: repo
    app.dependency_overrides[get_redis_queue] = lambda: queue
    return TestClient(app), repo, queue


# ---------------------------------------------------------------------------
# Step 1 API & Model Tests
# ---------------------------------------------------------------------------


def test_create_task_without_scheduled_at_is_pending_and_enqueued() -> None:
    """When scheduled_at is omitted, task starts as PENDING and is enqueued immediately."""
    client, repo, queue = build_client()

    response = client.post(
        "/tasks",
        json={"task_type": "echo", "payload": {"msg": "immediate"}},
    )

    assert response.status_code == 201
    body = response.json()
    task_id = UUID(body["id"])
    assert body["status"] == "PENDING"
    assert body["scheduled_at"] is None
    assert repo.tasks[task_id].status == TaskStatus.PENDING
    assert repo.tasks[task_id].scheduled_at is None
    assert queue.enqueued == [(task_id, TaskPriority.NORMAL)]


def test_create_task_with_future_scheduled_at_is_scheduled_and_not_enqueued() -> None:
    """When scheduled_at is in the future, task starts as SCHEDULED and is NOT placed in the ready queue."""
    client, repo, queue = build_client()
    future_time = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()

    response = client.post(
        "/tasks",
        json={
            "task_type": "echo",
            "payload": {"msg": "run later"},
            "priority": "HIGH",
            "scheduled_at": future_time,
        },
    )

    assert response.status_code == 201
    body = response.json()
    task_id = UUID(body["id"])
    assert body["status"] == "SCHEDULED"
    assert body["priority"] == "HIGH"
    assert body["scheduled_at"] is not None
    assert repo.tasks[task_id].status == TaskStatus.SCHEDULED
    assert repo.tasks[task_id].priority == TaskPriority.HIGH
    assert repo.tasks[task_id].scheduled_at is not None
    # Must NOT be placed in the ready queue
    assert queue.enqueued == []


def test_create_task_with_past_scheduled_at_is_pending_and_enqueued() -> None:
    """When scheduled_at is in the past, task starts as PENDING and is enqueued immediately."""
    client, repo, queue = build_client()
    past_time = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()

    response = client.post(
        "/tasks",
        json={
            "task_type": "echo",
            "payload": {"msg": "already due"},
            "priority": "CRITICAL",
            "scheduled_at": past_time,
        },
    )

    assert response.status_code == 201
    body = response.json()
    task_id = UUID(body["id"])
    assert body["status"] == "PENDING"
    assert body["priority"] == "CRITICAL"
    assert repo.tasks[task_id].status == TaskStatus.PENDING
    assert repo.tasks[task_id].priority == TaskPriority.CRITICAL
    assert repo.tasks[task_id].scheduled_at is not None
    assert queue.enqueued == [(task_id, TaskPriority.CRITICAL)]


def test_create_task_with_now_scheduled_at_is_pending_and_enqueued() -> None:
    """When scheduled_at is equal to current time, task starts as PENDING and is enqueued."""
    client, repo, queue = build_client()
    now_time = datetime.now(timezone.utc).isoformat()

    response = client.post(
        "/tasks",
        json={
            "task_type": "echo",
            "payload": {"msg": "run now"},
            "scheduled_at": now_time,
        },
    )

    assert response.status_code == 201
    body = response.json()
    task_id = UUID(body["id"])
    assert body["status"] == "PENDING"
    assert repo.tasks[task_id].status == TaskStatus.PENDING
    assert len(queue.enqueued) == 1
    assert queue.enqueued[0][0] == task_id


def test_get_task_returns_scheduled_at_field() -> None:
    """GET /tasks/{id} returns scheduled_at value for both scheduled and immediate tasks."""
    client, repo, _ = build_client()
    future_dt = datetime.now(timezone.utc) + timedelta(days=1)
    task = Task(
        id=uuid4(),
        task_type="email",
        status=TaskStatus.SCHEDULED,
        priority=TaskPriority.HIGH,
        payload={"to": "user@example.com"},
        max_retries=3,
        retry_count=0,
        scheduled_at=future_dt,
        created_at=datetime.now(timezone.utc),
    )
    repo.create(task)

    response = client.get(f"/tasks/{task.id}")
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(task.id)
    assert body["status"] == "SCHEDULED"
    assert body["priority"] == "HIGH"
    assert body["scheduled_at"] is not None


@pytest.mark.parametrize(
    "priority_val,expected_enum",
    [
        ("LOW", TaskPriority.LOW),
        ("NORMAL", TaskPriority.NORMAL),
        ("HIGH", TaskPriority.HIGH),
        ("CRITICAL", TaskPriority.CRITICAL),
    ],
)
def test_priority_preserved_for_scheduled_tasks(priority_val: str, expected_enum: TaskPriority) -> None:
    """Scheduled tasks retain their assigned priority across all priority levels."""
    client, repo, queue = build_client()
    future_time = (datetime.now(timezone.utc) + timedelta(hours=5)).isoformat()

    response = client.post(
        "/tasks",
        json={
            "task_type": "batch",
            "payload": {"items": [1, 2, 3]},
            "priority": priority_val,
            "scheduled_at": future_time,
        },
    )

    assert response.status_code == 201
    body = response.json()
    task_id = UUID(body["id"])
    assert body["status"] == "SCHEDULED"
    assert body["priority"] == priority_val
    assert repo.tasks[task_id].priority == expected_enum
    assert queue.enqueued == []


# ---------------------------------------------------------------------------
# Step 2 Scheduler/Promoter Tests
# ---------------------------------------------------------------------------


def test_promoter_future_task_is_not_promoted() -> None:
    """Requirement a: Future SCHEDULED tasks are not promoted."""
    repo = InMemoryTaskRepository()
    queue = FakeQueue()
    session = FakeSession()
    promoter = TaskPromoter(queue=queue, task_repository=repo, session=session)

    future_time = datetime.now(timezone.utc) + timedelta(hours=1)
    task = Task(
        id=uuid4(),
        task_type="report",
        status=TaskStatus.SCHEDULED,
        priority=TaskPriority.NORMAL,
        scheduled_at=future_time,
        created_at=datetime.now(timezone.utc),
    )
    repo.create(task)

    promoted = promoter.promote_due_tasks(at=datetime.now(timezone.utc))

    assert promoted == 0
    assert repo.tasks[task.id].status == TaskStatus.SCHEDULED
    assert queue.enqueued == []


def test_promoter_due_task_becomes_pending() -> None:
    """Requirement b: Due SCHEDULED task becomes PENDING."""
    repo = InMemoryTaskRepository()
    queue = FakeQueue()
    session = FakeSession()
    promoter = TaskPromoter(queue=queue, task_repository=repo, session=session)

    past_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    task = Task(
        id=uuid4(),
        task_type="report",
        status=TaskStatus.SCHEDULED,
        priority=TaskPriority.NORMAL,
        scheduled_at=past_time,
        created_at=datetime.now(timezone.utc),
    )
    repo.create(task)

    promoted = promoter.promote_due_tasks(at=datetime.now(timezone.utc))

    assert promoted == 1
    assert repo.tasks[task.id].status == TaskStatus.PENDING
    assert session.committed is True


@pytest.mark.parametrize(
    "priority_enum",
    [
        TaskPriority.LOW,
        TaskPriority.NORMAL,
        TaskPriority.HIGH,
        TaskPriority.CRITICAL,
    ],
)
def test_promoter_due_task_placed_in_correct_priority_queue(priority_enum: TaskPriority) -> None:
    """Requirement c: Due task is placed into the correct priority queue, preserving priority."""
    repo = InMemoryTaskRepository()
    queue = FakeQueue()
    session = FakeSession()
    promoter = TaskPromoter(queue=queue, task_repository=repo, session=session)

    due_time = datetime.now(timezone.utc) - timedelta(seconds=1)
    task = Task(
        id=uuid4(),
        task_type="work",
        status=TaskStatus.SCHEDULED,
        priority=priority_enum,
        scheduled_at=due_time,
        created_at=datetime.now(timezone.utc),
    )
    repo.create(task)

    promoted = promoter.promote_due_tasks(at=datetime.now(timezone.utc))

    assert promoted == 1
    assert repo.tasks[task.id].status == TaskStatus.PENDING
    assert repo.tasks[task.id].priority == priority_enum
    assert queue.enqueued == [(task.id, priority_enum)]


def test_promoter_task_cannot_be_promoted_twice() -> None:
    """Requirement d: A task cannot be promoted twice on subsequent promoter runs."""
    repo = InMemoryTaskRepository()
    queue = FakeQueue()
    session = FakeSession()
    promoter = TaskPromoter(queue=queue, task_repository=repo, session=session)

    due_time = datetime.now(timezone.utc) - timedelta(minutes=1)
    task = Task(
        id=uuid4(),
        task_type="invoice",
        status=TaskStatus.SCHEDULED,
        priority=TaskPriority.HIGH,
        scheduled_at=due_time,
        created_at=datetime.now(timezone.utc),
    )
    repo.create(task)

    # First run
    promoted_first = promoter.promote_due_tasks(at=datetime.now(timezone.utc))
    assert promoted_first == 1
    assert queue.enqueued == [(task.id, TaskPriority.HIGH)]

    # Second run immediately after
    promoted_second = promoter.promote_due_tasks(at=datetime.now(timezone.utc))
    assert promoted_second == 0
    assert len(queue.enqueued) == 1


def test_promoter_concurrent_attempts_cannot_enqueue_same_task_twice() -> None:
    """Requirement e: Concurrent scheduler attempts with skip_locked cannot enqueue the same task twice."""
    repo = InMemoryTaskRepository()
    queue = FakeQueue()
    session1 = FakeSession()
    session2 = FakeSession()
    promoter1 = TaskPromoter(queue=queue, task_repository=repo, session=session1)
    promoter2 = TaskPromoter(queue=queue, task_repository=repo, session=session2)

    due_time = datetime.now(timezone.utc) - timedelta(minutes=2)
    task = Task(
        id=uuid4(),
        task_type="sync",
        status=TaskStatus.SCHEDULED,
        priority=TaskPriority.CRITICAL,
        scheduled_at=due_time,
        created_at=datetime.now(timezone.utc),
    )
    repo.create(task)

    # Simulate promoter1 querying and holding row lock
    now = datetime.now(timezone.utc)
    due_for_1 = repo.list_scheduled_due(at=now, for_update=True, skip_locked=True)
    assert len(due_for_1) == 1

    # Concurrent promoter2 tries to query while promoter1 holds the lock
    due_for_2 = repo.list_scheduled_due(at=now, for_update=True, skip_locked=True)
    # With skip_locked, promoter2 skips the locked row
    assert len(due_for_2) == 0

    # Promoter1 completes promotion
    for t in due_for_1:
        t.mark_pending()
        repo.update(t)
        queue.enqueue(t.id, priority=t.priority)

    # Promoter2 attempts promotion now
    promoted_by_2 = promoter2.promote_due_tasks(at=now)
    assert promoted_by_2 == 0

    assert queue.enqueued == [(task.id, TaskPriority.CRITICAL)]
    assert repo.tasks[task.id].status == TaskStatus.PENDING


def test_promoter_redis_enqueue_failure_does_not_silently_strand_task() -> None:
    """Requirement f: Redis enqueue failure marks the task as FAILED with error context."""
    repo = InMemoryTaskRepository()
    queue = FakeQueue(fail_on_enqueue=True)
    session = FakeSession()
    promoter = TaskPromoter(queue=queue, task_repository=repo, session=session)

    due_time = datetime.now(timezone.utc) - timedelta(seconds=30)
    task = Task(
        id=uuid4(),
        task_type="job",
        status=TaskStatus.SCHEDULED,
        priority=TaskPriority.NORMAL,
        scheduled_at=due_time,
        created_at=datetime.now(timezone.utc),
    )
    repo.create(task)

    promoted = promoter.promote_due_tasks(at=datetime.now(timezone.utc))

    # Promoted count should be 0 since enqueue failed
    assert promoted == 0
    # Task must NOT remain stranded in PENDING without being in Redis queue; it becomes FAILED
    assert repo.tasks[task.id].status == TaskStatus.FAILED
    assert "Dispatch failed during promotion" in (repo.tasks[task.id].error_message or "")


def test_retry_and_dlq_behavior_unaffected_by_promoter() -> None:
    """Requirements g & h: RETRY_WAITING and DEAD_LETTERED tasks are completely ignored by the promoter."""
    repo = InMemoryTaskRepository()
    queue = FakeQueue()
    session = FakeSession()
    promoter = TaskPromoter(queue=queue, task_repository=repo, session=session)

    past_time = datetime.now(timezone.utc) - timedelta(minutes=10)

    # RETRY_WAITING task
    retry_task = Task(
        id=uuid4(),
        task_type="retryable",
        status=TaskStatus.RETRY_WAITING,
        priority=TaskPriority.HIGH,
        next_retry_at=past_time,
        scheduled_at=past_time,
        created_at=datetime.now(timezone.utc),
    )
    repo.create(retry_task)

    # DEAD_LETTERED task
    dlq_task = Task(
        id=uuid4(),
        task_type="dead_lettered",
        status=TaskStatus.DEAD_LETTERED,
        priority=TaskPriority.HIGH,
        scheduled_at=past_time,
        created_at=datetime.now(timezone.utc),
    )
    repo.create(dlq_task)

    promoted = promoter.promote_due_tasks(at=datetime.now(timezone.utc))

    assert promoted == 0
    assert repo.tasks[retry_task.id].status == TaskStatus.RETRY_WAITING
    assert repo.tasks[dlq_task.id].status == TaskStatus.DEAD_LETTERED
    assert queue.enqueued == []
