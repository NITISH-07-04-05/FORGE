from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timedelta, timezone
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
        self.enqueued: list[tuple[UUID, TaskPriority]] = []

    def enqueue(self, task_id: UUID, priority: TaskPriority = TaskPriority.NORMAL) -> None:
        self.enqueued.append((task_id, priority))


class InMemoryTaskRepository:
    def __init__(self) -> None:
        self.tasks: dict[UUID, Task] = {}

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
        matching = [
            t
            for t in self.tasks.values()
            if t.status == TaskStatus.SCHEDULED and t.scheduled_at is not None and t.scheduled_at <= at
        ]
        matching.sort(key=lambda t: (t.scheduled_at or at, t.created_at))
        return matching[:limit]

    def update(self, task: Task) -> Task:
        self.tasks[task.id] = task
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


def test_delay_seconds_omitted_is_pending_and_enqueued() -> None:
    """When delay_seconds is omitted (None), task is immediately PENDING and enqueued."""
    client, repo, queue = build_client()

    response = client.post(
        "/tasks",
        json={"task_type": "echo", "payload": {"msg": "no delay"}},
    )

    assert response.status_code == 201
    body = response.json()
    task_id = UUID(body["id"])
    assert body["status"] == "PENDING"
    assert body["scheduled_at"] is None
    assert repo.tasks[task_id].status == TaskStatus.PENDING
    assert queue.enqueued == [(task_id, TaskPriority.NORMAL)]


def test_delay_seconds_zero_is_pending_and_enqueued() -> None:
    """When delay_seconds is 0, task is treated as immediately executable."""
    client, repo, queue = build_client()

    response = client.post(
        "/tasks",
        json={"task_type": "echo", "payload": {"msg": "zero delay"}, "delay_seconds": 0},
    )

    assert response.status_code == 201
    body = response.json()
    task_id = UUID(body["id"])
    assert body["status"] == "PENDING"
    assert body["scheduled_at"] is None
    assert repo.tasks[task_id].status == TaskStatus.PENDING
    assert queue.enqueued == [(task_id, TaskPriority.NORMAL)]


def test_delay_seconds_positive_is_scheduled_and_not_enqueued() -> None:
    """When delay_seconds > 0, task is SCHEDULED with computed scheduled_at and not enqueued."""
    client, repo, queue = build_client()
    before = datetime.now(timezone.utc)

    response = client.post(
        "/tasks",
        json={
            "task_type": "echo",
            "payload": {"msg": "delayed"},
            "priority": "HIGH",
            "delay_seconds": 300,
        },
    )

    after = datetime.now(timezone.utc)

    assert response.status_code == 201
    body = response.json()
    task_id = UUID(body["id"])
    assert body["status"] == "SCHEDULED"
    assert body["priority"] == "HIGH"
    assert body["scheduled_at"] is not None
    assert repo.tasks[task_id].status == TaskStatus.SCHEDULED
    assert repo.tasks[task_id].priority == TaskPriority.HIGH

    # Verify computed scheduled_at is approx now + 300 seconds
    saved_scheduled_at = repo.tasks[task_id].scheduled_at
    assert saved_scheduled_at is not None
    assert before + timedelta(seconds=300) <= saved_scheduled_at <= after + timedelta(seconds=300)

    # Must NOT be enqueued yet
    assert queue.enqueued == []


def test_delay_seconds_negative_rejected() -> None:
    """Negative delay_seconds is rejected cleanly with 422 Unprocessable Entity."""
    client, _, _ = build_client()

    response = client.post(
        "/tasks",
        json={"task_type": "echo", "payload": {"msg": "invalid delay"}, "delay_seconds": -5},
    )

    assert response.status_code == 422
    errors = response.json().get("detail", [])
    assert any("delay_seconds" in str(err) for err in errors)


def test_both_scheduled_at_and_delay_seconds_rejected() -> None:
    """Providing both scheduled_at and delay_seconds is explicitly rejected with 422 validation error."""
    client, _, _ = build_client()
    future_time = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

    response = client.post(
        "/tasks",
        json={
            "task_type": "echo",
            "payload": {"msg": "conflicting scheduling"},
            "scheduled_at": future_time,
            "delay_seconds": 60,
        },
    )

    assert response.status_code == 422
    body = response.json()
    assert "Cannot provide both 'scheduled_at' and 'delay_seconds'" in str(body)


@pytest.mark.parametrize(
    "priority_val,expected_enum",
    [
        ("LOW", TaskPriority.LOW),
        ("NORMAL", TaskPriority.NORMAL),
        ("HIGH", TaskPriority.HIGH),
        ("CRITICAL", TaskPriority.CRITICAL),
    ],
)
def test_delay_seconds_preserves_priority(priority_val: str, expected_enum: TaskPriority) -> None:
    """Delayed tasks maintain their specified priority across all levels."""
    client, repo, queue = build_client()

    response = client.post(
        "/tasks",
        json={
            "task_type": "job",
            "payload": {"data": 123},
            "priority": priority_val,
            "delay_seconds": 120,
        },
    )

    assert response.status_code == 201
    body = response.json()
    task_id = UUID(body["id"])
    assert body["status"] == "SCHEDULED"
    assert body["priority"] == priority_val
    assert repo.tasks[task_id].priority == expected_enum
    assert queue.enqueued == []


def test_delayed_task_promoted_via_promoter() -> None:
    """Delayed task becomes eligible for promotion once the calculated scheduled_at is reached."""
    client, repo, queue = build_client()
    session = FakeSession()

    response = client.post(
        "/tasks",
        json={
            "task_type": "reminder",
            "payload": {"text": "check in"},
            "priority": "CRITICAL",
            "delay_seconds": 10,
        },
    )
    assert response.status_code == 201
    task_id = UUID(response.json()["id"])
    assert queue.enqueued == []

    promoter = TaskPromoter(queue=queue, task_repository=repo, session=session)

    # Before delay expires -> not promoted
    promoted_before = promoter.promote_due_tasks(at=datetime.now(timezone.utc) + timedelta(seconds=5))
    assert promoted_before == 0
    assert repo.tasks[task_id].status == TaskStatus.SCHEDULED

    # After delay expires -> promoted to PENDING and enqueued to CRITICAL queue
    promoted_after = promoter.promote_due_tasks(at=datetime.now(timezone.utc) + timedelta(seconds=15))
    assert promoted_after == 1
    assert repo.tasks[task_id].status == TaskStatus.PENDING
    assert queue.enqueued == [(task_id, TaskPriority.CRITICAL)]


def test_delayed_task_retry_and_dlq_behavior_unaffected() -> None:
    """Verifies that delayed tasks follow normal retry and DLQ transitions once running."""
    task = Task(
        id=uuid4(),
        task_type="work",
        status=TaskStatus.SCHEDULED,
        priority=TaskPriority.HIGH,
        max_retries=1,
        retry_count=0,
        scheduled_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        created_at=datetime.now(timezone.utc),
    )

    # 1. Promote to PENDING
    task.mark_pending()
    assert task.status == TaskStatus.PENDING

    # 2. RUNNING
    task.mark_running()
    assert task.status == TaskStatus.RUNNING

    # 3. Execution failure -> RETRY_WAITING
    task.mark_retry_waiting("network timeout", next_retry_at=datetime.now(timezone.utc) + timedelta(seconds=5))
    assert task.status == TaskStatus.RETRY_WAITING
    assert task.retry_count == 1
    assert task.status != TaskStatus.SCHEDULED

    # 4. Retry run -> DLQ
    task.mark_running()
    task.mark_dead_lettered("final failure")
    assert task.status == TaskStatus.DEAD_LETTERED
