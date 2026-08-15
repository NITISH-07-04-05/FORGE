from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timezone
from uuid import UUID, uuid4

from fastapi.testclient import TestClient
import pytest

from app.api.routers.tasks import get_db, get_redis_queue, get_task_repository
from app.main import create_app
from app.models.task import Task
from app.models.task_priority import TaskPriority
from app.models.task_status import TaskStatus


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

    def enqueue(self, task_id: UUID, priority: TaskPriority = TaskPriority.NORMAL) -> None:
        self.enqueued.append(task_id)


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


def test_task_default_priority_is_normal() -> None:
    """When creating a task without priority, it defaults to NORMAL in ORM and API response."""
    client, repo, _ = build_client()

    response = client.post(
        "/tasks",
        json={"task_type": "echo", "payload": {"msg": "default priority"}},
    )

    assert response.status_code == 201
    body = response.json()
    task_id = UUID(body["id"])
    assert body["priority"] == "NORMAL"
    assert repo.tasks[task_id].priority == TaskPriority.NORMAL


@pytest.mark.parametrize(
    "priority_val,expected_enum",
    [
        ("LOW", TaskPriority.LOW),
        ("NORMAL", TaskPriority.NORMAL),
        ("HIGH", TaskPriority.HIGH),
        ("CRITICAL", TaskPriority.CRITICAL),
    ],
)
def test_task_explicit_priority(priority_val: str, expected_enum: TaskPriority) -> None:
    """When creating a task with an explicit priority, it is saved and returned correctly."""
    client, repo, _ = build_client()

    response = client.post(
        "/tasks",
        json={
            "task_type": "echo",
            "payload": {"msg": "explicit priority"},
            "priority": priority_val,
        },
    )

    assert response.status_code == 201
    body = response.json()
    task_id = UUID(body["id"])
    assert body["priority"] == priority_val
    assert repo.tasks[task_id].priority == expected_enum


def test_task_response_serialization_includes_priority() -> None:
    """GET /tasks/{id} serializes and returns the task priority."""
    client, repo, _ = build_client()
    task = Task(
        id=uuid4(),
        task_type="echo",
        status=TaskStatus.PENDING,
        priority=TaskPriority.CRITICAL,
        payload={"msg": "critical job"},
        max_retries=0,
        retry_count=0,
        created_at=datetime.now(timezone.utc),
    )
    repo.create(task)

    response = client.get(f"/tasks/{task.id}")
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(task.id)
    assert body["priority"] == "CRITICAL"


def test_priority_preserved_through_lifecycle_and_retry() -> None:
    """Task priority remains untouched across transitions: RUNNING, RETRY_WAITING, DEAD_LETTERED, recover, and COMPLETED."""
    task = Task(
        id=uuid4(),
        task_type="process",
        status=TaskStatus.PENDING,
        priority=TaskPriority.HIGH,
        max_retries=2,
        retry_count=0,
        payload={"work": 42},
        created_at=datetime.now(timezone.utc),
    )

    # 1. PENDING -> RUNNING
    task.mark_running()
    assert task.status == TaskStatus.RUNNING
    assert task.priority == TaskPriority.HIGH

    # 2. RUNNING -> RETRY_WAITING (retry 1)
    retry_at = datetime.now(timezone.utc)
    task.mark_retry_waiting("temp error", next_retry_at=retry_at)
    assert task.status == TaskStatus.RETRY_WAITING
    assert task.priority == TaskPriority.HIGH
    assert task.retry_count == 1

    # 3. RETRY_WAITING -> RUNNING
    task.mark_running()
    assert task.status == TaskStatus.RUNNING
    assert task.priority == TaskPriority.HIGH

    # 4. RUNNING -> DEAD_LETTERED (retries exhausted)
    task.mark_dead_lettered("final failure")
    assert task.status == TaskStatus.DEAD_LETTERED
    assert task.priority == TaskPriority.HIGH

    # 5. DEAD_LETTERED -> PENDING (recovered)
    task.recover()
    assert task.status == TaskStatus.PENDING
    assert task.priority == TaskPriority.HIGH
    assert task.retry_count == 0

    # 6. PENDING -> RUNNING -> COMPLETED
    task.mark_running()
    task.mark_completed()
    assert task.status == TaskStatus.COMPLETED
    assert task.priority == TaskPriority.HIGH
