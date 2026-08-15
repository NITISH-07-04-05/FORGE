from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timezone
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from app.api.routers.tasks import get_db, get_redis_queue, get_task_repository
from app.main import create_app
from app.models.task import Task
from app.models.task_priority import TaskPriority
from app.models.task_status import TaskStatus


class FakeSession:
    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def refresh(self, task: Task) -> None:
        return None

    def close(self) -> None:
        return None


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
        task.next_retry_at = task.next_retry_at if task.next_retry_at is not None else None
        task.retry_enqueued_at = task.retry_enqueued_at if task.retry_enqueued_at is not None else None
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


def test_create_task_persists_pending_task_and_enqueues() -> None:
    client, repo, queue = build_client()

    response = client.post(
        "/tasks",
        json={"task_type": "echo", "payload": {"message": "Hello FORGE"}},
    )

    assert response.status_code == 201
    body = response.json()
    task_id = UUID(body["id"])
    assert task_id in repo.tasks
    assert body["status"] == "PENDING"
    assert queue.enqueued == [task_id]


def test_get_task_returns_existing_task() -> None:
    client, repo, _ = build_client()
    task = Task(task_type="echo", payload={"message": "Hello"})
    repo.create(task)

    response = client.get(f"/tasks/{task.id}")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(task.id)
    assert body["task_type"] == "echo"
