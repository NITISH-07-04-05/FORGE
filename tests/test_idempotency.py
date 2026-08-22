from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from threading import Barrier, Lock, Thread
from typing import Any
from uuid import UUID
import tempfile

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from app.api.routers.tasks import get_db, get_redis_queue
from app.db.base import Base
from app.main import create_app
from app.models.task import Task
from app.models.task_priority import TaskPriority
from app.models.task_status import TaskStatus


class RecordingQueue:
    def __init__(self) -> None:
        self.enqueued: list[tuple[UUID, TaskPriority]] = []
        self._lock = Lock()

    def enqueue(self, task_id: UUID, priority: TaskPriority = TaskPriority.NORMAL) -> None:
        with self._lock:
            self.enqueued.append((task_id, priority))


def build_client() -> tuple[TestClient, RecordingQueue, sessionmaker[Session]]:
    db_fd, db_path = tempfile.mkstemp(suffix=".sqlite3")
    os.close(db_fd)
    engine = create_engine(
        f"sqlite+pysqlite:///{db_path}",
        connect_args={"check_same_thread": False},
        poolclass=NullPool,
    )
    Base.metadata.tables["tasks"].c.payload.server_default = None
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    queue = RecordingQueue()
    app = create_app()

    def override_db() -> Generator[Session, None, None]:
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_redis_queue] = lambda: queue
    return TestClient(app), queue, session_factory


def test_first_idempotent_request_creates_task() -> None:
    client, queue, _ = build_client()

    response = client.post(
        "/tasks",
        json={
            "task_type": "echo",
            "payload": {"msg": "hello"},
            "idempotency_key": "abc-123",
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["task_type"] == "echo"
    assert queue.enqueued == [(UUID(body["id"]), TaskPriority.NORMAL)]


def test_repeated_idempotent_request_returns_same_task_and_does_not_enqueue_twice() -> None:
    client, queue, session_factory = build_client()

    first = client.post(
        "/tasks",
        json={
            "task_type": "echo",
            "payload": {"msg": "hello"},
            "idempotency_key": "dup-key",
        },
    )
    second = client.post(
        "/tasks",
        json={
            "task_type": "echo",
            "payload": {"msg": "hello"},
            "idempotency_key": "dup-key",
        },
    )

    assert first.status_code == 201
    assert second.status_code == 201
    assert first.json()["id"] == second.json()["id"]
    assert queue.enqueued == [(UUID(first.json()["id"]), TaskPriority.NORMAL)]

    with session_factory() as db:
        assert db.query(Task).count() == 1


def test_no_idempotency_key_preserves_current_behavior() -> None:
    client, queue, session_factory = build_client()

    first = client.post("/tasks", json={"task_type": "echo", "payload": {"msg": "a"}})
    second = client.post("/tasks", json={"task_type": "echo", "payload": {"msg": "a"}})

    assert first.status_code == 201
    assert second.status_code == 201
    assert first.json()["id"] != second.json()["id"]
    assert len(queue.enqueued) == 2

    with session_factory() as db:
        assert db.query(Task).count() == 2


def test_same_idempotency_key_with_different_request_returns_409() -> None:
    client, queue, _ = build_client()

    first = client.post(
        "/tasks",
        json={
            "task_type": "echo",
            "payload": {"msg": "hello"},
            "idempotency_key": "same-key",
        },
    )
    second = client.post(
        "/tasks",
        json={
            "task_type": "different",
            "payload": {"msg": "hello"},
            "idempotency_key": "same-key",
        },
    )

    assert first.status_code == 201
    assert second.status_code == 409
    assert len(queue.enqueued) == 1


def test_concurrent_duplicate_submissions_create_only_one_task() -> None:
    client, queue, session_factory = build_client()
    barrier = Barrier(2)
    results: list[dict[str, Any]] = []
    errors: list[BaseException] = []
    results_lock = Lock()

    def submit() -> None:
        try:
            barrier.wait(timeout=5)
            response = client.post(
                "/tasks",
                json={
                    "task_type": "echo",
                    "payload": {"msg": "concurrent"},
                    "idempotency_key": "concurrent-key",
                },
            )
            with results_lock:
                results.append({"status": response.status_code, "body": response.json()})
        except BaseException as exc:  # pragma: no cover - surface thread failures
            with results_lock:
                errors.append(exc)

    threads = [Thread(target=submit) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not errors
    assert len(results) == 2
    assert all(result["status"] == 201 for result in results)
    assert results[0]["body"]["id"] == results[1]["body"]["id"]
    assert len(queue.enqueued) == 1

    with session_factory() as db:
        assert db.query(Task).count() == 1


def test_idempotency_works_with_delayed_tasks() -> None:
    client, queue, session_factory = build_client()
    response = client.post(
        "/tasks",
        json={
            "task_type": "echo",
            "payload": {"msg": "later"},
            "delay_seconds": 60,
            "idempotency_key": "delay-key",
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "SCHEDULED"
    assert queue.enqueued == []

    with session_factory() as db:
        task = db.query(Task).filter(Task.id == UUID(body["id"])).one()
        assert task.status == TaskStatus.SCHEDULED
        assert task.scheduled_at is not None


def test_idempotent_tasks_keep_priority_and_work_with_retry_dlq_transitions() -> None:
    task = Task(
        task_type="echo",
        status=TaskStatus.PENDING,
        priority=TaskPriority.CRITICAL,
        payload={"msg": "x"},
        max_retries=1,
        retry_count=0,
        created_at=datetime.now(timezone.utc),
        idempotency_key="retain-priority",
        request_fingerprint="fingerprint",
    )

    task.mark_running()
    task.mark_retry_waiting("temporary", next_retry_at=datetime.now(timezone.utc) + timedelta(seconds=5))
    assert task.status == TaskStatus.RETRY_WAITING
    assert task.priority == TaskPriority.CRITICAL

    task.mark_running()
    task.mark_dead_lettered("final")
    assert task.status == TaskStatus.DEAD_LETTERED
    assert task.priority == TaskPriority.CRITICAL
