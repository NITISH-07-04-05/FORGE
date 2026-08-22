from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone
from threading import Thread
from uuid import UUID, uuid4

from redis import Redis
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from app.db.base import Base
from app.execution.lease import TaskLeaseManager
from app.execution.stale import StaleTaskDetector, StaleTaskRecoverer, StaleTaskRecoveryError
from app.models.task import Task
from app.models.task_priority import TaskPriority
from app.models.task_status import TaskStatus
from app.queue.redis_queue import RedisQueue
from app.repositories.task_repository import TaskRepository


def make_sqlite_session_factory() -> tuple[sessionmaker[Session], str]:
    fd, path = tempfile.mkstemp(suffix=".sqlite3")
    os.close(fd)
    engine = create_engine(f"sqlite+pysqlite:///{path}", connect_args={"check_same_thread": False}, poolclass=NullPool)
    Base.metadata.tables["tasks"].c.payload.server_default = None
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False), path


def make_task(status: TaskStatus, *, priority: TaskPriority = TaskPriority.NORMAL) -> Task:
    return Task(
        id=uuid4(),
        task_type="echo",
        status=status,
        priority=priority,
        max_retries=3,
        retry_count=1,
        payload={"msg": "hello"},
        created_at=datetime.now(timezone.utc),
        started_at=datetime.now(timezone.utc) if status == TaskStatus.RUNNING else None,
    )


def make_redis_client() -> Redis:
    return Redis.from_url("redis://localhost:6379/0", decode_responses=True)


def cleanup_redis(redis_client: Redis, *prefixes: str) -> None:
    for prefix in prefixes:
        keys = redis_client.keys(f"{prefix}*")
        if keys:
            redis_client.delete(*keys)
    redis_client.close()


class FailingQueue(RedisQueue):
    def enqueue(self, task_id: UUID, priority: TaskPriority = TaskPriority.NORMAL) -> None:
        raise RuntimeError("redis enqueue failed")


def test_running_task_with_active_lease_is_not_stale() -> None:
    redis_client = make_redis_client()
    lease_manager = TaskLeaseManager(redis_client=redis_client, prefix=f"test:lease:{uuid4().hex}")
    session_factory, db_path = make_sqlite_session_factory()
    session = session_factory()
    repo = TaskRepository(session)
    task = make_task(TaskStatus.RUNNING)
    repo.create(task)
    lease_manager.acquire(task.id, "worker-1", ttl=30)
    detector = StaleTaskDetector(repo, lease_manager)

    assert detector.is_stale(task) is False
    assert detector.list_candidates() == []

    session.close()
    cleanup_redis(redis_client, lease_manager._prefix)  # type: ignore[attr-defined]
    os.remove(db_path)


def test_running_task_with_missing_lease_is_detected() -> None:
    redis_client = make_redis_client()
    lease_manager = TaskLeaseManager(redis_client=redis_client, prefix=f"test:lease:{uuid4().hex}")
    session_factory, db_path = make_sqlite_session_factory()
    session = session_factory()
    repo = TaskRepository(session)
    task = make_task(TaskStatus.RUNNING)
    repo.create(task)
    detector = StaleTaskDetector(repo, lease_manager)

    assert detector.is_stale(task) is True
    assert [candidate.task_id for candidate in detector.list_candidates()] == [task.id]

    session.close()
    cleanup_redis(redis_client, lease_manager._prefix)  # type: ignore[attr-defined]
    os.remove(db_path)


def test_non_running_tasks_are_ignored() -> None:
    redis_client = make_redis_client()
    lease_manager = TaskLeaseManager(redis_client=redis_client, prefix=f"test:lease:{uuid4().hex}")
    session_factory, db_path = make_sqlite_session_factory()
    session = session_factory()
    repo = TaskRepository(session)
    pending = make_task(TaskStatus.PENDING)
    completed = make_task(TaskStatus.COMPLETED)
    repo.create(pending)
    repo.create(completed)
    detector = StaleTaskDetector(repo, lease_manager)

    assert detector.list_candidates() == []

    session.close()
    cleanup_redis(redis_client, lease_manager._prefix)  # type: ignore[attr-defined]
    os.remove(db_path)


def test_stale_task_can_be_recovered_to_pending_and_requeued() -> None:
    redis_client = make_redis_client()
    lease_prefix = f"test:lease:{uuid4().hex}"
    recovery_prefix = f"test:queue:{uuid4().hex}"
    lease_manager = TaskLeaseManager(redis_client=redis_client, prefix=lease_prefix)
    queue = RedisQueue(redis_client=redis_client, queue_name=recovery_prefix)
    session_factory, db_path = make_sqlite_session_factory()
    session = session_factory()
    repo = TaskRepository(session)
    task = make_task(TaskStatus.RUNNING, priority=TaskPriority.HIGH)
    task.retry_count = 2
    task.max_retries = 5
    repo.create(task)
    recoverer = StaleTaskRecoverer(repo, lease_manager, queue)

    assert recoverer.recover(task.id) is True
    session.refresh(task)
    assert task.status == TaskStatus.PENDING
    assert task.retry_count == 2
    assert task.max_retries == 5
    assert task.priority == TaskPriority.HIGH
    assert task.payload == {"msg": "hello"}
    assert queue.dequeue() == task.id

    session.close()
    cleanup_redis(redis_client, lease_prefix, recovery_prefix)
    os.remove(db_path)


def test_concurrent_recovery_attempts_cannot_enqueue_twice() -> None:
    redis_client = make_redis_client()
    lease_prefix = f"test:lease:{uuid4().hex}"
    recovery_prefix = f"test:queue:{uuid4().hex}"
    lease_manager = TaskLeaseManager(redis_client=redis_client, prefix=lease_prefix)
    queue = RedisQueue(redis_client=redis_client, queue_name=recovery_prefix)
    session_factory, db_path = make_sqlite_session_factory()
    session = session_factory()
    repo = TaskRepository(session)
    task = make_task(TaskStatus.RUNNING)
    repo.create(task)
    session.commit()

    session_1 = session_factory()
    session_2 = session_factory()
    repo_1 = TaskRepository(session_1)
    repo_2 = TaskRepository(session_2)
    recoverer_1 = StaleTaskRecoverer(repo_1, lease_manager, queue)
    recoverer_2 = StaleTaskRecoverer(repo_2, lease_manager, queue)

    results: list[bool] = []

    def recover_one(recoverer: StaleTaskRecoverer) -> None:
        results.append(recoverer.recover(task.id))

    thread_1 = Thread(target=recover_one, args=(recoverer_1,))
    thread_2 = Thread(target=recover_one, args=(recoverer_2,))
    thread_1.start()
    thread_2.start()
    thread_1.join()
    thread_2.join()

    assert results.count(True) == 1
    assert results.count(False) == 1
    assert queue.dequeue() == task.id
    assert queue.dequeue() is None

    session.close()
    session_1.close()
    session_2.close()
    cleanup_redis(redis_client, lease_prefix, recovery_prefix)
    os.remove(db_path)


def test_task_changed_before_recovery_is_not_modified() -> None:
    redis_client = make_redis_client()
    lease_prefix = f"test:lease:{uuid4().hex}"
    recovery_prefix = f"test:queue:{uuid4().hex}"
    lease_manager = TaskLeaseManager(redis_client=redis_client, prefix=lease_prefix)
    queue = RedisQueue(redis_client=redis_client, queue_name=recovery_prefix)
    session_factory, db_path = make_sqlite_session_factory()
    session = session_factory()
    repo = TaskRepository(session)
    task = make_task(TaskStatus.RUNNING)
    repo.create(task)
    session.commit()

    task.status = TaskStatus.COMPLETED
    session.commit()

    recoverer = StaleTaskRecoverer(repo, lease_manager, queue)
    assert recoverer.recover(task.id) is False
    session.refresh(task)
    assert task.status == TaskStatus.COMPLETED
    assert queue.dequeue() is None

    session.close()
    cleanup_redis(redis_client, lease_prefix, recovery_prefix)
    os.remove(db_path)


def test_lease_appearing_again_prevents_recovery() -> None:
    redis_client = make_redis_client()
    lease_prefix = f"test:lease:{uuid4().hex}"
    recovery_prefix = f"test:queue:{uuid4().hex}"
    lease_manager = TaskLeaseManager(redis_client=redis_client, prefix=lease_prefix)
    queue = RedisQueue(redis_client=redis_client, queue_name=recovery_prefix)
    session_factory, db_path = make_sqlite_session_factory()
    session = session_factory()
    repo = TaskRepository(session)
    task = make_task(TaskStatus.RUNNING)
    repo.create(task)
    lease_manager.acquire(task.id, "worker-2", ttl=30)

    recoverer = StaleTaskRecoverer(repo, lease_manager, queue)
    assert recoverer.recover(task.id) is False
    session.refresh(task)
    assert task.status == TaskStatus.RUNNING
    assert queue.dequeue() is None

    session.close()
    cleanup_redis(redis_client, lease_prefix, recovery_prefix)
    os.remove(db_path)


def test_recovered_task_is_queued_with_correct_priority() -> None:
    redis_client = make_redis_client()
    lease_prefix = f"test:lease:{uuid4().hex}"
    recovery_prefix = f"test:queue:{uuid4().hex}"
    lease_manager = TaskLeaseManager(redis_client=redis_client, prefix=lease_prefix)
    queue = RedisQueue(redis_client=redis_client, queue_name=recovery_prefix)
    session_factory, db_path = make_sqlite_session_factory()
    session = session_factory()
    repo = TaskRepository(session)
    task = make_task(TaskStatus.RUNNING, priority=TaskPriority.CRITICAL)
    repo.create(task)
    recoverer = StaleTaskRecoverer(repo, lease_manager, queue)

    assert recoverer.recover(task.id) is True
    assert queue.dequeue() == task.id
    assert queue.dequeue() is None

    session.close()
    cleanup_redis(redis_client, lease_prefix, recovery_prefix)
    os.remove(db_path)


def test_stale_recovery_enqueue_failure_marks_task_failed() -> None:
    redis_client = make_redis_client()
    lease_prefix = f"test:lease:{uuid4().hex}"
    recovery_prefix = f"test:queue:{uuid4().hex}"
    lease_manager = TaskLeaseManager(redis_client=redis_client, prefix=lease_prefix)
    queue = FailingQueue(redis_client=redis_client, queue_name=recovery_prefix)
    session_factory, db_path = make_sqlite_session_factory()
    session = session_factory()
    repo = TaskRepository(session)
    task = make_task(TaskStatus.RUNNING, priority=TaskPriority.NORMAL)
    repo.create(task)
    recoverer = StaleTaskRecoverer(repo, lease_manager, queue)

    try:
        recoverer.recover(task.id)
        assert False, "Expected StaleTaskRecoveryError"
    except StaleTaskRecoveryError:
        pass

    session.refresh(task)
    assert task.status == TaskStatus.FAILED
    assert "Dispatch failed during stale recovery" in (task.error_message or "")
    assert queue.dequeue() is None

    session.close()
    cleanup_redis(redis_client, lease_prefix, recovery_prefix)
    os.remove(db_path)
