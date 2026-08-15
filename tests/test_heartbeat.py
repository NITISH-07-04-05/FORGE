from __future__ import annotations

import time
from uuid import uuid4

import pytest
from redis import Redis

from app.core.config import settings
from app.execution.heartbeat import WorkerHeartbeat
from app.execution.registry import ExecutionRegistry
from app.execution.worker import Worker


@pytest.fixture
def redis_client() -> Redis:
    client = Redis.from_url(settings.redis_url, decode_responses=True)
    yield client
    client.close()


@pytest.fixture
def heartbeat_manager(redis_client: Redis) -> WorkerHeartbeat:
    test_prefix = f"test:worker:heartbeat:{uuid4().hex}"
    manager = WorkerHeartbeat(redis_client=redis_client, prefix=test_prefix)
    yield manager
    # Cleanup
    keys = redis_client.keys(f"{test_prefix}*")
    if keys:
        redis_client.delete(*keys)
    manager.close()


def test_heartbeat_creation(heartbeat_manager: WorkerHeartbeat) -> None:
    """A worker can publish an initial heartbeat with a TTL."""
    worker_id = f"worker-{uuid4().hex}"

    assert heartbeat_manager.is_alive(worker_id) is False

    created = heartbeat_manager.heartbeat(worker_id, ttl=10)
    assert created is True
    assert heartbeat_manager.is_alive(worker_id) is True
    assert 0 < heartbeat_manager.get_ttl(worker_id) <= 10


def test_heartbeat_renewal(heartbeat_manager: WorkerHeartbeat) -> None:
    """Subsequent heartbeats refresh the TTL for the active worker."""
    worker_id = f"worker-{uuid4().hex}"

    heartbeat_manager.heartbeat(worker_id, ttl=5)
    time.sleep(1)
    ttl_before = heartbeat_manager.get_ttl(worker_id)
    assert ttl_before <= 4

    # Renew with a longer TTL
    renewed = heartbeat_manager.heartbeat(worker_id, ttl=20)
    assert renewed is True
    ttl_after = heartbeat_manager.get_ttl(worker_id)
    assert 15 <= ttl_after <= 20
    assert heartbeat_manager.is_alive(worker_id) is True


def test_alive_detection(heartbeat_manager: WorkerHeartbeat) -> None:
    """is_alive accurately returns True for active workers and False for unknown workers."""
    alive_worker = f"worker-alive-{uuid4().hex}"
    unknown_worker = f"worker-unknown-{uuid4().hex}"

    heartbeat_manager.heartbeat(alive_worker, ttl=15)

    assert heartbeat_manager.is_alive(alive_worker) is True
    assert heartbeat_manager.is_alive(unknown_worker) is False
    assert heartbeat_manager.get_ttl(unknown_worker) == -2


def test_expiration_after_ttl(heartbeat_manager: WorkerHeartbeat) -> None:
    """A worker heartbeat expires naturally once its TTL elapses without renewal."""
    worker_id = f"worker-{uuid4().hex}"

    heartbeat_manager.heartbeat(worker_id, ttl=1)
    assert heartbeat_manager.is_alive(worker_id) is True

    # Wait for TTL expiration
    time.sleep(1.5)

    assert heartbeat_manager.is_alive(worker_id) is False
    assert heartbeat_manager.get_ttl(worker_id) == -2


def test_independent_heartbeats_for_multiple_workers(heartbeat_manager: WorkerHeartbeat) -> None:
    """Multiple workers maintain independent heartbeat lifecycles."""
    worker_1 = f"worker-1-{uuid4().hex}"
    worker_2 = f"worker-2-{uuid4().hex}"

    heartbeat_manager.heartbeat(worker_1, ttl=2)
    heartbeat_manager.heartbeat(worker_2, ttl=20)

    assert heartbeat_manager.is_alive(worker_1) is True
    assert heartbeat_manager.is_alive(worker_2) is True

    # Wait for worker_1 to expire while worker_2 remains alive
    time.sleep(2.5)

    assert heartbeat_manager.is_alive(worker_1) is False
    assert heartbeat_manager.is_alive(worker_2) is True
    assert heartbeat_manager.get_ttl(worker_2) > 0


def test_worker_publishes_heartbeat_on_process_task(heartbeat_manager: WorkerHeartbeat) -> None:
    """Worker automatically publishes a heartbeat via heartbeat_manager during task processing."""
    class StubQueue:
        def dequeue(self):
            return None

    worker = Worker(
        queue=StubQueue(),
        task_repository=None,
        registry=ExecutionRegistry({}),
        session=None,
        heartbeat_manager=heartbeat_manager,
        heartbeat_ttl_seconds=10,
    )

    assert heartbeat_manager.is_alive(worker.worker_id) is False
    worker.process_next_task()
    assert heartbeat_manager.is_alive(worker.worker_id) is True
    assert 0 < heartbeat_manager.get_ttl(worker.worker_id) <= 10
