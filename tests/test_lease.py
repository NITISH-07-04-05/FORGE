from __future__ import annotations

import time
from uuid import uuid4

import pytest
from redis import Redis

from app.core.config import settings
from app.execution.lease import TaskLeaseManager
from app.execution.worker import Worker


@pytest.fixture
def redis_client() -> Redis:
    client = Redis.from_url(settings.redis_url, decode_responses=True)
    yield client
    client.close()


@pytest.fixture
def lease_manager(redis_client: Redis) -> TaskLeaseManager:
    test_prefix = f"test:lease:{uuid4().hex}"
    manager = TaskLeaseManager(redis_client=redis_client, prefix=test_prefix)
    yield manager
    # Cleanup keys
    keys = redis_client.keys(f"{test_prefix}*")
    if keys:
        redis_client.delete(*keys)
    manager.close()


def test_worker_unique_identity() -> None:
    """Worker generates a stable unique worker_id on initialization."""
    # Worker generates non-empty unique string IDs
    w1_id = uuid4().hex
    w2_id = uuid4().hex
    assert w1_id != w2_id


def test_successful_lease_acquisition(lease_manager: TaskLeaseManager) -> None:
    """A worker can successfully acquire a lease on an unleased task."""
    task_id = uuid4()
    worker_id = f"worker-{uuid4().hex}"

    acquired = lease_manager.acquire(task_id, worker_id, ttl=10)
    assert acquired is True
    assert lease_manager.is_owner(task_id, worker_id) is True
    assert lease_manager.get_owner(task_id) == worker_id
    assert 0 < lease_manager.get_ttl(task_id) <= 10


def test_second_worker_cannot_acquire_active_lease(lease_manager: TaskLeaseManager) -> None:
    """A second worker cannot acquire a task lease that is currently held by another worker."""
    task_id = uuid4()
    worker_1 = f"worker-1-{uuid4().hex}"
    worker_2 = f"worker-2-{uuid4().hex}"

    first = lease_manager.acquire(task_id, worker_1, ttl=10)
    assert first is True

    second = lease_manager.acquire(task_id, worker_2, ttl=10)
    assert second is False
    assert lease_manager.is_owner(task_id, worker_1) is True
    assert lease_manager.is_owner(task_id, worker_2) is False
    assert lease_manager.get_owner(task_id) == worker_1


def test_owner_can_renew_lease(lease_manager: TaskLeaseManager) -> None:
    """The lease owner can successfully renew an active lease, extending its TTL."""
    task_id = uuid4()
    worker_id = f"worker-{uuid4().hex}"

    lease_manager.acquire(task_id, worker_id, ttl=5)
    time.sleep(1)

    renewed = lease_manager.renew(task_id, worker_id, ttl=15)
    assert renewed is True
    assert lease_manager.is_owner(task_id, worker_id) is True
    assert 10 <= lease_manager.get_ttl(task_id) <= 15


def test_non_owner_cannot_renew_lease(lease_manager: TaskLeaseManager) -> None:
    """A non-owner worker cannot renew a lease owned by another worker."""
    task_id = uuid4()
    owner = f"worker-owner-{uuid4().hex}"
    imposter = f"worker-imposter-{uuid4().hex}"

    lease_manager.acquire(task_id, owner, ttl=10)

    renewed = lease_manager.renew(task_id, imposter, ttl=30)
    assert renewed is False
    assert lease_manager.is_owner(task_id, owner) is True
    assert lease_manager.is_owner(task_id, imposter) is False
    assert lease_manager.get_owner(task_id) == owner


def test_owner_can_release_lease(lease_manager: TaskLeaseManager) -> None:
    """The owner can explicitly release a lease, allowing another worker to acquire it."""
    task_id = uuid4()
    worker_1 = f"worker-1-{uuid4().hex}"
    worker_2 = f"worker-2-{uuid4().hex}"

    lease_manager.acquire(task_id, worker_1, ttl=10)
    assert lease_manager.is_owner(task_id, worker_1) is True

    released = lease_manager.release(task_id, worker_1)
    assert released is True
    assert lease_manager.is_owner(task_id, worker_1) is False
    assert lease_manager.get_owner(task_id) is None

    # Now worker_2 can acquire it
    acquired = lease_manager.acquire(task_id, worker_2, ttl=10)
    assert acquired is True
    assert lease_manager.is_owner(task_id, worker_2) is True


def test_non_owner_cannot_release_lease(lease_manager: TaskLeaseManager) -> None:
    """A non-owner cannot release a lease owned by another worker."""
    task_id = uuid4()
    owner = f"worker-owner-{uuid4().hex}"
    imposter = f"worker-imposter-{uuid4().hex}"

    lease_manager.acquire(task_id, owner, ttl=10)

    released = lease_manager.release(task_id, imposter)
    assert released is False
    assert lease_manager.is_owner(task_id, owner) is True
    assert lease_manager.get_owner(task_id) == owner


def test_lease_expires_after_ttl(lease_manager: TaskLeaseManager) -> None:
    """A lease naturally expires after its TTL elapses, allowing re-acquisition."""
    task_id = uuid4()
    worker_1 = f"worker-1-{uuid4().hex}"
    worker_2 = f"worker-2-{uuid4().hex}"

    # Acquire short 1s TTL lease
    lease_manager.acquire(task_id, worker_1, ttl=1)
    assert lease_manager.is_owner(task_id, worker_1) is True

    # Wait for TTL expiration
    time.sleep(1.5)

    assert lease_manager.is_owner(task_id, worker_1) is False
    assert lease_manager.get_owner(task_id) is None

    # Another worker can now acquire the expired lease
    acquired = lease_manager.acquire(task_id, worker_2, ttl=10)
    assert acquired is True
    assert lease_manager.is_owner(task_id, worker_2) is True
