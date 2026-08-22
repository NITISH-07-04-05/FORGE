from __future__ import annotations

from typing import Union
from uuid import UUID

from redis import Redis

from app.core.config import settings

TaskIdType = Union[UUID, str]
WorkerIdType = Union[UUID, str]


class TaskLeaseManager:
    """Redis-backed execution lease abstraction for task ownership."""

    def __init__(
        self,
        redis_client: Redis | None = None,
        prefix: str = "forge:lease",
    ) -> None:
        self._redis = redis_client or Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            health_check_interval=30,
        )
        self._prefix = prefix

    def _key_for(self, task_id: TaskIdType) -> str:
        return f"{self._prefix}:{task_id}"

    def acquire(self, task_id: TaskIdType, worker_id: WorkerIdType, ttl: int) -> bool:
        """Atomically acquire an execution lease on a task if not currently leased.

        Returns True if the lease was acquired, False if already leased by another worker.
        """
        key = self._key_for(task_id)
        result = self._redis.set(key, str(worker_id), ex=int(ttl), nx=True)
        return bool(result)

    def renew(self, task_id: TaskIdType, worker_id: WorkerIdType, ttl: int) -> bool:
        """Atomically renew an active lease only if the caller is the current owner.

        Returns True if renewed, False if not owner or lease has expired.
        """
        key = self._key_for(task_id)
        script = """
        local key = KEYS[1]
        local worker_id = ARGV[1]
        local ttl = tonumber(ARGV[2])

        if redis.call('GET', key) == worker_id then
            return redis.call('EXPIRE', key, ttl)
        else
            return 0
        end
        """
        result = self._redis.eval(script, 1, key, str(worker_id), int(ttl))
        return bool(result)

    def release(self, task_id: TaskIdType, worker_id: WorkerIdType) -> bool:
        """Atomically release an active lease only if the caller is the current owner.

        Returns True if released, False if not owner or lease has expired.
        """
        key = self._key_for(task_id)
        script = """
        local key = KEYS[1]
        local worker_id = ARGV[1]

        if redis.call('GET', key) == worker_id then
            return redis.call('DEL', key)
        else
            return 0
        end
        """
        result = self._redis.eval(script, 1, key, str(worker_id))
        return bool(result)

    def is_owner(self, task_id: TaskIdType, worker_id: WorkerIdType) -> bool:
        """Check whether the given worker currently owns the task lease."""
        key = self._key_for(task_id)
        current_owner = self._redis.get(key)
        return current_owner == str(worker_id)

    def get_owner(self, task_id: TaskIdType) -> str | None:
        """Return the current lease owner worker_id or None if unleased."""
        key = self._key_for(task_id)
        return self._redis.get(key)

    def get_ttl(self, task_id: TaskIdType) -> int:
        """Return remaining TTL in seconds for the task lease (-2 if expired/nonexistent, -1 if no TTL)."""
        key = self._key_for(task_id)
        return int(self._redis.ttl(key))

    def acquire_claim(self, claim_key: str, ttl: int) -> bool:
        """Acquire a short-lived Redis claim for recovery coordination."""
        return bool(self._redis.set(claim_key, "claimed", ex=int(ttl), nx=True))

    def release_claim(self, claim_key: str) -> bool:
        """Release a short-lived Redis claim if still present."""
        return bool(self._redis.delete(claim_key))

    def close(self) -> None:
        self._redis.close()


# Alias for flexible importing
TaskLease = TaskLeaseManager
