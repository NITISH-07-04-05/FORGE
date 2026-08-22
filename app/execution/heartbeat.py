from __future__ import annotations

from typing import Union
from uuid import UUID

from redis import Redis
from redis.exceptions import RedisError

from app.core.config import settings

WorkerIdType = Union[UUID, str]


class WorkerHeartbeat:
    """Redis-backed worker heartbeat tracker using TTL expiration."""

    def __init__(
        self,
        redis_client: Redis | None = None,
        prefix: str = "forge:worker:heartbeat",
    ) -> None:
        self._redis = redis_client or Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            health_check_interval=30,
        )
        self._prefix = prefix

    def _key_for(self, worker_id: WorkerIdType) -> str:
        return f"{self._prefix}:{worker_id}"

    def heartbeat(self, worker_id: WorkerIdType, ttl: int = 15) -> bool:
        """Publish or refresh the heartbeat for the given worker with the specified TTL."""
        key = self._key_for(worker_id)
        # Sets key with TTL; subsequent calls refresh the TTL
        result = self._redis.set(key, "alive", ex=int(ttl))
        return bool(result)

    def is_alive(self, worker_id: WorkerIdType) -> bool:
        """Return True if the worker heartbeat key is present and not expired."""
        key = self._key_for(worker_id)
        return bool(self._redis.exists(key))

    def get_ttl(self, worker_id: WorkerIdType) -> int:
        """Return remaining TTL in seconds (-2 if expired/nonexistent, -1 if no TTL)."""
        key = self._key_for(worker_id)
        return int(self._redis.ttl(key))

    def list_worker_ids(self) -> list[str]:
        """Return all worker IDs whose heartbeat key currently exists."""
        pattern = f"{self._prefix}:*"
        worker_ids: list[str] = []

        for key in self._redis.scan_iter(match=pattern):
            worker_ids.append(str(key).removeprefix(f"{self._prefix}:"))

        worker_ids.sort()
        return worker_ids

    def remove(self, worker_id: WorkerIdType) -> bool:
        """Explicitly remove a worker's heartbeat (e.g. on clean worker shutdown)."""
        key = self._key_for(worker_id)
        return bool(self._redis.delete(key))

    def close(self) -> None:
        self._redis.close()


# Alias for flexible importing
WorkerHeartbeatManager = WorkerHeartbeat
