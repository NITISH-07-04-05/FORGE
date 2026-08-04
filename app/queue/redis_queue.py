from __future__ import annotations

from uuid import UUID

from redis import Redis

from app.core.config import settings


class RedisQueue:
    """Isolate queue operations so Redis can evolve without leaking into services."""

    def __init__(self) -> None:
        # Centralizing Redis configuration here keeps transport details out of business logic.
        self._redis = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            health_check_interval=30,
        )
        self._queue_name = settings.queue_name

    def enqueue(self, task_id: UUID) -> None:
        self._redis.rpush(self._queue_name, str(task_id))

    def dequeue(self) -> UUID | None:
        value = self._redis.lpop(self._queue_name)

        if value is None:
            return None

        return UUID(value)

    def queue_length(self) -> int:
        return self._redis.llen(self._queue_name)
