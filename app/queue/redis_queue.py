from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from redis import Redis

from app.core.config import settings


class RedisQueue:
    """Isolate queue operations so Redis can evolve without leaking into services."""

    def __init__(
        self,
        dequeue_timeout: int = 1,
        redis_client: Redis | None = None,
        queue_name: str | None = None,
    ) -> None:
        # Centralizing Redis configuration here keeps transport details out of business logic.
        self._redis = redis_client or Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            health_check_interval=30,
        )
        self._queue_name = queue_name or settings.queue_name
        self._delayed_queue_name = f"{self._queue_name}:delayed"
        # A finite blocking timeout lets workers check for shutdown without polling Redis directly.
        self._dequeue_timeout = dequeue_timeout

    def enqueue(self, task_id: UUID) -> None:
        self._redis.rpush(self._queue_name, str(task_id))

    def enqueue_delayed(self, task_id: UUID, run_at: datetime) -> None:
        self._redis.zadd(self._delayed_queue_name, {str(task_id): run_at.timestamp()})

    def dequeue(self) -> UUID | None:
        self._promote_due_delayed_tasks()
        result = self._redis.blpop(self._queue_name, timeout=self._dequeue_timeout)

        if result is None:
            return None

        _, value = result
        return UUID(value)

    def queue_length(self) -> int:
        return self._redis.llen(self._queue_name)

    def delayed_queue_length(self) -> int:
        return self._redis.zcard(self._delayed_queue_name)

    def close(self) -> None:
        self._redis.close()

    def _promote_due_delayed_tasks(self, limit: int = 100) -> int:
        now = datetime.now(timezone.utc).timestamp()
        script = """
        local delayed_queue = KEYS[1]
        local ready_queue = KEYS[2]
        local now = tonumber(ARGV[1])
        local limit = tonumber(ARGV[2])
        local task_ids = redis.call('ZRANGEBYSCORE', delayed_queue, '-inf', now, 'LIMIT', 0, limit)

        for _, task_id in ipairs(task_ids) do
            redis.call('ZREM', delayed_queue, task_id)
            redis.call('RPUSH', ready_queue, task_id)
        end

        return #task_ids
        """
        promoted = self._redis.eval(script, 2, self._delayed_queue_name, self._queue_name, now, limit)
        return int(promoted)
