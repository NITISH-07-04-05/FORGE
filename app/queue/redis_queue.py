from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from redis import Redis

from app.core.config import settings
from app.models.task_priority import TaskPriority

PRIORITY_ORDER: tuple[TaskPriority, ...] = (
    TaskPriority.CRITICAL,
    TaskPriority.HIGH,
    TaskPriority.NORMAL,
    TaskPriority.LOW,
)


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
        # Ordered list of queue keys for strict priority blpop: CRITICAL -> HIGH -> NORMAL -> LOW
        self._priority_queue_names = [
            f"{self._queue_name}:{priority.value.lower()}" for priority in PRIORITY_ORDER
        ]
        # A finite blocking timeout lets workers check for shutdown without polling Redis directly.
        self._dequeue_timeout = dequeue_timeout

    def _queue_name_for(self, priority: TaskPriority = TaskPriority.NORMAL) -> str:
        return f"{self._queue_name}:{priority.value.lower()}"

    def enqueue(self, task_id: UUID, priority: TaskPriority = TaskPriority.NORMAL) -> None:
        queue_key = self._queue_name_for(priority)
        self._redis.rpush(queue_key, str(task_id))

    def enqueue_delayed(
        self,
        task_id: UUID,
        run_at: datetime,
        priority: TaskPriority = TaskPriority.NORMAL,
    ) -> None:
        # Delayed entries encode priority as a suffix in the member string (e.g. "<task_id>:<priority>")
        member = f"{task_id}:{priority.value}"
        self._redis.zadd(self._delayed_queue_name, {member: run_at.timestamp()})

    def dequeue(self) -> UUID | None:
        self._promote_due_delayed_tasks()
        # blpop inspects keys in the exact order passed: CRITICAL, then HIGH, then NORMAL, then LOW.
        # Within each list, RPUSH + BLPOP guarantees strict FIFO ordering.
        result = self._redis.blpop(self._priority_queue_names, timeout=self._dequeue_timeout)

        if result is None:
            return None

        _, value = result
        return UUID(value)

    def queue_length(self, priority: TaskPriority | None = None) -> int:
        if priority is not None:
            return self._redis.llen(self._queue_name_for(priority))
        return sum(self._redis.llen(key) for key in self._priority_queue_names)

    def delayed_queue_length(self) -> int:
        return self._redis.zcard(self._delayed_queue_name)

    def close(self) -> None:
        self._redis.close()

    def _promote_due_delayed_tasks(self, limit: int = 100) -> int:
        now = datetime.now(timezone.utc).timestamp()
        script = """
        local delayed_queue = KEYS[1]
        local base_queue = ARGV[1]
        local now = tonumber(ARGV[2])
        local limit = tonumber(ARGV[3])
        local entries = redis.call('ZRANGEBYSCORE', delayed_queue, '-inf', now, 'LIMIT', 0, limit)

        for _, entry in ipairs(entries) do
            redis.call('ZREM', delayed_queue, entry)
            -- Split member into task_id and priority (formatted as "UUID:PRIORITY")
            local sep = string.find(entry, ":")
            local task_id
            local priority
            if sep then
                task_id = string.sub(entry, 1, sep - 1)
                priority = string.lower(string.sub(entry, sep + 1))
            else
                task_id = entry
                priority = "normal"
            end
            local target_queue = base_queue .. ":" .. priority
            redis.call('RPUSH', target_queue, task_id)
        end

        return #entries
        """
        promoted = self._redis.eval(
            script,
            1,
            self._delayed_queue_name,
            self._queue_name,
            now,
            limit,
        )
        return int(promoted)
