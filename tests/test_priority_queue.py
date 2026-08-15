from __future__ import annotations

import concurrent.futures
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from redis import Redis

from app.core.config import settings
from app.models.task_priority import TaskPriority
from app.queue.redis_queue import RedisQueue


@pytest.fixture
def redis_client() -> Redis:
    client = Redis.from_url(settings.redis_url, decode_responses=True)
    yield client
    client.close()


@pytest.fixture
def clean_queue(redis_client: Redis) -> RedisQueue:
    test_queue_name = f"test:queue:{uuid4().hex}"
    queue = RedisQueue(
        dequeue_timeout=1,
        redis_client=redis_client,
        queue_name=test_queue_name,
    )
    yield queue
    # Cleanup all keys for this test queue
    keys = redis_client.keys(f"{test_queue_name}*")
    if keys:
        redis_client.delete(*keys)
    queue.close()


def test_priority_ordering(clean_queue: RedisQueue) -> None:
    """Tasks are dequeued in strict priority order: CRITICAL > HIGH > NORMAL > LOW regardless of arrival order."""
    low_id = uuid4()
    normal_id = uuid4()
    high_id = uuid4()
    critical_id = uuid4()

    # Enqueue in reverse priority order (LOW first, CRITICAL last)
    clean_queue.enqueue(low_id, priority=TaskPriority.LOW)
    clean_queue.enqueue(normal_id, priority=TaskPriority.NORMAL)
    clean_queue.enqueue(high_id, priority=TaskPriority.HIGH)
    clean_queue.enqueue(critical_id, priority=TaskPriority.CRITICAL)

    # Dequeue order must be strictly: CRITICAL, HIGH, NORMAL, LOW
    assert clean_queue.dequeue() == critical_id
    assert clean_queue.dequeue() == high_id
    assert clean_queue.dequeue() == normal_id
    assert clean_queue.dequeue() == low_id
    assert clean_queue.dequeue() is None


def test_fifo_within_equal_priority(clean_queue: RedisQueue) -> None:
    """Tasks with equal priority preserve strict FIFO arrival order."""
    high_1 = uuid4()
    high_2 = uuid4()
    high_3 = uuid4()

    normal_1 = uuid4()
    normal_2 = uuid4()

    # Enqueue multiple HIGH and NORMAL tasks
    clean_queue.enqueue(normal_1, priority=TaskPriority.NORMAL)
    clean_queue.enqueue(high_1, priority=TaskPriority.HIGH)
    clean_queue.enqueue(normal_2, priority=TaskPriority.NORMAL)
    clean_queue.enqueue(high_2, priority=TaskPriority.HIGH)
    clean_queue.enqueue(high_3, priority=TaskPriority.HIGH)

    # All HIGHs first in FIFO order: high_1, high_2, high_3
    assert clean_queue.dequeue() == high_1
    assert clean_queue.dequeue() == high_2
    assert clean_queue.dequeue() == high_3

    # Then all NORMALs in FIFO order: normal_1, normal_2
    assert clean_queue.dequeue() == normal_1
    assert clean_queue.dequeue() == normal_2
    assert clean_queue.dequeue() is None


def test_empty_queue_returns_none(clean_queue: RedisQueue) -> None:
    """Dequeueing from an empty queue cleanly returns None on timeout."""
    assert clean_queue.dequeue() is None
    assert clean_queue.queue_length() == 0


def test_multiple_workers_consuming_safely(redis_client: Redis) -> None:
    """Multiple concurrent workers consuming from the priority queue receive distinct tasks without duplicates."""
    test_queue_name = f"test:concurrent:{uuid4().hex}"
    task_count = 40
    priorities = [TaskPriority.LOW, TaskPriority.NORMAL, TaskPriority.HIGH, TaskPriority.CRITICAL]

    # Enqueue tasks of mixed priorities
    enqueue_queue = RedisQueue(dequeue_timeout=1, redis_client=redis_client, queue_name=test_queue_name)
    expected_ids = set()
    for i in range(task_count):
        tid = uuid4()
        expected_ids.add(tid)
        enqueue_queue.enqueue(tid, priority=priorities[i % len(priorities)])

    consumed_ids: list[UUID] = []

    def worker_consume() -> list[UUID]:
        worker_queue = RedisQueue(dequeue_timeout=1, queue_name=test_queue_name)
        dequeued: list[UUID] = []
        while True:
            item = worker_queue.dequeue()
            if item is None:
                break
            dequeued.append(item)
        worker_queue.close()
        return dequeued

    # Run 4 concurrent worker threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(worker_consume) for _ in range(4)]
        for f in concurrent.futures.as_completed(futures):
            consumed_ids.extend(f.result())

    # Every task must be consumed exactly once
    assert len(consumed_ids) == task_count
    assert set(consumed_ids) == expected_ids

    # Cleanup
    keys = redis_client.keys(f"{test_queue_name}*")
    if keys:
        redis_client.delete(*keys)
    enqueue_queue.close()


def test_retry_re_enqueue_preserving_priority(clean_queue: RedisQueue) -> None:
    """Delayed retry re-enqueue preserves the specified priority when promoted."""
    crit_task = uuid4()
    low_task = uuid4()

    # Enqueue critical delayed retry due in the past and low ready task
    due_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    clean_queue.enqueue_delayed(crit_task, run_at=due_at, priority=TaskPriority.CRITICAL)
    clean_queue.enqueue(low_task, priority=TaskPriority.LOW)

    # When dequeue is called, delayed critical task is promoted to CRITICAL queue and dequeued before LOW
    first = clean_queue.dequeue()
    assert first == crit_task

    second = clean_queue.dequeue()
    assert second == low_task
