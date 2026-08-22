from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from redis import Redis

from app.core.config import settings
from app.execution.heartbeat import WorkerHeartbeat
from app.execution.lease import TaskLeaseManager
from app.execution.registry import ExecutionRegistry, TaskHandler
from app.execution.stale import StaleTaskRecoverer
from app.execution.worker import Worker
from app.execution.worker_registry import WorkerRegistry
from app.metrics.registry import ForgeMetrics, InMemoryMetricsRecorder
from app.models.task import Task
from app.models.task_priority import TaskPriority
from app.models.task_status import TaskStatus
from app.queue.redis_queue import RedisQueue


class FakeSession:
    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


class InMemoryRepo:
    def __init__(self, tasks: list[Task] | None = None) -> None:
        self.tasks = {task.id: task for task in tasks or []}
        self.session = FakeSession()

    def get(self, task_id: UUID) -> Task | None:
        return self.tasks.get(task_id)

    def get_for_update(self, task_id: UUID) -> Task | None:
        return self.tasks.get(task_id)

    def update(self, task: Task) -> Task:
        self.tasks[task.id] = task
        return task

    def list_running(self, limit: int = 100) -> list[Task]:
        return [task for task in self.tasks.values() if task.status == TaskStatus.RUNNING][:limit]


class EchoHandler(TaskHandler):
    def execute(self, payload: dict[str, object]) -> None:
        return None


class FailingHandler(TaskHandler):
    def execute(self, payload: dict[str, object]) -> None:
        raise RuntimeError("boom")


class FailingMetricsRecorder(InMemoryMetricsRecorder):
    def increment(self, name: str, amount: int = 1) -> None:
        raise RuntimeError("metrics down")

    def observe(self, name: str, value: float) -> None:
        raise RuntimeError("metrics down")


def make_task(
    task_type: str,
    *,
    status: TaskStatus = TaskStatus.PENDING,
    priority: TaskPriority = TaskPriority.NORMAL,
    max_retries: int = 0,
    retry_count: int = 0,
    next_retry_at: datetime | None = None,
    started_at: datetime | None = None,
) -> Task:
    return Task(
        id=uuid4(),
        task_type=task_type,
        status=status,
        priority=priority,
        max_retries=max_retries,
        retry_count=retry_count,
        next_retry_at=next_retry_at,
        payload={"msg": "hello"},
        created_at=datetime.now(timezone.utc),
        started_at=started_at,
    )


def make_metrics() -> tuple[ForgeMetrics, InMemoryMetricsRecorder]:
    recorder = InMemoryMetricsRecorder()
    return ForgeMetrics(recorder=recorder), recorder


def cleanup_redis(prefix: str) -> None:
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    keys = redis_client.keys(f"{prefix}*")
    if keys:
        redis_client.delete(*keys)
    redis_client.close()


def test_completed_task_increments_completion_counter() -> None:
    metrics, recorder = make_metrics()
    task = make_task("echo")
    worker = Worker(
        queue=type("Q", (), {"dequeue": lambda self: task.id})(),
        task_repository=InMemoryRepo([task]),
        registry=ExecutionRegistry({"echo": EchoHandler()}),
        session=FakeSession(),
        metrics=metrics,
    )

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.COMPLETED
    assert recorder.get_counter("tasks_completed") == 1


def test_execution_duration_is_recorded() -> None:
    metrics, recorder = make_metrics()
    task = make_task("echo")
    worker = Worker(
        queue=type("Q", (), {"dequeue": lambda self: task.id})(),
        task_repository=InMemoryRepo([task]),
        registry=ExecutionRegistry({"echo": EchoHandler()}),
        session=FakeSession(),
        metrics=metrics,
    )

    assert worker.process_next_task() is True
    durations = recorder.get_histogram("task_execution_duration_seconds")
    assert len(durations) == 1
    assert durations[0] >= 0
    assert len(recorder.get_histogram("task_execution_duration_seconds")) == 1


def test_failed_task_increments_failure_counter() -> None:
    metrics, recorder = make_metrics()
    task = make_task("boom")
    worker = Worker(
        queue=type("Q", (), {"dequeue": lambda self: task.id})(),
        task_repository=InMemoryRepo([task]),
        registry=ExecutionRegistry({"boom": FailingHandler()}),
        session=FakeSession(),
        metrics=metrics,
    )

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.FAILED
    assert recorder.get_counter("tasks_failed") == 1


def test_retry_increments_retry_counter() -> None:
    metrics, recorder = make_metrics()
    task = make_task("boom", max_retries=1)
    worker = Worker(
        queue=type("Q", (), {"dequeue": lambda self: task.id})(),
        task_repository=InMemoryRepo([task]),
        registry=ExecutionRegistry({"boom": FailingHandler()}),
        session=FakeSession(),
        metrics=metrics,
    )

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.RETRY_WAITING
    assert recorder.get_counter("tasks_retried") == 1


def test_dlq_increments_dead_letter_counter() -> None:
    metrics, recorder = make_metrics()
    task = make_task("boom", max_retries=1, retry_count=1)
    worker = Worker(
        queue=type("Q", (), {"dequeue": lambda self: task.id})(),
        task_repository=InMemoryRepo([task]),
        registry=ExecutionRegistry({"boom": FailingHandler()}),
        session=FakeSession(),
        metrics=metrics,
    )

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.DEAD_LETTERED
    assert recorder.get_counter("tasks_dead_lettered") == 1


def test_stale_recovery_increments_recovery_counter() -> None:
    metrics, recorder = make_metrics()
    redis_prefix = f"test:lease:{uuid4().hex}"
    queue_name = f"test:queue:{uuid4().hex}"
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    lease_manager = TaskLeaseManager(redis_client=redis_client, prefix=redis_prefix)
    queue = RedisQueue(redis_client=redis_client, queue_name=queue_name)
    task = make_task("echo", status=TaskStatus.RUNNING, started_at=datetime.now(timezone.utc))
    repo = InMemoryRepo([task])
    recoverer = StaleTaskRecoverer(repo, lease_manager, queue, metrics=metrics)

    assert recoverer.recover(task.id) is True
    assert recorder.get_counter("stale_tasks_recovered") == 1

    cleanup_redis(redis_prefix)
    cleanup_redis(queue_name)


def test_ready_queue_depth_can_be_queried_by_priority() -> None:
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    queue_name = f"test:queue:{uuid4().hex}"
    queue = RedisQueue(redis_client=redis_client, queue_name=queue_name)
    metrics, _ = make_metrics()
    queue.enqueue(uuid4(), priority=TaskPriority.HIGH)

    assert metrics.queue_ready_depth(queue, TaskPriority.HIGH) == 1

    cleanup_redis(queue_name)


def test_delayed_queue_depth_can_be_queried() -> None:
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    queue_name = f"test:queue:{uuid4().hex}"
    queue = RedisQueue(redis_client=redis_client, queue_name=queue_name)
    metrics, _ = make_metrics()
    queue.enqueue_delayed(uuid4(), datetime.now(timezone.utc), priority=TaskPriority.NORMAL)

    assert metrics.delayed_queue_depth(queue) == 1

    cleanup_redis(queue_name)


def test_active_worker_count_can_be_queried() -> None:
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    heartbeat_prefix = f"test:worker:heartbeat:{uuid4().hex}"
    heartbeat = WorkerHeartbeat(redis_client=redis_client, prefix=heartbeat_prefix)
    registry = WorkerRegistry(heartbeat_manager=heartbeat)
    metrics, _ = make_metrics()
    heartbeat.heartbeat("worker-a", ttl=10)
    heartbeat.heartbeat("worker-b", ttl=10)

    assert metrics.active_worker_count(registry) == 2

    cleanup_redis(heartbeat_prefix)


def test_metrics_failure_does_not_break_task_execution() -> None:
    recorder = FailingMetricsRecorder()
    metrics = ForgeMetrics(recorder=recorder)
    task = make_task("echo")
    worker = Worker(
        queue=type("Q", (), {"dequeue": lambda self: task.id})(),
        task_repository=InMemoryRepo([task]),
        registry=ExecutionRegistry({"echo": EchoHandler()}),
        session=FakeSession(),
        metrics=metrics,
    )

    assert worker.process_next_task() is True
    assert task.status == TaskStatus.COMPLETED
