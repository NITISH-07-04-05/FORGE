from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from threading import Lock
from typing import Any

from app.execution.worker_registry import WorkerRegistry
from app.models.task_priority import TaskPriority
from app.queue.redis_queue import RedisQueue


@dataclass
class MetricsSnapshot:
    counters: dict[str, int] = field(default_factory=dict)
    gauges: dict[str, float] = field(default_factory=dict)
    histograms: dict[str, list[float]] = field(default_factory=dict)


class MetricsRecorder:
    def increment(self, name: str, amount: int = 1) -> None: ...
    def observe(self, name: str, value: float) -> None: ...
    def set_gauge(self, name: str, value: float) -> None: ...
    def get_counter(self, name: str) -> int: ...
    def get_gauge(self, name: str) -> float | None: ...
    def get_histogram(self, name: str) -> list[float]: ...
    def snapshot(self) -> MetricsSnapshot: ...


class NoopMetricsRecorder(MetricsRecorder):
    def increment(self, name: str, amount: int = 1) -> None:
        return None

    def observe(self, name: str, value: float) -> None:
        return None

    def set_gauge(self, name: str, value: float) -> None:
        return None

    def get_counter(self, name: str) -> int:
        return 0

    def get_gauge(self, name: str) -> float | None:
        return None

    def get_histogram(self, name: str) -> list[float]:
        return []

    def snapshot(self) -> MetricsSnapshot:
        return MetricsSnapshot()


class InMemoryMetricsRecorder(MetricsRecorder):
    def __init__(self) -> None:
        self._lock = Lock()
        self._counters: dict[str, int] = defaultdict(int)
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, list[float]] = defaultdict(list)

    def increment(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] += amount

    def observe(self, name: str, value: float) -> None:
        with self._lock:
            self._histograms[name].append(value)

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def get_counter(self, name: str) -> int:
        with self._lock:
            return self._counters.get(name, 0)

    def get_gauge(self, name: str) -> float | None:
        with self._lock:
            return self._gauges.get(name)

    def get_histogram(self, name: str) -> list[float]:
        with self._lock:
            return list(self._histograms.get(name, []))

    def snapshot(self) -> MetricsSnapshot:
        with self._lock:
            return MetricsSnapshot(
                counters=dict(self._counters),
                gauges=dict(self._gauges),
                histograms={name: list(values) for name, values in self._histograms.items()},
            )


class ForgeMetrics:
    def __init__(self, recorder: MetricsRecorder | None = None) -> None:
        self._recorder = recorder or NoopMetricsRecorder()

    def _safe(self, fn: Any, *args: Any, **kwargs: Any) -> None:
        try:
            fn(*args, **kwargs)
        except Exception:
            return None

    def record_task_completed(self) -> None:
        self._safe(self._recorder.increment, "tasks_completed")

    def record_task_failed(self) -> None:
        self._safe(self._recorder.increment, "tasks_failed")

    def record_task_retried(self) -> None:
        self._safe(self._recorder.increment, "tasks_retried")

    def record_task_dead_lettered(self) -> None:
        self._safe(self._recorder.increment, "tasks_dead_lettered")

    def record_stale_task_recovered(self) -> None:
        self._safe(self._recorder.increment, "stale_tasks_recovered")

    def record_execution_duration(self, seconds: float) -> None:
        self._safe(self._recorder.observe, "task_execution_duration_seconds", seconds)

    def queue_ready_depth(self, queue: RedisQueue, priority: TaskPriority | None = None) -> int:
        if priority is None:
            return queue.queue_length()
        return queue.queue_length(priority)

    def delayed_queue_depth(self, queue: RedisQueue) -> int:
        return queue.delayed_queue_length()

    def active_worker_count(self, worker_registry: WorkerRegistry) -> int:
        return len(worker_registry.list_worker_ids())

    def worker_is_alive(self, worker_registry: WorkerRegistry, worker_id: str) -> bool:
        return worker_registry.is_alive(worker_id)

    def worker_heartbeat_ttl(self, worker_registry: WorkerRegistry, worker_id: str) -> int:
        return worker_registry.get_ttl(worker_id)

    def snapshot(self) -> MetricsSnapshot:
        return self._recorder.snapshot()

    def get_counter(self, name: str) -> int:
        return self._recorder.get_counter(name)

    def get_gauge(self, name: str) -> float | None:
        return self._recorder.get_gauge(name)

    def get_histogram(self, name: str) -> list[float]:
        return self._recorder.get_histogram(name)


forge_metrics = ForgeMetrics()
