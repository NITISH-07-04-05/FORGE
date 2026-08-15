from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from uuid import UUID, uuid4

from sqlalchemy.orm import Session

from app.execution.heartbeat import WorkerHeartbeat
from app.execution.registry import ExecutionRegistry
from app.models.task import Task
from app.models.task_status import TaskStatus
from app.queue.redis_queue import RedisQueue
from app.repositories.task_repository import TaskRepository

logger = logging.getLogger(__name__)


class Worker:
    """Small orchestration loop that bridges the queue, registry, and persistence layer."""

    def __init__(
        self,
        queue: RedisQueue,
        task_repository: TaskRepository,
        registry: ExecutionRegistry,
        session: Session,
        retry_base_delay_seconds: int = 1,
        worker_id: str | None = None,
        heartbeat_manager: WorkerHeartbeat | None = None,
        heartbeat_ttl_seconds: int = 15,
        heartbeat_interval_seconds: int = 5,
    ) -> None:
        self.worker_id = worker_id or f"worker-{uuid4().hex}"
        self._queue = queue
        self._task_repository = task_repository
        self._registry = registry
        # The worker owns transaction boundaries for task execution state changes.
        self._session = session
        self._retry_base_delay_seconds = retry_base_delay_seconds
        self._heartbeat_manager = heartbeat_manager
        self._heartbeat_ttl_seconds = heartbeat_ttl_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._last_heartbeat_at: datetime | None = None
        self._running = False

    def _send_heartbeat(self) -> None:
        if self._heartbeat_manager is None:
            return

        now = datetime.now(timezone.utc)
        if (
            self._last_heartbeat_at is None
            or (now - self._last_heartbeat_at).total_seconds() >= self._heartbeat_interval_seconds
        ):
            try:
                self._heartbeat_manager.heartbeat(self.worker_id, ttl=self._heartbeat_ttl_seconds)
                self._last_heartbeat_at = now
            except Exception:
                logger.exception("Failed to publish heartbeat for worker %s.", self.worker_id)

    def run(self) -> None:
        self._running = True

        while self._running:
            self._send_heartbeat()
            self._schedule_due_retries()
            self.process_next_task()

    def stop(self) -> None:
        self._running = False
        if self._heartbeat_manager is not None:
            try:
                self._heartbeat_manager.remove(self.worker_id)
            except Exception:
                logger.exception("Failed to remove heartbeat for worker %s on stop.", self.worker_id)

    def process_next_task(self) -> bool:
        self._send_heartbeat()
        task_id = self._queue.dequeue()

        if task_id is None:
            return False

        self._process_task(task_id)
        return True

    def _process_task(self, task_id: UUID) -> None:
        task = self._load_task(task_id)

        if task is None:
            logger.warning("Skipping queued task %s because it no longer exists.", task_id)
            return

        started_at: datetime | None = None

        try:
            started_at = self._mark_running(task)
            handler = self._registry.get(task.task_type)
            handler.execute(task.payload)
            self._mark_completed(task)
            self._session.commit()
        except Exception as exc:
            self._session.rollback()
            logger.exception("Task %s failed during execution.", task_id)
            task = self._load_task(task_id)

            if task is None:
                return

            if task.retry_count < task.max_retries:
                self._handle_retryable_failure(task, str(exc), started_at=started_at)
            else:
                self._handle_terminal_failure(task, str(exc), started_at=started_at)

    def _load_task(self, task_id: UUID) -> Task | None:
        return self._task_repository.get(task_id)

    def _mark_running(self, task: Task) -> datetime:
        started_at = task.mark_running()
        self._task_repository.update(task)
        return started_at

    def _mark_completed(self, task: Task) -> None:
        task.mark_completed()
        self._task_repository.update(task)

    def _schedule_due_retries(self) -> None:
        now = datetime.now(timezone.utc)
        due_tasks = self._task_repository.list_retry_waiting_due(at=now)

        for task in due_tasks:
            if task.next_retry_at is None:
                continue

            try:
                self._queue.enqueue_delayed(task.id, task.next_retry_at, priority=task.priority)
                task.retry_enqueued_at = task.next_retry_at
                self._task_repository.update(task)
                self._session.commit()
            except Exception:
                self._session.rollback()
                logger.exception("Failed to schedule retry for task %s.", task.id)

    def _retry_delay_for(self, retry_count: int) -> datetime:
        delay_seconds = self._retry_base_delay_seconds * (2 ** max(0, retry_count - 1))
        return datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

    def _handle_retryable_failure(self, task: Task, error_message: str, started_at: datetime | None) -> None:
        if task.status == TaskStatus.PENDING:
            task.mark_running(at=started_at)

        next_retry_at = self._retry_delay_for(task.retry_count + 1)
        task.mark_retry_waiting(error_message=error_message, next_retry_at=next_retry_at)

        try:
            self._task_repository.update(task)
            self._session.commit()
        except Exception:
            self._session.rollback()
            logger.exception("Failed to persist RETRY_WAITING state for task %s.", task.id)
            return

        try:
            self._queue.enqueue_delayed(task.id, next_retry_at, priority=task.priority)
        except Exception:
            logger.exception("Failed to enqueue delayed retry for task %s.", task.id)

    def _handle_terminal_failure(self, task: Task, error_message: str, started_at: datetime | None) -> None:
        # Tasks with retries configured that exhaust their budget enter the DLQ.
        # Zero-retry failures (max_retries=0) remain ordinary FAILED — V1 behavior preserved.
        if task.max_retries > 0 and task.retry_count >= task.max_retries:
            self._handle_dead_letter(task, error_message, started_at)
        else:
            if task.status == TaskStatus.RUNNING:
                task.mark_failed(error_message)
            else:
                task.mark_failed(error_message, started_at=started_at)

            try:
                self._task_repository.update(task)
                self._session.commit()
            except Exception:
                self._session.rollback()
                logger.exception("Failed to persist FAILED state for task %s.", task.id)

    def _handle_dead_letter(self, task: Task, error_message: str, started_at: datetime | None) -> None:
        if task.status == TaskStatus.RUNNING:
            task.mark_dead_lettered(error_message)
        else:
            task.mark_dead_lettered(error_message, started_at=started_at)

        try:
            self._task_repository.update(task)
            self._session.commit()
        except Exception:
            self._session.rollback()
            logger.exception("Failed to persist DEAD_LETTERED state for task %s.", task.id)
