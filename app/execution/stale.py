from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from app.execution.lease import TaskLeaseManager
from app.models.task import Task
from app.models.task_status import TaskStatus
from app.queue.redis_queue import RedisQueue
from app.repositories.task_repository import TaskRepository


class StaleTaskRecoveryError(RuntimeError):
    """Raised when a recovered stale task cannot be re-enqueued safely."""


@dataclass(frozen=True, slots=True)
class StaleTaskCandidate:
    task_id: UUID
    started_at: datetime | None


class StaleTaskDetector:
    """Detect tasks that are stuck RUNNING after their execution lease expired."""

    def __init__(self, task_repository: TaskRepository, lease_manager: TaskLeaseManager) -> None:
        self._task_repository = task_repository
        self._lease_manager = lease_manager

    def list_candidates(self, limit: int = 100) -> list[StaleTaskCandidate]:
        candidates: list[StaleTaskCandidate] = []

        for task in self._task_repository.list_running(limit=limit):
            if not self._lease_manager.get_owner(task.id):
                candidates.append(StaleTaskCandidate(task_id=task.id, started_at=task.started_at))

        return candidates

    def is_stale(self, task: Task) -> bool:
        return task.status == TaskStatus.RUNNING and not self._lease_manager.get_owner(task.id)


class StaleTaskRecoverer:
    """Recover stale RUNNING tasks back to PENDING and requeue them safely."""

    def __init__(
        self,
        task_repository: TaskRepository,
        lease_manager: TaskLeaseManager,
        queue: RedisQueue,
        claim_ttl_seconds: int = 30,
    ) -> None:
        self._task_repository = task_repository
        self._lease_manager = lease_manager
        self._queue = queue
        self._claim_ttl_seconds = claim_ttl_seconds

    def _claim_key(self, task_id: UUID) -> str:
        return f"forge:recovery:claim:{task_id}"

    def _acquire_claim(self, task_id: UUID) -> bool:
        return self._lease_manager.acquire_claim(self._claim_key(task_id), ttl=self._claim_ttl_seconds)

    def _release_claim(self, task_id: UUID) -> None:
        self._lease_manager.release_claim(self._claim_key(task_id))

    def recover(self, task_id: UUID, limit: int | None = None) -> bool:
        if not self._acquire_claim(task_id):
            return False

        try:
            task = self._task_repository.get_for_update(task_id)

            if task is None or task.status != TaskStatus.RUNNING:
                return False

            if self._lease_manager.get_owner(task.id):
                return False

            task.recover_stale(at=datetime.now(timezone.utc))
            self._task_repository.update(task)

            try:
                self._task_repository.session.commit()
            except Exception:
                self._task_repository.session.rollback()
                raise

            try:
                self._queue.enqueue(task.id, priority=task.priority)
            except Exception as exc:
                self._task_repository.session.rollback()
                task.mark_failed(f"Dispatch failed during stale recovery: {exc}")
                self._task_repository.update(task)
                self._task_repository.session.commit()
                raise StaleTaskRecoveryError(
                    f"Task {task.id} could not be re-enqueued after stale recovery."
                ) from exc

            return True
        finally:
            self._release_claim(task_id)
