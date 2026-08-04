from __future__ import annotations

from time import sleep

from sqlalchemy.orm import Session

from app.execution.registry import ExecutionRegistry
from app.queue.redis_queue import RedisQueue
from app.repositories.task_repository import TaskRepository


class Worker:
    """Small orchestration loop that bridges the queue, registry, and persistence layer."""

    def __init__(
        self,
        queue: RedisQueue,
        task_repository: TaskRepository,
        registry: ExecutionRegistry,
        session: Session,
    ) -> None:
        self._queue = queue
        self._task_repository = task_repository
        self._registry = registry
        # The worker owns transaction boundaries for task execution state changes.
        self._session = session

    def run(self) -> None:
        while True:
            task_id = self._queue.dequeue()

            if task_id is None:
                # A short sleep avoids a tight polling loop when the queue is idle.
                sleep(1)
                continue

            task = self._task_repository.get(task_id)

            if task is None:
                continue

            try:
                handler = self._registry.get(task.task_type)

                task.status = "RUNNING"
                task.error_message = None
                self._task_repository.update(task)
                self._session.commit()

                handler.execute(task.payload)

                task.status = "COMPLETED"
                self._task_repository.update(task)
                self._session.commit()
            except Exception as exc:
                self._session.rollback()

                task.status = "FAILED"
                task.error_message = str(exc)

                try:
                    self._task_repository.update(task)
                    self._session.commit()
                except Exception:
                    self._session.rollback()

                continue
