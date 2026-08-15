from __future__ import annotations

import logging

from app.core.logging import configure_logging
from app.core.config import settings
from app.db.session import SessionLocal
from app.execution.handlers import EchoTaskHandler
from app.execution.handlers import FailTaskHandler
from app.execution.handlers import FlakyOnceTaskHandler
from app.execution.heartbeat import WorkerHeartbeat
from app.execution.registry import ExecutionRegistry
from app.execution.worker import Worker
from app.queue.redis_queue import RedisQueue
from app.repositories.task_repository import TaskRepository


def build_registry() -> ExecutionRegistry:
    return ExecutionRegistry({
        "echo": EchoTaskHandler(),
        "fail": FailTaskHandler(),
        "flaky_once": FlakyOnceTaskHandler(),
    })


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    queue = RedisQueue()
    session = SessionLocal()
    heartbeat = WorkerHeartbeat()

    try:
        worker = Worker(
            queue=queue,
            task_repository=TaskRepository(session),
            registry=build_registry(),
            session=session,
            retry_base_delay_seconds=settings.retry_base_delay_seconds,
            heartbeat_manager=heartbeat,
        )
        logger.info("Starting FORGE worker %s.", worker.worker_id)
        worker.run()
    finally:
        queue.close()
        heartbeat.close()
        session.close()


if __name__ == "__main__":
    main()
