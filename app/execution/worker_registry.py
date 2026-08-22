from __future__ import annotations

from dataclasses import dataclass

from app.execution.heartbeat import WorkerHeartbeat


@dataclass(frozen=True, slots=True)
class WorkerStatus:
    worker_id: str
    alive: bool
    heartbeat_ttl_seconds: int


class WorkerRegistry:
    """Operator-facing view over active workers backed by Redis heartbeats."""

    def __init__(self, heartbeat_manager: WorkerHeartbeat | None = None, prefix: str = "forge:worker:heartbeat") -> None:
        self._heartbeat_manager = heartbeat_manager or WorkerHeartbeat(prefix=prefix)

    def list_worker_ids(self) -> list[str]:
        return self._heartbeat_manager.list_worker_ids()

    def is_alive(self, worker_id: str) -> bool:
        return self._heartbeat_manager.is_alive(worker_id)

    def get_ttl(self, worker_id: str) -> int:
        return self._heartbeat_manager.get_ttl(worker_id)

    def list_workers(self) -> list[WorkerStatus]:
        return [
            WorkerStatus(
                worker_id=worker_id,
                alive=self.is_alive(worker_id),
                heartbeat_ttl_seconds=self.get_ttl(worker_id),
            )
            for worker_id in self.list_worker_ids()
        ]


WorkerRegistryManager = WorkerRegistry
