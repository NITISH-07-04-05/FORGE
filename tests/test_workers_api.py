from __future__ import annotations

import time
from uuid import uuid4

from fastapi.testclient import TestClient
from redis import Redis
from redis.exceptions import RedisError

from app.api.routers.workers import get_worker_heartbeat, get_worker_registry
from app.core.config import settings
from app.execution.heartbeat import WorkerHeartbeat
from app.execution.worker_registry import WorkerRegistry, WorkerStatus
from app.main import create_app


def make_client() -> tuple[TestClient, Redis, WorkerHeartbeat]:
    app = create_app()
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    heartbeat = WorkerHeartbeat(redis_client=redis_client, prefix=f"test:worker:heartbeat:{uuid4().hex}")
    registry = WorkerRegistry(heartbeat_manager=heartbeat)

    def override_heartbeat() -> WorkerHeartbeat:
        return heartbeat

    def override_registry() -> WorkerRegistry:
        return registry

    app.dependency_overrides[get_worker_heartbeat] = override_heartbeat
    app.dependency_overrides[get_worker_registry] = override_registry
    return TestClient(app), redis_client, heartbeat


def cleanup(redis_client: Redis, heartbeat: WorkerHeartbeat) -> None:
    keys = redis_client.keys(f"{heartbeat._prefix}*")  # type: ignore[attr-defined]
    if keys:
        redis_client.delete(*keys)
    redis_client.close()


def test_no_active_workers() -> None:
    client, redis_client, heartbeat = make_client()

    response = client.get("/workers")

    assert response.status_code == 200
    assert response.json() == {"workers": []}

    cleanup(redis_client, heartbeat)


def test_one_active_worker() -> None:
    client, redis_client, heartbeat = make_client()
    worker_id = f"worker-{uuid4().hex}"
    heartbeat.heartbeat(worker_id, ttl=30)

    response = client.get("/workers")

    assert response.status_code == 200
    body = response.json()
    assert body["workers"] == [
        {"worker_id": worker_id, "alive": True, "heartbeat_ttl_seconds": body["workers"][0]["heartbeat_ttl_seconds"]}
    ]
    assert 0 < body["workers"][0]["heartbeat_ttl_seconds"] <= 30

    cleanup(redis_client, heartbeat)


def test_multiple_active_workers() -> None:
    client, redis_client, heartbeat = make_client()
    worker_1 = f"worker-{uuid4().hex}"
    worker_2 = f"worker-{uuid4().hex}"
    heartbeat.heartbeat(worker_1, ttl=30)
    heartbeat.heartbeat(worker_2, ttl=20)

    response = client.get("/workers")

    assert response.status_code == 200
    workers = response.json()["workers"]
    assert {worker["worker_id"] for worker in workers} == {worker_1, worker_2}

    cleanup(redis_client, heartbeat)


def test_expired_heartbeat_is_not_reported_as_active() -> None:
    client, redis_client, heartbeat = make_client()
    worker_id = f"worker-{uuid4().hex}"
    heartbeat.heartbeat(worker_id, ttl=1)
    time.sleep(1.5)

    response = client.get("/workers")

    assert response.status_code == 200
    assert response.json() == {"workers": []}

    cleanup(redis_client, heartbeat)


def test_worker_ttl_is_reported_correctly() -> None:
    client, redis_client, heartbeat = make_client()
    worker_id = f"worker-{uuid4().hex}"
    heartbeat.heartbeat(worker_id, ttl=20)

    response = client.get("/workers")

    assert response.status_code == 200
    ttl = response.json()["workers"][0]["heartbeat_ttl_seconds"]
    assert 0 < ttl <= 20

    cleanup(redis_client, heartbeat)


def test_api_response_serialization() -> None:
    client, redis_client, heartbeat = make_client()
    worker = WorkerStatus(worker_id="worker-123", alive=True, heartbeat_ttl_seconds=15)

    class StubRegistry:
        def list_workers(self) -> list[WorkerStatus]:
            return [worker]

    app = client.app
    def override_registry() -> StubRegistry:
        return StubRegistry()

    app.dependency_overrides[get_worker_registry] = override_registry

    response = client.get("/workers")

    assert response.status_code == 200
    assert response.json() == {
        "workers": [
            {"worker_id": "worker-123", "alive": True, "heartbeat_ttl_seconds": 15}
        ]
    }

    cleanup(redis_client, heartbeat)


def test_redis_failure_handling() -> None:
    client, redis_client, heartbeat = make_client()

    class FailingRegistry:
        def list_workers(self) -> list[WorkerStatus]:
            raise RedisError("redis unavailable")

    def override_registry() -> FailingRegistry:
        return FailingRegistry()

    client.app.dependency_overrides[get_worker_registry] = override_registry

    response = client.get("/workers")

    assert response.status_code == 503
    assert response.json()["detail"] == "Worker registry is temporarily unavailable."

    cleanup(redis_client, heartbeat)
