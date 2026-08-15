# FORGE V1

FORGE is a distributed background task execution platform built around a simple V1 pipeline:

```text
Client
  |
  v
FastAPI API
  |
  v
PostgreSQL
  |
  v
Redis Queue
  |
  v
Worker
  |
  v
Execution Registry
  |
  v
Task Handler
  |
  v
PostgreSQL
```

Background execution lets the API accept work quickly, persist durable task state, and hand off longer-running processing to workers without blocking HTTP requests.

## Architecture

```text
                Client
                  |
                  v
             FastAPI API
              /       \
             v         v
       PostgreSQL     Redis
             ^          |
             |          v
             |       Worker
             |          |
             |          v
             |   Execution Registry
             |          |
             |          v
             +---- Task Handler
```

- `app/api`: accepts task requests and task lookups.
- `app/services`: owns application workflows such as create-and-enqueue.
- `app/repositories`: persists task state in PostgreSQL.
- `app/queue`: isolates Redis queue transport.
- `app/execution/worker.py`: orchestrates dequeue, state changes, handler execution, and failure recording.
- `app/execution/registry.py`: maps `task_type` values to handlers.
- `app/execution/handlers/echo.py`: example V1 handler used to prove the pipeline.

## V1 Failure Policy

Task creation uses a simple explicit V1 policy instead of a distributed transaction:

- The API creates and flushes a `PENDING` task in the current database transaction.
- The service then enqueues the task ID into Redis.
- If enqueue fails, the API rolls back the database transaction and returns `503 Service Unavailable`.
- If enqueue succeeds but the later database commit fails, the queue may contain an orphaned task ID; the worker handles that case by skipping missing tasks and continuing.

## Local Setup

1. Install dependencies with `uv sync`.
2. Copy `.env.example` values into `.env` if needed.
3. Start PostgreSQL and Redis:

```powershell
docker compose up -d postgres redis
```

4. Run migrations:

```powershell
uv run alembic upgrade head
```

## Run The API

```powershell
uv run uvicorn app.main:app --reload
```

## Run The Worker

```powershell
uv run python -m app.execution.main
```

## Docker Setup

Bring up the complete V1 stack:

```powershell
docker compose up --build
```

Services:

- `forge-api`
- `forge-worker`
- `forge-postgres`
- `forge-redis`

## Example Task Submission

```http
POST /tasks
Content-Type: application/json

{
  "task_type": "echo",
  "payload": {
    "message": "Hello FORGE"
  }
}
```

Example:

```powershell
curl -X POST http://localhost:8000/tasks `
  -H "Content-Type: application/json" `
  -d "{\"task_type\":\"echo\",\"payload\":{\"message\":\"Hello FORGE\"}}"
```

## Example Lifecycle

1. `POST /tasks` creates a `PENDING` task in PostgreSQL and enqueues its ID in Redis.
2. The worker dequeues the task ID and loads the task from PostgreSQL.
3. The registry resolves `echo` to `EchoTaskHandler`.
4. The worker marks the task `RUNNING`.
5. The handler executes.
6. The worker marks the task `COMPLETED`.

Check status:

```http
GET /tasks/{task_id}
```

The response will report `COMPLETED` on success or `FAILED` with `error_message` on execution failure.

## Current V1 Limitations

- No retries
- No scheduling
- No priorities
- No heartbeats
- No WebSockets
- No Kafka
- No autoscaling
- No Kubernetes orchestration
- Minimal observability beyond application logs
