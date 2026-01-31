# distributed-task-scheduler

A distributed task processing MVP in Go (Gin + Redis queues + worker pool + retry & DLQ).

## Architecture (MVP)

Client → API (Gin) → Redis + Disk

Redis:

- **List queues**: `queue:high`, `queue:normal` (priority queues)
- **Hash**: `task:{id}` (task metadata & status)
- **Lock**: `lock:task:{id}` (SETNX+TTL + Lua release)
- **DLQ**: `queue:dlq` (moved here after max retries)

Disk:

- API stores uploaded task zip: `data/tasks/`
- API stores result zip: `data/results/`
- Worker generates temp result zip: `data/tmp/` (deleted after successful upload)

Worker:

- `BLPOP` from queues (high first)
- bounded concurrency worker pool (jobs channel + N consumers)
- retries with exponential backoff on failure; after max retries, marks task as `dead` and pushes to `queue:dlq`
- `POST /tasks/:id/status` (running → uploading → success/failed)
- `POST /tasks/:id/result` upload result zip
- `GET /tasks/:id/result` download result zip

## Tech Stack

- Go + Gin
- Redis (List/Hash)
- Docker Compose

## Quick Start

### 1) Start Redis

```bash
docker compose up -d
```

### 2) Start API

```bash
go run ./cmd/api/main.go
```

### 2) Start Worker

```bash
go run ./cmd/worker/main.go
```

## Configuration (env)

Common:

- `HTTP_ADDR` (default: `:8090`)
- `REDIS_ADDR` (default: `localhost:6379`)
- `DATA_DIR` (default: `data`)

Worker:

- `WORKER_CONCURRENCY` (default: `4`)
- `WORKER_HTTP_TIMEOUT_SECONDS` (default: `10`)

Idempotency lock:

- `TASK_LOCK_TTL_SECONDS` (default: `300`)

Retry/DLQ:

- `TASK_MAX_RETRY` (default: `3`)
- `TASK_RETRY_BASE_SECONDS` (default: `1`)

Example (PowerShell):

```powershell
$env:WORKER_CONCURRENCY="8"
$env:TASK_MAX_RETRY="5"
go run .\cmd\worker\main.go
```

## Demo (Windows PowerShell)

### Create a demo zip:

```bash
"hello task" | Out-File -Encoding utf8 demo.txt
Compress-Archive -Path .\demo.txt -DestinationPath .\demo_task.zip -Force
```

### Create task (upload zip and enqueue):

```bash
curl.exe -F "task_type=demo" -F "priority=high" -F "task_file=@.\demo_task.zip" http://localhost:8090/tasks
```

### Query task status (replace <task_id>):

```bash
curl.exe http://localhost:8090/tasks/<task_id>
```

### Download task zip (agent/worker can use this endpoint):

```bash
curl.exe -OJ http://localhost:8090/tasks/<task_id>/download
```

### (v0.3) Auto worker closed-loop: status + result

#### Download original result zip

```bash
curl.exe -L -o .\<task_id>\_result.zip http://localhost:8090/tasks/<task_id>/result
Expand-Archive -Path .\da9b350b-765d-491f-9477-59d195782454_result.zip -DestinationPath .\unzipped -Force
Get-Content .\unzipped\result.txt
```

### (v0.4) Retry + DLQ (fault injection)

Start worker with fault injection enabled:

```bash
$env:ENABLE_FAIL_TEST="1"
go run .\cmd\worker\main.go
```

Create a failing task:

```bash
$resp = curl.exe -s -F "task_type=fail" -F "priority=high" -F "task_file=@.\demo_task.zip" http://localhost:8090/tasks
$taskId = ($resp | ConvertFrom-Json).task_id
$taskId
```

Verify DLQ and task status:

```bash
docker exec -it redis redis-cli LLEN queue:dlq
docker exec -it redis redis-cli LRANGE queue:dlq 0 0
curl.exe http://localhost:8090/tasks/$taskId
```

## Changelog

- v0.1: API supports task create/query/download; enqueue into Redis priority queues (queue:high/queue:normal)
- v0.2: worker consumes queues and updates task status
- v0.3: worker auto status + result upload/download
  - v0.3.1: idempotency lock to prevent duplicate execution (SETNX+TTL + Lua release)
  - v0.3.2: worker pool (bounded concurrency with jobs channel)
- v0.4: retry with exponential backoff + DLQ (queue:dlq)
