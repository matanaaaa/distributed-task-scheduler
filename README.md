# distributed-task-scheduler

A distributed task scheduler in Go (Redis ZSet/Hash, worker pool, retry & DLQ).

## Architecture (MVP)

Client → API (Gin) → Redis

- **List**: `queue:high`, `queue:normal` (priority queues)
- **Hash**: `task:{id}` (task metadata & status)

Worker → `BLPOP` queues → update `task:{id}.status` (`queued → running → success`)

## Tech Stack

- Go + Gin
- Redis (ZSet/Hash)
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

## Changelog

- v0.1: API supports task create/query/download; enqueue into Redis priority queues (queue:high/queue:normal)
- v0.2: worker consumes queues and updates task status
- v0.3: (planned) upload/download task result + retry/DLQ
