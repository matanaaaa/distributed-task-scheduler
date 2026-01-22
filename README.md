# distributed-task-scheduler

A distributed task scheduler in Go (Redis ZSet/Hash, worker pool, retry & DLQ).

## Tech Stack

- Go + Gin
- Redis (ZSet/Hash)
- Docker Compose

## Quick Start

```bash
docker compose up -d
```

## Changelog
- v0.1: API supports task create/query/download; enqueue into Redis priority queues (queue:high/queue:normal)
- v0.2: (planned) worker consumes queues and updates task status
- v0.3: (planned) upload/download task result + retry/DLQ
