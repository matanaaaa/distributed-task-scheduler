# distributed-task-scheduler

A distributed task scheduler in Go (Redis ZSet/Hash, worker pool, retry & DLQ).

## Architecture (MVP)

Client → API (Gin) → Redis

- **List**: `queue:high`, `queue:normal` (priority queues)
- **Hash**: `task:{id}` (task metadata & status)

Worker → BLPOP queues → POST /tasks/:id/status (running → uploading → success/failed)
Worker → generate a fake result.zip → POST /tasks/:id/result upload
API persists result to data/results/ and serves download via GET /tasks/:id/result

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

### (v0.3) Auto worker closed-loop: status + result

#### Download original result zip

```bash
curl.exe -L -o .\<task_id>\_result.zip http://localhost:8090/tasks/<task_id>/result
Expand-Archive -Path .\da9b350b-765d-491f-9477-59d195782454_result.zip -DestinationPath .\unzipped -Force
Get-Content .\unzipped\result.txt
```

### (v0.3) previous edition : status report + result upload/download

```bash
//生成一个demo.txt，压缩成demo_task.zip
"hello task" | Out-File -Encoding utf8 demo.txt
Compress-Archive -Path .\demo.txt -DestinationPath .\demo_task.zip -Force

//调用POST/tasks 上传demo_task.zip
curl.exe -F "task_type=demo" -F "priority=high" -F "task_file=@.\demo_task.zip" http://localhost:8090/tasks

//生成result.txt，压成result.zip
"result ok" | Out-File -Encoding utf8 result.txt
Compress-Archive -Path .\result.txt -DestinationPath .\result.zip -Force

$body = @{
  phase="running"
  progress=60
  msg="processing"
  status="running"
} | ConvertTo-Json

//调用POST/tasks/{task_id}/status把JSON发给服务端
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8090/tasks/<task_id>/status" `
  -ContentType "application/json" `
  -Body $body

//调用GET/tasks/{id}，把Redis的task:{task_id}Hash全读出来
curl.exe http://localhost:8090/tasks/<task_id>

//调用POST/tasks/{id}/result上传result.zip
curl.exe -F "result_file=@.\result.zip" http://localhost:8090/tasks/<task_id>/result

//调用GET/tasks/{id}/result下载结果zip
curl.exe -OJ http://localhost:8090/tasks/<task_id>/result

```

## Changelog

- v0.1: API supports task create/query/download; enqueue into Redis priority queues (queue:high/queue:normal)
- v0.2: worker consumes queues and updates task status
- v0.3: worker auto status + result upload/download
