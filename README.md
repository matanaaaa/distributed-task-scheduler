# distributed-task-scheduler

A distributed task processing MVP in Go (Gin + Redis queues + worker pool + retry & DLQ + rate limit).

## Architecture (MVP)

Client → API (Gin) → Redis + Disk

Redis:

- **List queues**: `queue:high`, `queue:normal` (priority queues)
- **Hash**: `task:{id}` (task metadata & status)
- **Lock**: `lock:task:{id}` (SETNX+TTL + Lua release)
- **DLQ**: `queue:dlq` (moved here after max retries)
- **In-flight queue**: `queue:inflight` (tasks in progress)
- **Processing**: `z:processing` (sorted set, tracks tasks currently being processed with timestamps)

Disk:

- API stores uploaded task zip: `data/tasks/`
- API stores result zip: `data/results/`
- Worker generates temp result zip: `data/tmp/` (deleted after successful upload)

Worker:

- **BRPopLPush from queues** (high first)
- **Bounded concurrency worker pool** (jobs channel + N consumers)
- **Best-effort idempotency lock** (SETNX + TTL) to reduce duplicate execution
- **Result upload** is protected by attempt gating (stale attempt → 409, latest wins)
- **Retries with exponential backoff on failure**; after max retries, marks task as `dead` and pushes to `queue:dlq`
- **Watchdog mechanism**: Monitors tasks in inflight and processing queues, requeues timed-out tasks to their respective priority queues
- **POST /tasks/:id/status** (running → uploading → success/failed)
- **POST /tasks/:id/result** upload result zip
- **GET /tasks/:id/result** download result zip
- **GET /healthz**: basic health check (Redis ping + data dir writable)
- **GET /metrics**: plain text metrics (queue length + processing gauge + task counters)

## Tech Stack

- Go + Gin
- Redis (List/Hash/Sorted Set)
- Docker Compose

## Quick Start

### Option A) Docker Compose (v0.8, recommended)

Bring up the full stack (redis + api + workers) with one command.

```powershell
docker compose up -d --build --scale worker=3
```

API: http://localhost:8090
Default execution mode: local (use_docker=false)

### Option B) Manual (without compose)

### 1) Start Redis

```bash
docker compose up -d
```

### 2) Start API

```bash
go run ./cmd/api/main.go
```

### 3) Start Worker

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
- `TASK_EXEC_IMAGE` (only used when use_docker=true)
- `TASK_EXEC_TIMEOUT_SECONDS` (default: 30)

Idempotency lock:

- `TASK_LOCK_TTL_SECONDS` (default: `300`)

Retry/DLQ:

- `TASK_MAX_RETRY` (default: `3`)
- `TASK_RETRY_BASE_SECONDS` (default: `1`)

Rate limit (POST /tasks only):

- `TASKS_RATE_LIMIT` (default: `20`)
- `TASKS_RATE_WINDOW_SECONDS` (default: `10`)

Zip security boundaries:

- `TASK_ZIP_MAX_BYTES` (default: `20971520` = 20MB)  
  API upload size limit for `POST /tasks` (rejects oversized uploads early)

- `TASK_UNZIP_MAX_BYTES` (default: `134217728` = 128MB)  
  Worker total uncompressed limit when extracting task.zip (prevents zip bombs)

- `TASK_UNZIP_ENTRY_MAX_BYTES` (default: `33554432` = 32MB)  
  Worker per-entry uncompressed limit when extracting task.zip (prevents single huge file)

Example (Config through PowerShell):

```powershell
$env:WORKER_CONCURRENCY="8"
$env:TASK_MAX_RETRY="5"
go run .\cmd\worker\main.go
```

## Task Contract & Execution

This project treats each task as a zip bundle.

### Task zip format

When creating a task, you upload a task.zip. The zip MUST include:

- run.sh (required): entrypoint script executed by the worker

Your run.sh MUST write all artifacts to:

- output/ (directory, created by run.sh if not exists)

Recommended structure inside task.zip:

```text
task.zip
├─ run.sh
└─ (your files...)
```

Example run.sh behavior:

- read inputs from current working directory (/work)
- create output/
- write artifacts into output/

### How the worker runs a task

- **Local mode (default)**: unzip → `bash run.sh`
- **Docker sandbox (optional)**: unzip → `docker run ... bash run.sh`

```bash
docker run --rm \
  --name task_<task_id>_attempt_<attempt> \
  --network none \
  --cpus 1 --memory 512m \
  -v <workdir>:/work -w /work \
  $TASK_EXEC_IMAGE bash run.sh

```

- <workdir> is a host directory containing the extracted task files

- container working directory is /work

- entrypoint is bash run.sh

### Result bundle (worker → API)

After execution, the worker uploads a result.zip containing:

```text
result.zip
├─ output/         # task artifacts produced by run.sh
├─ stdout.log      # captured stdout from container execution
├─ stderr.log      # captured stderr from container execution
└─ metadata.json   # execution metadata (image, exit code, duration, etc.)
```

Notes:

- The worker uploads result.zip even on failure (timeout / non-zero exit / contract violation / unzip/download error),
  so users can always download /tasks/:id/result to debug without checking Redis.
- metadata.json includes attempt, exit_code, timeout, duration_ms and error (if any).

### Idempotency & result consistency (attempt gating)

The worker uses a best-effort idempotency lock (SETNX + TTL), so duplicate execution may still happen (lock expiry / restart / network jitter).
Each result upload includes an `attempt` number and the API only persists the **latest attempt** (stale uploads return `409 Conflict`).

Sandbox env configuration

Worker execution environment variables:

- TASK_EXEC_IMAGE (default: ubuntu:22.04)

- TASK_EXEC_TIMEOUT_SECONDS (default: 30)

Notes:

- If execution exceeds TASK_EXEC_TIMEOUT_SECONDS, the worker should terminate the run and mark the attempt as failed (and will follow retry/DLQ policy if enabled).

- TASK_EXEC_IMAGE should contain the runtime dependencies your run.sh needs.

### Zip safety boundaries (defense-in-depth)

To protect the API/worker from malicious task archives (zip slip / zip bomb), the system enforces:

- API ingress: request body / upload size limited to **20MB** (`TASK_ZIP_MAX_BYTES`)
- Worker unzip:
  - **Zip-slip detection**: reject entries that escape dstDir (e.g. `../evil.txt`, absolute paths)
  - **Total uncompressed limit**: **128MB** (`TASK_UNZIP_MAX_BYTES`)
  - **Per-entry uncompressed limit**: **32MB** (`TASK_UNZIP_ENTRY_MAX_BYTES`)

If unzip is rejected, the worker still uploads `result.zip`,
and `metadata.json.error` contains the exact reason (no files are written outside the work directory).

## Demo (Windows PowerShell)

### Create a demo zip:

```powershell
# Create a minimal task zip that follows the contract (contains run.sh and writes to output/)
Remove-Item -Recurse -Force .\demo_task_dir -ErrorAction SilentlyContinue
Remove-Item -Force .\demo_task.zip -ErrorAction SilentlyContinue

New-Item -ItemType Directory -Force .\demo_task_dir | Out-Null

$sh = @"
#!/bin/sh
set -e
mkdir -p output
echo "hello from task" > output/result.txt
echo "stdout line"
echo "stderr line" 1>&2
"@ -replace "`r`n", "`n"

$runShPath = Join-Path (Get-Location) "demo_task_dir\run.sh"
[System.IO.File]::WriteAllText($runShPath, $sh, (New-Object System.Text.UTF8Encoding($false)))

Compress-Archive -Force -Path .\demo_task_dir\* -DestinationPath .\demo_task.zip

```

### Create task (upload zip and enqueue):

```powershell
curl.exe -F "task_type=demo" -F "priority=high" -F "task_file=@.\demo_task.zip" http://localhost:8090/tasks
```

### Query task status (replace <task_id>):

```powershell
curl.exe http://localhost:8090/tasks/<task_id>
```

### Download task zip (agent/worker can use this endpoint):

```powershell
curl.exe -OJ http://localhost:8090/tasks/<task_id>/download
```

### (v0.3) Auto worker closed-loop: status + result

#### Download original result zip

```powershell
# Download and inspect result.zip
$resp = curl.exe -s -F "task_type=demo" -F "priority=high" -F "task_file=@.\demo_task.zip" http://localhost:8090/tasks
$taskId = ($resp | ConvertFrom-Json).task_id
$taskId

curl.exe http://localhost:8090/tasks/$taskId

curl.exe -L -o .\result.zip http://localhost:8090/tasks/$taskId/result
Expand-Archive -Path .\result.zip -DestinationPath .\unzipped -Force
Get-Content .\unzipped\metadata.json
Get-Content .\unzipped\output\result.txt

```

### (v0.4) Retry + DLQ (fault injection)

Start worker with fault injection enabled:

```powershell
$env:ENABLE_FAIL_TEST="1"
go run .\cmd\worker\main.go
```

Create a failing task:

```powershell
$resp = curl.exe -s -F "task_type=fail" -F "priority=high" -F "task_file=@.\demo_task.zip" http://localhost:8090/tasks
$taskId = ($resp | ConvertFrom-Json).task_id
$taskId
```

Verify DLQ and task status:

```powershell
docker exec -it redis redis-cli LLEN queue:dlq
docker exec -it redis redis-cli LRANGE queue:dlq 0 0
curl.exe http://localhost:8090/tasks/$taskId
```

### (v0.5) Rate limit (POST /tasks)

Example: allow 3 requests / 10 seconds, the 4th+ returns 429.

```powershell
for ($i=1; $i -le 5; $i++) {
Write-Host "== request $i =="
curl.exe -s -w "`nHTTP %{http_code}`n" `    -F "task_type=demo" -F "priority=high" -F "task_file=@.\demo_task.zip"`
http://localhost:8090/tasks
}
```

### (v0.6) Healthz + Metrics

Health check:

```powershell
curl.exe -i http://localhost:8090/healthz
```

Metrics:

```powershell
curl.exe http://localhost:8090/metrics
```

Example output:

```text
# TYPE dts_queue_length gauge
dts_queue_length{queue="high"} 0
dts_queue_length{queue="normal"} 0
dts_queue_length{queue="dlq"} 3

# TYPE dts_tasks_processing gauge
dts_tasks_processing 0

# TYPE dts_tasks_total counter
dts_tasks_total{status="success"} 7
dts_tasks_total{status="failed"} 2
dts_tasks_total{status="dead"} 1
```

Notes:

- `dts_tasks_processing` is a gauge for in-flight tasks (acquired lock but not finished)
- `dts_tasks_total{status="failed"}` counts each failed attempt (including retries)
- `dts_tasks_total{status="dead"}` counts tasks moved to DLQ

### (v0.7) Security tests (zip boundaries)

PowerShell scripts are provided under `scripts/security/`.

- Zip Slip: `../evil.txt` entry should be rejected (see `make_zipslip.ps1`).
- Zip Bomb: total unzip > 128MB should be rejected (see `make_zipbomb.ps1`).
- Single entry > 32MB should be rejected (see `make_entry40m.ps1`).

Verification: after submitting a malicious zip, download `/tasks/<task_id>/result`,
then check `metadata.json.error` for the exact rejection reason.

## Changelog

- v0.1: API supports task create/query/download; enqueue into Redis priority queues (queue:high/queue:normal)
- v0.2: worker consumes queues and updates task status
- v0.3: worker auto status + result upload/download
  - v0.3.1: idempotency lock to prevent duplicate execution (SETNX+TTL + Lua release)
  - v0.3.2: worker pool (bounded concurrency with jobs channel)
- v0.4: retry with exponential backoff + DLQ (queue:dlq)
- v0.5: rate limit for POST /tasks (429 on exceed)
- v0.6: observability endpoints `/healthz` and `/metrics`
- v0.7: task contract + docker sandbox execution (run.sh + output/ + result)
  - v0.7.1: always upload result.zip even on failed attempts (timeout/exit!=0/contract/unzip/download), attempt tracking + docker sandbox hardening (name/network/resource limits)
  - v0.7.2: zip security boundaries (API upload limit + worker unzip zip-slip + size limits)
- v0.8: Docker Compose one-command stack (redis + api + workers), worker horizontal scaling via `--scale worker=N`, and default local exec mode (`use_docker=false`) to run demos without docker-in-docker.
- v0.9: Added watchdog mechanism for monitoring tasks in inflight and processing queues; requeues timed-out tasks back to their respective priority queues.
