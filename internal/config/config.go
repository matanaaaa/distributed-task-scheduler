package config

import (
	"os"
	"strconv"
)

type Config struct {
	HTTPAddr  string
	RedisAddr string
	DataDir   string
	APIBaseURL string


	// Worker
	WorkerConcurrency        int
	WorkerHTTPTimeoutSeconds int

	// Idempotency lock
	TaskLockTTLSeconds int

	// Retry/DLQ
	TaskMaxRetry        int
	TaskRetryBaseSeconds int

	// Rate limit (POST /tasks)
	TasksRateLimit        int 
	TasksRateWindowSeconds int 

	// Task execution (real pipeline)
	TaskExecImage          string
	TaskExecTimeoutSeconds int

	// Zip security boundaries
	TaskZipMaxBytes        int64 // upload size limit (API)
	TaskUnzipMaxBytes      int64 // total uncompressed limit (worker)
	TaskUnzipEntryMaxBytes int64 // per-entry uncompressed limit (worker)

}

func Load() Config {
	cfg := Config{
		HTTPAddr:  getEnv("HTTP_ADDR", ":8090"),
		RedisAddr: getEnv("REDIS_ADDR", "localhost:6379"),
		DataDir:   getEnv("DATA_DIR", "data"),
		APIBaseURL: getEnv("API_BASE_URL", "http://localhost:8090"),


		WorkerConcurrency:        getEnvInt("WORKER_CONCURRENCY", 4),
		WorkerHTTPTimeoutSeconds: getEnvInt("WORKER_HTTP_TIMEOUT_SECONDS", 10),

		TaskLockTTLSeconds: getEnvInt("TASK_LOCK_TTL_SECONDS", 300),

		TaskMaxRetry:         getEnvInt("TASK_MAX_RETRY", 3),
		TaskRetryBaseSeconds: getEnvInt("TASK_RETRY_BASE_SECONDS", 1),

		TasksRateLimit:         getEnvInt("TASKS_RATE_LIMIT", 20),
		TasksRateWindowSeconds: getEnvInt("TASKS_RATE_WINDOW_SECONDS", 10),

		TaskExecImage:          getEnv("TASK_EXEC_IMAGE", "ubuntu:22.04"),
		TaskExecTimeoutSeconds: getEnvInt("TASK_EXEC_TIMEOUT_SECONDS", 30),

		TaskZipMaxBytes:        getEnvInt64("TASK_ZIP_MAX_BYTES", 20<<20),
		TaskUnzipMaxBytes:      getEnvInt64("TASK_UNZIP_MAX_BYTES", 128<<20),
		TaskUnzipEntryMaxBytes: getEnvInt64("TASK_UNZIP_ENTRY_MAX_BYTES", 32<<20),

	}
	return cfg
}

func getEnv(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}

func getEnvInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getEnvInt64(k string, def int64) int64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

