package config

import (
	"os"
	"strconv"
)

type Config struct {
	HTTPAddr  string
	RedisAddr string
	DataDir   string

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

}

func Load() Config {
	cfg := Config{
		HTTPAddr:  getEnv("HTTP_ADDR", ":8090"),
		RedisAddr: getEnv("REDIS_ADDR", "localhost:6379"),
		DataDir:   getEnv("DATA_DIR", "data"),

		WorkerConcurrency:        getEnvInt("WORKER_CONCURRENCY", 4),
		WorkerHTTPTimeoutSeconds: getEnvInt("WORKER_HTTP_TIMEOUT_SECONDS", 10),

		TaskLockTTLSeconds: getEnvInt("TASK_LOCK_TTL_SECONDS", 300),

		TaskMaxRetry:         getEnvInt("TASK_MAX_RETRY", 3),
		TaskRetryBaseSeconds: getEnvInt("TASK_RETRY_BASE_SECONDS", 1),

		TasksRateLimit:         getEnvInt("TASKS_RATE_LIMIT", 20),
		TasksRateWindowSeconds: getEnvInt("TASKS_RATE_WINDOW_SECONDS", 10),

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
