package config

import (
	"os"
	"regexp"
	"strconv"
)

type Config struct {
	HTTPAddr  string
	RedisAddr string

	// MetaDSN 元数据库连接串，必须带 parseTime=true 才能把 DATETIME 扫进 time.Time
	MetaDSN          string
	MetaMaxOpenConns int

	// Worker
	WorkerConcurrency int

	// 幂等锁 TTL，同时作为队列租约时长
	TaskLockTTLSeconds int

	// 重试与 DLQ
	TaskMaxRetry         int
	TaskRetryBaseSeconds int

	// 写接口限流（创建作业 / 触发执行）
	WriteRateLimit         int
	WriteRateWindowSeconds int
}

func Load() Config {
	return Config{
		HTTPAddr:  getEnv("HTTP_ADDR", ":8090"),
		RedisAddr: getEnv("REDIS_ADDR", "localhost:6379"),

		MetaDSN: getEnv("META_DSN",
			"root:root@tcp(127.0.0.1:3306)/dts_meta?parseTime=true&loc=UTC&charset=utf8mb4"),
		MetaMaxOpenConns: getEnvInt("META_MAX_OPEN_CONNS", 16),

		WorkerConcurrency: getEnvInt("WORKER_CONCURRENCY", 4),

		TaskLockTTLSeconds: getEnvInt("TASK_LOCK_TTL_SECONDS", 300),

		TaskMaxRetry:         getEnvInt("TASK_MAX_RETRY", 3),
		TaskRetryBaseSeconds: getEnvInt("TASK_RETRY_BASE_SECONDS", 1),

		WriteRateLimit:         getEnvInt("WRITE_RATE_LIMIT", 60),
		WriteRateWindowSeconds: getEnvInt("WRITE_RATE_WINDOW_SECONDS", 10),
	}
}

// dsnSecret 匹配 DSN 里的密码段 user:password@
var dsnSecret = regexp.MustCompile(`^([^:/?#]+):[^@]*@`)

// RedactDSN 去掉 DSN 里的密码，用于日志输出。
// 默认 DSN 带的是本地开发口令，生产环境必须通过环境变量注入。
func RedactDSN(dsn string) string {
	return dsnSecret.ReplaceAllString(dsn, "$1:***@")
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
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
