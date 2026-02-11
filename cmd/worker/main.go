package main

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/matanaaaa/distributed-task-scheduler/internal/config"
	"github.com/matanaaaa/distributed-task-scheduler/internal/worker"
)

func main() {
	cfg := config.Load()

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("[worker] redis ping failed: %v", err)
	}

	apiBase := "http://localhost:8090"

	w := worker.New(rdb, apiBase, cfg.DataDir, time.Duration(cfg.WorkerHTTPTimeoutSeconds)*time.Second)
	
	w.SetConcurrency(cfg.WorkerConcurrency)
	w.SetLockTTL(time.Duration(cfg.TaskLockTTLSeconds) * time.Second)
	w.SetRetry(cfg.TaskMaxRetry, time.Duration(cfg.TaskRetryBaseSeconds)*time.Second)
	w.SetExec(cfg.TaskExecImage, time.Duration(cfg.TaskExecTimeoutSeconds)*time.Second)
	w.SetUnzipLimits(cfg.TaskUnzipMaxBytes, cfg.TaskUnzipEntryMaxBytes)

	// warmup
	if err := worker.WarmUpDockerImage(context.Background(), cfg.TaskExecImage); err != nil {
		log.Printf("[worker] docker warmup skipped: %v", err)
	}
	
	if err := w.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
