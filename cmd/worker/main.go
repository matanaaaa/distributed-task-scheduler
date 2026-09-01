package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/matanaaaa/distributed-task-scheduler/internal/config"
	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/meta"
	"github.com/matanaaaa/distributed-task-scheduler/internal/worker"

	// 注册 MySQL 源与目标实现
	_ "github.com/matanaaaa/distributed-task-scheduler/internal/connector/mysqlconn"
)

func main() {
	cfg := config.Load()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := rdb.Ping(ctx).Err(); err != nil {
		cancel()
		log.Fatalf("[worker] redis ping failed: %v", err)
	}
	cancel()

	m, err := meta.Open(cfg.MetaDSN, cfg.MetaMaxOpenConns)
	if err != nil {
		log.Fatalf("[worker] open meta db failed: %v", err)
	}
	defer m.Close()

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	if err := m.Ping(ctx); err != nil {
		cancel()
		log.Fatalf("[worker] meta db ping failed: %v", err)
	}
	cancel()

	w := worker.New(rdb, m)
	w.SetConcurrency(cfg.WorkerConcurrency)
	w.SetLockTTL(time.Duration(cfg.TaskLockTTLSeconds) * time.Second)
	w.SetRetry(cfg.TaskMaxRetry, time.Duration(cfg.TaskRetryBaseSeconds)*time.Second)

	// 收到信号先停止领新任务，正在跑的分片被中断后
	// 靠租约超时重新入队，从 checkpoint 续跑
	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	defer connector.CloseSharedPools()

	if err := w.Run(runCtx); err != nil && err != context.Canceled {
		log.Fatalf("[worker] exited with error: %v", err)
	}
	log.Println("[worker] shutdown complete")
}
