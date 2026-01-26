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

	w := worker.New(rdb, apiBase, cfg.DataDir, 10*time.Second)
	
	if err := w.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
