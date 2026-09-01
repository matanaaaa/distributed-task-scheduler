package main

import (
	"context"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/matanaaaa/distributed-task-scheduler/internal/api"
	"github.com/matanaaaa/distributed-task-scheduler/internal/config"
	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/meta"
	"github.com/matanaaaa/distributed-task-scheduler/internal/queue"
	syncpipe "github.com/matanaaaa/distributed-task-scheduler/internal/sync"

	// 注册 MySQL 源与目标实现
	_ "github.com/matanaaaa/distributed-task-scheduler/internal/connector/mysqlconn"
)

func main() {
	cfg := config.Load()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := rdb.Ping(ctx).Err(); err != nil {
		cancel()
		log.Fatalf("[api] redis ping failed: %v", err)
	}
	cancel()

	m, err := meta.Open(cfg.MetaDSN, cfg.MetaMaxOpenConns)
	if err != nil {
		log.Fatalf("[api] open meta db failed: %v", err)
	}
	defer m.Close()

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	if err := m.Ping(ctx); err != nil {
		cancel()
		log.Fatalf("[api] meta db ping failed: %v", err)
	}
	cancel()

	log.Printf("[api] meta db connected: %s", config.RedactDSN(cfg.MetaDSN))

	q := queue.New(rdb)

	svc := &api.Service{
		Meta:    m,
		RDB:     rdb,
		Planner: syncpipe.NewPlanner(m, q),
	}

	defer connector.CloseSharedPools()

	r := gin.Default()
	api.RegisterRoutes(r, &api.Handler{S: svc}, cfg)

	log.Printf("[api] listening on %s", cfg.HTTPAddr)
	if err := r.Run(cfg.HTTPAddr); err != nil {
		log.Fatal(err)
	}
}
