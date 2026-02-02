package main

import (
	"context"
	"log"

	"github.com/gin-gonic/gin"

	"github.com/matanaaaa/distributed-task-scheduler/internal/api"
	"github.com/matanaaaa/distributed-task-scheduler/internal/config"
	"github.com/matanaaaa/distributed-task-scheduler/internal/store"
)

func main() {
	cfg := config.Load()

	st := store.NewRedisStore(cfg.RedisAddr)
	if err := st.Ping(context.Background()); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}

	svc := &api.Service{
		Store:   st,
		DataDir: cfg.DataDir,
		RDB:     st.Client(),
	}
	log.Printf("[api] DataDir=%s", svc.DataDir)

	h := &api.Handler{S: svc}

	r := gin.Default()
	api.RegisterRoutes(r, h, cfg)

	log.Printf("api listening on %s", cfg.HTTPAddr)
	if err := r.Run(cfg.HTTPAddr); err != nil {
		log.Fatal(err)
	}
}
