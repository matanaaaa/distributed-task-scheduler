package api

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/matanaaaa/distributed-task-scheduler/internal/config"
)

func RegisterRoutes(r *gin.Engine, h *Handler, cfg config.Config) {
	r.GET("/ping", h.Ping)
	r.GET("/healthz", h.Healthz)
	r.GET("/metrics", h.Metrics)

	// 写接口限流。作业配置里含库凭据和原样拼进 SQL 的过滤条件，
	// 生产部署必须在这一组接口前再加鉴权，限流只挡滥用不挡越权。
	writeLimit := RateLimitWrites(h.S.RDB, RateLimitConfig{
		Limit:  int64(cfg.WriteRateLimit),
		Window: time.Duration(cfg.WriteRateWindowSeconds) * time.Second,
		Prefix: "rl:write:",
	})

	jobs := r.Group("/jobs")
	{
		jobs.GET("", h.ListJobs)
		jobs.GET("/:id", h.GetJob)
		jobs.GET("/:id/runs", h.ListJobRuns)

		jobs.POST("", writeLimit, h.CreateJob)
		jobs.PUT("/:id", writeLimit, h.UpdateJob)
		jobs.DELETE("/:id", writeLimit, h.DeleteJob)
		jobs.POST("/:id/enabled", writeLimit, h.SetJobEnabled)
		jobs.POST("/:id/trigger", writeLimit, h.TriggerJob)
	}

	runs := r.Group("/runs")
	{
		runs.GET("/:id", h.GetRun)
		runs.GET("/:id/tasks", h.ListRunTasks)
		runs.GET("/:id/reconciliations", h.ListRunReconciliations)
		runs.GET("/:id/errors", h.ListRunErrors)
	}
}
