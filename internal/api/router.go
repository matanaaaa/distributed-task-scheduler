package api

import (
	"time"

	
	"github.com/gin-gonic/gin"
	"github.com/matanaaaa/distributed-task-scheduler/internal/config"
)

type Handler struct {
	S *Service
}

func RegisterRoutes(r *gin.Engine, h *Handler, cfg config.Config) {
	r.GET("/ping", h.Ping)

	rl := RateLimitPostTasks(h.S.RDB, RateLimitConfig{
		Limit:  int64(cfg.TasksRateLimit),
		Window: time.Duration(cfg.TasksRateWindowSeconds) * time.Second,
		Prefix: "rl:tasks:",
	})
	r.POST("/tasks", rl, h.CreateTask)
	r.GET("/tasks/:id", h.GetTask)
	r.GET("/tasks/:id/download", h.DownloadTaskZip)

	r.POST("/tasks/:id/status", h.ReportTaskStatus)
    r.POST("/tasks/:id/result", h.UploadTaskResult)
    r.GET("/tasks/:id/result", h.DownloadTaskResult)
}
