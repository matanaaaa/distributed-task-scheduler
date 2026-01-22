package api

import "github.com/gin-gonic/gin"

type Handler struct {
	S *Service
}

func RegisterRoutes(r *gin.Engine, h *Handler) {
	r.GET("/ping", h.Ping)

	r.POST("/tasks", h.CreateTask)
	r.GET("/tasks/:id", h.GetTask)
	r.GET("/tasks/:id/download", h.DownloadTaskZip)
}
