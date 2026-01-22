package api

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/matanaaaa/distributed-task-scheduler/internal/store"
)

type Service struct {
	Store   *store.RedisStore
	DataDir string
}

func (s *Service) tasksDir() string   { return filepath.Join(s.DataDir, "tasks") }
func (s *Service) resultsDir() string { return filepath.Join(s.DataDir, "results") }

func ensureDirs(dataDir string) error {
	if err := os.MkdirAll(filepath.Join(dataDir, "tasks"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "results"), 0o755); err != nil {
		return err
	}
	return nil
}

func (h *Handler) Ping(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "api"})
}

func (h *Handler) CreateTask(c *gin.Context) {
	ctx := context.Background()

	taskType := strings.TrimSpace(c.PostForm("task_type"))
	priority := strings.ToLower(strings.TrimSpace(c.PostForm("priority")))
	useDockerStr := strings.ToLower(strings.TrimSpace(c.PostForm("use_docker")))
	dependsOn := strings.TrimSpace(c.PostForm("depends_on"))

	if taskType == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "task_type is required"})
		return
	}
	if priority != "high" && priority != "normal" {
		priority = "normal"
	}

	useDocker := false
	if useDockerStr == "true" || useDockerStr == "1" || useDockerStr == "yes" {
		useDocker = true
	}

	fh, err := c.FormFile("task_file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "task_file is required"})
		return
	}

	// 生成 task_id
	id := uuid.NewString()
	zipName := id + "_task.zip"

	// 确保 data 目录存在
	if err := ensureDirs(h.S.DataDir); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create data dirs", "detail": err.Error()})
		return
	}

	// 保存 zip
	zipPath := filepath.Join(h.S.tasksDir(), zipName)
	if err := c.SaveUploadedFile(fh, zipPath); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save task zip", "detail": err.Error()})
		return
	}

	now := time.Now().UTC().Format(time.RFC3339)
	// HSET task:{id}
	fields := map[string]any{
		"id":         id,
		"task_type":  taskType,
		"priority":   priority,
		"use_docker": boolToStr(useDocker),
		"depends_on": dependsOn,
		"status":     "queued",
		"zip_name":   zipName,
		"created_at": now,
		"updated_at": now,
	}
	if err := h.S.Store.SaveTask(ctx, id, fields); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save task", "detail": err.Error()})
		return
	}

	// RPUSH queue
	if err := h.S.Store.Enqueue(ctx, priority, store.QueueMessage{
		TaskID:   id,
		ZipName: zipName,
		TaskType: taskType,
		Priority: priority,
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue", "detail": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"task_id":  id,
		"status":   "queued",
		"zip_name": zipName,
	})
}

func (h *Handler) GetTask(c *gin.Context) {
	ctx := context.Background()
	id := c.Param("id")

	m, err := h.S.Store.GetTask(ctx, id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error", "detail": err.Error()})
		return
	}
	if len(m) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
		return
	}
	c.JSON(http.StatusOK, m)
}

func (h *Handler) DownloadTaskZip(c *gin.Context) {
	id := c.Param("id")

	// 先从 redis 取 zip_name（避免自己拼）
	ctx := context.Background()
	m, err := h.S.Store.GetTask(ctx, id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error", "detail": err.Error()})
		return
	}
	if len(m) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
		return
	}
	zipName := m["zip_name"]
	if zipName == "" {
		zipName = id + "_task.zip"
	}

	zipPath := filepath.Join(h.S.tasksDir(), zipName)
	if _, err := os.Stat(zipPath); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "task zip not found"})
		return
	}

	// 让浏览器/agent 直接保存
	c.FileAttachment(zipPath, zipName)
}

func boolToStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
