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

type ReportStatusReq struct {
    Phase    string `json:"phase"`
    Progress int    `json:"progress"`
    Msg      string `json:"msg"`
    Status   string `json:"status"` // optional: running/success/failed
}

func (h *Handler) ReportTaskStatus(c *gin.Context) {
    ctx := context.Background()
    id := c.Param("id")

    // 确认 task 存在
    m, err := h.S.Store.GetTask(ctx, id)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error", "detail": err.Error()})
        return
    }
    if len(m) == 0 {
        c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
        return
    }

    var req ReportStatusReq
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json", "detail": err.Error()})
        return
    }

    fields := map[string]any{
        "updated_at": time.Now().UTC().Format(time.RFC3339),
    }
    if req.Phase != "" {
        fields["phase"] = req.Phase
    }
    if req.Msg != "" {
        fields["msg"] = req.Msg
    }
    if req.Progress >= 0 && req.Progress <= 100 {
        fields["progress"] = req.Progress
    }
    if req.Status != "" {
        // 允许 running/success/failed
        switch req.Status {
        case "queued", "running", "success", "failed":
            fields["status"] = req.Status
        default:
            c.JSON(http.StatusBadRequest, gin.H{"error": "invalid status"})
            return
        }
    }

    if err := h.S.Store.UpdateTask(ctx, id, fields); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update task", "detail": err.Error()})
        return
    }

    c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) UploadTaskResult(c *gin.Context) {
    ctx := context.Background()
    id := c.Param("id")

    // 确认 task 存在
    m, err := h.S.Store.GetTask(ctx, id)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error", "detail": err.Error()})
        return
    }
    if len(m) == 0 {
        c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
        return
    }

    if err := ensureDirs(h.S.DataDir); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create data dirs", "detail": err.Error()})
        return
    }

    fh, err := c.FormFile("result_file")
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "result_file is required"})
        return
    }

    resultName := id + "_result.zip"
    resultPath := filepath.Join(h.S.resultsDir(), resultName)

    if err := c.SaveUploadedFile(fh, resultPath); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save result zip", "detail": err.Error()})
        return
    }

    now := time.Now().UTC().Format(time.RFC3339)
    fields := map[string]any{
        "result_name": resultName,
        "status":      "success",
        "phase":       "result_uploaded",
        "progress":    100,
        "updated_at":  now,
        "finished_at": now,
    }
    if err := h.S.Store.UpdateTask(ctx, id, fields); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update task", "detail": err.Error()})
        return
    }

    c.JSON(http.StatusOK, gin.H{"status": "ok", "result_name": resultName})
}

func (h *Handler) DownloadTaskResult(c *gin.Context) {
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

    resultName := m["result_name"]
    if resultName == "" {
        resultName = id + "_result.zip"
    }

    resultPath := filepath.Join(h.S.resultsDir(), resultName)
    if _, err := os.Stat(resultPath); err != nil {
        c.JSON(http.StatusNotFound, gin.H{"error": "result zip not found"})
        return
    }

    c.FileAttachment(resultPath, resultName)
}

