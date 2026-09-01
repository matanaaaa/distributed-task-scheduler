package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/meta"
	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
	"github.com/matanaaaa/distributed-task-scheduler/internal/transform"
)

func (h *Handler) Ping(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"service": "data-sync-platform",
		"sources": connector.RegisteredSources(),
		"sinks":   connector.RegisteredSinks(),
	})
}

// jobReq 创建/更新作业的请求体
type jobReq struct {
	Name        string `json:"name"`
	Description string `json:"description"`

	SourceSystem   string `json:"source_system"`
	ObjectType     string `json:"object_type"`
	SourceIDColumn string `json:"source_id_column"`

	SourceType   string             `json:"source_type"`
	SourceConfig model.SourceConfig `json:"source_config"`
	SinkType     string             `json:"sink_type"`
	SinkConfig   model.SinkConfig   `json:"sink_config"`

	Rules []model.TransformRule `json:"rules"`

	SyncMode        string `json:"sync_mode"`
	WatermarkColumn string `json:"watermark_column"`

	ShardColumn string `json:"shard_column"`
	ShardCount  int    `json:"shard_count"`
	BatchSize   int    `json:"batch_size"`
	ReadQPS     int    `json:"read_qps"`

	Priority string `json:"priority"`
	Enabled  *bool  `json:"enabled"`
}

// validate 把配置错误挡在创建接口，而不是等运行时才炸
func (r *jobReq) validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return errors.New("name is required")
	}
	if r.SourceType == "" || r.SinkType == "" {
		return errors.New("source_type and sink_type are required")
	}
	if r.SourceConfig.Table == "" {
		return errors.New("source_config.table is required")
	}
	if r.SinkConfig.Table == "" {
		return errors.New("sink_config.table is required")
	}
	if len(r.SinkConfig.UniqueKey) == 0 {
		return errors.New("sink_config.unique_key is required for idempotent upsert")
	}
	if r.ShardColumn == "" {
		return errors.New("shard_column is required (must be an integer column)")
	}

	mode := model.SyncMode(r.SyncMode)
	if r.SyncMode == "" {
		mode = model.SyncModeFull
	}
	if !mode.Valid() {
		return errors.New("sync_mode must be full or incremental")
	}
	if mode == model.SyncModeIncremental && r.WatermarkColumn == "" {
		return errors.New("watermark_column is required for incremental sync")
	}

	if err := transform.ValidateRules(r.Rules); err != nil {
		return err
	}

	// 唯一键必须出现在映射目标列里，否则 upsert 匹配不上任何东西
	targets := make(map[string]struct{}, len(r.Rules))
	for _, rule := range r.Rules {
		targets[rule.To] = struct{}{}
	}
	for _, k := range r.SinkConfig.UniqueKey {
		if _, ok := targets[k]; !ok {
			return errors.New("sink_config.unique_key column " + k + " must be produced by a transform rule")
		}
	}
	return nil
}

func (r *jobReq) applyTo(j *model.Job) {
	j.Name = strings.TrimSpace(r.Name)
	j.Description = r.Description
	j.SourceSystem = r.SourceSystem
	j.ObjectType = r.ObjectType
	j.SourceIDColumn = r.SourceIDColumn
	j.SourceType = r.SourceType
	j.SourceConfig = r.SourceConfig
	j.SinkType = r.SinkType
	j.SinkConfig = r.SinkConfig
	j.Rules = r.Rules

	j.SyncMode = model.SyncMode(r.SyncMode)
	if r.SyncMode == "" {
		j.SyncMode = model.SyncModeFull
	}
	j.WatermarkColumn = r.WatermarkColumn

	j.ShardColumn = r.ShardColumn
	j.ShardCount = r.ShardCount
	if j.ShardCount <= 0 {
		j.ShardCount = 1
	}
	j.BatchSize = r.BatchSize
	if j.BatchSize <= 0 {
		j.BatchSize = 1000
	}
	j.ReadQPS = r.ReadQPS

	j.Priority = r.Priority
	if j.Priority != "high" {
		j.Priority = "normal"
	}
	j.Enabled = true
	if r.Enabled != nil {
		j.Enabled = *r.Enabled
	}
}

func (h *Handler) CreateJob(c *gin.Context) {
	var req jobReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json", "detail": err.Error()})
		return
	}
	if err := req.validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 提前构造一次连接器：表名非法、唯一键缺失这类问题在这里就暴露，
	// 不用等到第一次执行才发现
	if _, err := connector.NewSource(req.SourceType, req.SourceConfig); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid source config", "detail": err.Error()})
		return
	}
	if _, err := connector.NewSink(req.SinkType, req.SinkConfig); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sink config", "detail": err.Error()})
		return
	}

	now := time.Now().UTC()
	job := &model.Job{ID: uuid.NewString(), CreatedAt: now, UpdatedAt: now}
	req.applyTo(job)

	if err := h.S.Meta.CreateJob(c.Request.Context(), job); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create job", "detail": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, toJobView(job))
}

func (h *Handler) ListJobs(c *gin.Context) {
	enabledOnly := c.Query("enabled") == "true"
	jobs, err := h.S.Meta.ListJobs(c.Request.Context(), enabledOnly)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list jobs", "detail": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"jobs": toJobViews(jobs), "total": len(jobs)})
}

func (h *Handler) GetJob(c *gin.Context) {
	job, err := h.S.Meta.GetJob(c.Request.Context(), c.Param("id"))
	if err != nil {
		respondMetaErr(c, err, "job")
		return
	}
	c.JSON(http.StatusOK, toJobView(job))
}

func (h *Handler) UpdateJob(c *gin.Context) {
	ctx := c.Request.Context()
	job, err := h.S.Meta.GetJob(ctx, c.Param("id"))
	if err != nil {
		respondMetaErr(c, err, "job")
		return
	}

	var req jobReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json", "detail": err.Error()})
		return
	}
	if err := req.validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 水位不通过更新接口修改：它是运行状态而不是配置，
	// 手改会直接导致漏数据或重复同步
	req.applyTo(job)

	if err := h.S.Meta.UpdateJob(ctx, job); err != nil {
		respondMetaErr(c, err, "job")
		return
	}
	c.JSON(http.StatusOK, toJobView(job))
}

func (h *Handler) DeleteJob(c *gin.Context) {
	if err := h.S.Meta.DeleteJob(c.Request.Context(), c.Param("id")); err != nil {
		respondMetaErr(c, err, "job")
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

func (h *Handler) SetJobEnabled(c *gin.Context) {
	var body struct {
		Enabled *bool `json:"enabled"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Enabled == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "field 'enabled' (bool) is required"})
		return
	}
	if err := h.S.Meta.SetJobEnabled(c.Request.Context(), c.Param("id"), *body.Enabled); err != nil {
		respondMetaErr(c, err, "job")
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "enabled": *body.Enabled})
}

// TriggerJob 手动触发一次同步
func (h *Handler) TriggerJob(c *gin.Context) {
	ctx := c.Request.Context()

	job, err := h.S.Meta.GetJob(ctx, c.Param("id"))
	if err != nil {
		respondMetaErr(c, err, "job")
		return
	}

	run, err := h.S.Planner.Trigger(ctx, job, model.TriggerManual)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to trigger run", "detail": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, toRunView(run))
}

func (h *Handler) ListJobRuns(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	runs, err := h.S.Meta.ListRunsByJob(c.Request.Context(), c.Param("id"), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list runs", "detail": err.Error()})
		return
	}
	out := make([]runView, 0, len(runs))
	for i := range runs {
		out = append(out, toRunView(&runs[i]))
	}
	c.JSON(http.StatusOK, gin.H{"runs": out, "total": len(out)})
}

func (h *Handler) GetRun(c *gin.Context) {
	run, err := h.S.Meta.GetRun(c.Request.Context(), c.Param("id"))
	if err != nil {
		respondMetaErr(c, err, "run")
		return
	}
	c.JSON(http.StatusOK, toRunView(run))
}

func (h *Handler) ListRunTasks(c *gin.Context) {
	tasks, err := h.S.Meta.ListTasksByRun(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list tasks", "detail": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tasks": tasks, "total": len(tasks)})
}

func (h *Handler) ListRunReconciliations(c *gin.Context) {
	recs, err := h.S.Meta.ListReconciliationsByRun(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list reconciliations", "detail": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"reconciliations": recs, "total": len(recs)})
}

// ListRunErrors 查本次执行被跳过的坏数据，带溯源三元组，便于人工修数
func (h *Handler) ListRunErrors(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
	recs, err := h.S.Meta.ListErrorRecordsByRun(c.Request.Context(), c.Param("id"), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list error records", "detail": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"errors": recs, "total": len(recs)})
}

// respondMetaErr 把存储层错误映射成合适的 HTTP 状态码
func respondMetaErr(c *gin.Context, err error, what string) {
	if errors.Is(err, meta.ErrNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": what + " not found"})
		return
	}
	c.JSON(http.StatusInternalServerError, gin.H{"error": "storage error", "detail": err.Error()})
}
