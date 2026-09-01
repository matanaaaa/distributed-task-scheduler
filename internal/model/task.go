package model

import "time"

// Status 任务与执行状态
type Status string

const (
	StatusQueued   Status = "queued"
	StatusRunning  Status = "running"
	StatusSuccess  Status = "success"
	StatusFailed   Status = "failed"
	StatusRetrying Status = "retrying"
	// StatusDead 超过最大重试次数，已进 DLQ
	StatusDead Status = "dead"
	// StatusPartial 仅用于 JobRun：部分分片成功、部分最终失败
	StatusPartial Status = "partial"
)

// SyncTask 一个分片同步任务，也是 Redis 队列里的调度单元
type SyncTask struct {
	ID    string `json:"id"`
	RunID string `json:"run_id"`
	JobID string `json:"job_id"`

	ShardIndex int    `json:"shard_index"`
	ShardLo    string `json:"shard_lo"`
	ShardHi    string `json:"shard_hi"`

	Status     Status `json:"status"`
	Priority   string `json:"priority"`
	Attempt    int    `json:"attempt"`
	RetryCount int    `json:"retry_count"`

	// Checkpoint 本分片已成功写入的最大位置，重试时从此处续跑
	Checkpoint Watermark `json:"checkpoint"`

	RowsRead    int64 `json:"rows_read"`
	RowsWritten int64 `json:"rows_written"`
	RowsFailed  int64 `json:"rows_failed"`

	ErrorReason string     `json:"error_reason"`
	StartedAt   *time.Time `json:"started_at"`
	FinishedAt  *time.Time `json:"finished_at"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// Shard 返回该任务对应的分片区间
func (t *SyncTask) Shard() Shard {
	return Shard{Index: t.ShardIndex, Lo: t.ShardLo, Hi: t.ShardHi}
}
