package model

import "time"

// TriggerType 触发方式
type TriggerType string

const (
	TriggerManual TriggerType = "manual"
	TriggerCron   TriggerType = "cron"
)

// JobRun 一次同步执行，由若干分片任务组成
type JobRun struct {
	ID          string      `json:"id"`
	JobID       string      `json:"job_id"`
	TriggerType TriggerType `json:"trigger_type"`
	Status      Status      `json:"status"`

	SyncMode SyncMode `json:"sync_mode"`
	// 本次增量的复合水位区间 (WatermarkFrom, WatermarkTo]，全量时为零值
	WatermarkFrom Watermark `json:"watermark_from"`
	WatermarkTo   Watermark `json:"watermark_to"`

	ShardTotal  int `json:"shard_total"`
	ShardDone   int `json:"shard_done"`
	ShardFailed int `json:"shard_failed"`

	RowsRead    int64 `json:"rows_read"`
	RowsWritten int64 `json:"rows_written"`
	RowsFailed  int64 `json:"rows_failed"`

	ErrorReason string     `json:"error_reason"`
	StartedAt   time.Time  `json:"started_at"`
	FinishedAt  *time.Time `json:"finished_at"`
}
