package model

import "time"

// ErrorType 错误是否值得重试。
// 这是同步语义下 Retry/DLQ 的分岔点：基础设施抖动该退避重试，
// 坏数据重试一万次也不会变好，只会把队列堵死。
type ErrorType string

const (
	// ErrorRetryable 基础设施类：连接超时、死锁、目标端暂时不可用
	ErrorRetryable ErrorType = "retryable"
	// ErrorNonRetryable 数据类：字段类型错误、必填缺失、非法值、超长
	ErrorNonRetryable ErrorType = "non_retryable"
)

// ErrorCode 细分原因，用于统计"到底是哪类问题在拖后腿"
type ErrorCode string

const (
	// 数据类
	CodeTypeMismatch    ErrorCode = "type_mismatch"
	CodeMissingRequired ErrorCode = "missing_required"
	CodeIllegalValue    ErrorCode = "illegal_value"
	CodeValueTooLong    ErrorCode = "value_too_long"

	// 基础设施类
	CodeDBDeadlock    ErrorCode = "db_deadlock"
	CodeDBTimeout     ErrorCode = "db_timeout"
	CodeDBConnFailure ErrorCode = "db_conn_failure"
	CodeDBReadOnly    ErrorCode = "db_read_only"

	CodeUnknown ErrorCode = "unknown"
)

// SyncErrorRecord 一条坏数据记录。
// 落表而不是反复重试，人工修数后可按三元组重放。
type SyncErrorRecord struct {
	ID     int64  `json:"id"`
	RunID  string `json:"run_id"`
	TaskID string `json:"task_id"`
	JobID  string `json:"job_id"`

	// 溯源三元组
	SourceSystem   string `json:"source_system"`
	ObjectType     string `json:"object_type"`
	SourceRecordID string `json:"source_record_id"`

	ErrorType ErrorType `json:"error_type"`
	ErrorCode ErrorCode `json:"error_code"`
	ErrorMsg  string    `json:"error_msg"`
	// RawRow 出错行原始内容，便于人工修数后重放
	RawRow map[string]any `json:"raw_row,omitempty"`

	CreatedAt time.Time `json:"created_at"`
}
