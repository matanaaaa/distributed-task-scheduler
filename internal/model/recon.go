package model

import "time"

// ReconMode 对账深度，逐档升级
type ReconMode string

const (
	// ReconCount 只比总数，最便宜
	ReconCount ReconMode = "count"
	// ReconKeySet 比主键集合，能定位到具体缺失/多余的记录
	ReconKeySet ReconMode = "keyset"
	// ReconField 比关键字段，能发现主键相同但内容不一致
	ReconField ReconMode = "field"
)

// ReconResult 对账结论
type ReconResult string

const (
	ReconOK       ReconResult = "ok"
	ReconMismatch ReconResult = "mismatch"
	ReconError    ReconResult = "error"
)

// Reconciliation 一次对账结果
type Reconciliation struct {
	ID    string `json:"id"`
	RunID string `json:"run_id"`
	JobID string `json:"job_id"`

	Mode ReconMode `json:"mode"`

	SourceCount int64 `json:"source_count"`
	TargetCount int64 `json:"target_count"`
	// MissingCount 源有目标无
	MissingCount int64 `json:"missing_count"`
	// ExtraCount 目标有源无，通常源端物理删除导致（watermark 增量感知不到 DELETE）
	ExtraCount int64 `json:"extra_count"`
	// MismatchCount 主键相同但关键字段不一致
	MismatchCount int64 `json:"mismatch_count"`

	Result      ReconResult `json:"result"`
	ErrorReason string      `json:"error_reason"`
	// Detail 差异主键抽样，只保留前 N 条
	Detail *ReconDetail `json:"detail,omitempty"`

	CheckedAt time.Time `json:"checked_at"`
}

// ReconDetail 差异抽样明细
type ReconDetail struct {
	MissingKeys  []string `json:"missing_keys,omitempty"`
	ExtraKeys    []string `json:"extra_keys,omitempty"`
	MismatchKeys []string `json:"mismatch_keys,omitempty"`
	// Truncated 为 true 表示差异条数超过抽样上限，上面几组只是样本
	Truncated bool `json:"truncated"`
}
