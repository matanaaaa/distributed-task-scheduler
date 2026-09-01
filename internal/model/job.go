package model

import "time"

// SyncMode 同步模式
type SyncMode string

const (
	// SyncModeFull 全量：每次重扫整表
	SyncModeFull SyncMode = "full"
	// SyncModeIncremental 增量：按 watermark 列拉取变更
	SyncModeIncremental SyncMode = "incremental"
)

func (m SyncMode) Valid() bool {
	return m == SyncModeFull || m == SyncModeIncremental
}

// Watermark 复合水位 (Value, ID)。
// 只用时间列做增量会在同一时刻存在多条记录时翻页丢数据：
// 用 > 会漏掉同一时刻的其余记录，用 >= 又会无限重复该时刻。
// 因此以自增 id 作为第二排序键打破并列，边界重复由目标端幂等 upsert 吸收。
type Watermark struct {
	Value string `json:"value"`
	ID    int64  `json:"id"`
}

func (w Watermark) IsZero() bool {
	return w.Value == "" && w.ID == 0
}

// SourceConfig 源端配置
type SourceConfig struct {
	DSN   string `json:"dsn"`
	Table string `json:"table"`
	// Filter 行级过滤条件，直接拼进 WHERE，例如 "is_deleted = 0 AND status = 'active'"
	Filter string `json:"filter,omitempty"`
}

// SinkConfig 目标端配置
type SinkConfig struct {
	DSN   string `json:"dsn"`
	Table string `json:"table"`
	// UniqueKey 幂等写入依据，必须是业务唯一键而非自增主键。
	// 跨库同步不能用自增 id：源库和目标库的自增序列互不相干。
	UniqueKey []string `json:"unique_key"`
	// Filter 仅用于对账时统计目标端行数，把范围收敛到本作业负责的数据
	Filter string `json:"filter,omitempty"`
}

// TransformOp 字段级转换算子
type TransformOp string

const (
	// OpCopy 原样复制
	OpCopy TransformOp = "copy"
	// OpMaskPhone 手机号脱敏，保留前 3 后 4
	OpMaskPhone TransformOp = "mask_phone"
	// OpSHA256 取 hash，用于分析库去重统计
	OpSHA256 TransformOp = "sha256"
	// OpSyncNow 写入当前同步时间，无源列
	OpSyncNow TransformOp = "sync_now"
	// OpConst 写入固定值，用于目标端有默认值要求但源端没有对应列的场景
	OpConst TransformOp = "const"
	// OpCast 类型转换，源库与目标库列类型不一致时用
	OpCast TransformOp = "cast"
)

// CastType OpCast 支持的目标类型，刻意只留最常用的几种，
// 字段映射不需要长成一个规则引擎
type CastType string

const (
	CastInt    CastType = "int"
	CastFloat  CastType = "float"
	CastBool   CastType = "bool"
	CastString CastType = "string"
)

func (c CastType) Valid() bool {
	switch c {
	case CastInt, CastFloat, CastBool, CastString:
		return true
	}
	return false
}

// TransformRule 一条源列到目标列的映射规则。
// 只有出现在规则里的目标列才会被写入，因此目标端自有业务字段
// （如营销系统的 send_status / unsubscribed）天然不会被同步覆盖。
type TransformRule struct {
	From string      `json:"from,omitempty"`
	To   string      `json:"to"`
	Op   TransformOp `json:"op"`
	// Value 供 OpConst 使用，写入目标列的固定值
	Value string `json:"value,omitempty"`
	// CastTo 供 OpCast 使用，目标类型
	CastTo CastType `json:"cast_to,omitempty"`
}

// Job 用户配置的同步作业
type Job struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`

	// 溯源三元组的前两段，例如 crm / customer
	SourceSystem string `json:"source_system"`
	ObjectType   string `json:"object_type"`
	// SourceIDColumn 源记录标识列，构成三元组第三段，例如 customer_no
	SourceIDColumn string `json:"source_id_column"`

	SourceType   string       `json:"source_type"`
	SourceConfig SourceConfig `json:"source_config"`
	SinkType     string       `json:"sink_type"`
	SinkConfig   SinkConfig   `json:"sink_config"`

	Rules []TransformRule `json:"rules"`

	SyncMode        SyncMode  `json:"sync_mode"`
	WatermarkColumn string    `json:"watermark_column"`
	Watermark       Watermark `json:"watermark"`

	ShardColumn string `json:"shard_column"`
	ShardCount  int    `json:"shard_count"`

	BatchSize int `json:"batch_size"`
	// ReadQPS 源库读取限流（每秒批次数），0 表示不限，避免打爆 CRM 生产库
	ReadQPS int `json:"read_qps"`

	Priority string `json:"priority"`
	Enabled  bool   `json:"enabled"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Shard 一个分片区间 [Lo, Hi)，Hi 为空表示无上界
type Shard struct {
	Index int    `json:"index"`
	Lo    string `json:"lo"`
	Hi    string `json:"hi"`
}
