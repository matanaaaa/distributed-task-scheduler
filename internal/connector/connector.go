// Package connector 定义同步平台的源端与目标端抽象。
//
// 一个同步作业被拆成 source -> transform -> sink 三段。
// 加新数据源只需实现 Source，加新目标只需实现 Sink，
// 分片、断点、重试、对账这些机制不用动。
package connector

import (
	"context"
	"fmt"
	"sync"

	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
)

// Row 一行数据，按列名索引。
// 用 map 而不是 []any + 列名切片：转换层要按列名做映射与脱敏，
// 命名访问让规则配置直接对得上，代价是每行多一次哈希，
// 在批量 1000 行的量级下可以接受。
type Row map[string]any

// Batch 一批数据
type Batch struct {
	Rows []Row
	// Cursor 本批最后一行的位置，成功写入后用它推进 checkpoint
	Cursor model.Watermark
}

func (b *Batch) Len() int {
	if b == nil {
		return 0
	}
	return len(b.Rows)
}

// RowReader 分片读取游标。
//
// 刻意不用 channel：channel 版本要额外处理 goroutine 生命周期、
// 错误如何随流传递、ctx 取消后如何保证不泄漏。游标式接口把这些
// 都变成同步调用，错误直接返回，Close 释放资源，容易推理得多。
type RowReader interface {
	// Next 读下一批，读完返回 (nil, nil)
	Next(ctx context.Context) (*Batch, error)
	Close() error
}

// ReadRange 本次执行要同步的数据窗口 (From, To]。
//
// 窗口在 Run 开始时算一次，之后所有分片共用同一个窗口。
// 如果让每个分片各自去取"当前最大水位"，同步期间新写入的数据会让
// 各分片看到不同范围，既算不清对账，也说不准水位该推到哪。
type ReadRange struct {
	// From 下界，不含。零值表示不限（全量或首次增量）
	From model.Watermark
	// To 上界，含。零值表示不限
	To model.Watermark
}

// Source 数据源
type Source interface {
	// Split 在给定窗口内把作业切成若干分片，供多个 worker 并行拉取
	Split(ctx context.Context, job *model.Job, rng ReadRange) ([]model.Shard, error)

	// Open 打开一个分片的读取游标。
	// resume 为断点续跑起点，零值表示从分片头开始。
	Open(ctx context.Context, job *model.Job, shard model.Shard, rng ReadRange, resume model.Watermark) (RowReader, error)

	// Count 统计窗口内的源端记录数，用于对账
	Count(ctx context.Context, job *model.Job, rng ReadRange) (int64, error)

	// OpenSorted 按业务键升序读取窗口内全部行，供对账做归并比较。
	// 与 Open 的区别：不分片，且排序键是业务键而非分片列。
	OpenSorted(ctx context.Context, job *model.Job, rng ReadRange) (RowReader, error)

	// CurrentWatermark 读取源端当前最大水位，作为本次窗口的上界
	CurrentWatermark(ctx context.Context, job *model.Job) (model.Watermark, error)

	Close() error
}

// BadRow 一条写入失败且不值得重试的坏数据。
//
// 只带批内下标而不带源记录标识：Sink 拿到的是转换后的目标行，
// 里面未必还有源端的业务主键。由管道用 Index 回查源行取标识，
// 各层职责不越界。
type BadRow struct {
	Index int
	Row   Row
	Err   error
}

// WriteResult 一批写入的结果
type WriteResult struct {
	Written int64
	Failed  int64
	// BadRows 非重试类错误的行，由管道落到 sync_error_records
	BadRows []BadRow
}

// Sink 数据目标
type Sink interface {
	// Write 幂等批量写入。同一批重复写入不产生重复数据。
	Write(ctx context.Context, job *model.Job, batch *Batch) (WriteResult, error)

	// Count 统计目标端记录数，用于对账
	Count(ctx context.Context, job *model.Job) (int64, error)

	// OpenSorted 按业务唯一键升序读取目标端指定列，供对账做归并比较
	OpenSorted(ctx context.Context, job *model.Job, columns []string) (RowReader, error)

	Close() error
}

// SourceFactory 按配置构造 Source
type SourceFactory func(cfg model.SourceConfig) (Source, error)

// SinkFactory 按配置构造 Sink
type SinkFactory func(cfg model.SinkConfig) (Sink, error)

var (
	regMu     sync.RWMutex
	sourceReg = map[string]SourceFactory{}
	sinkReg   = map[string]SinkFactory{}
)

// RegisterSource 注册一种源类型，由具体实现包在 init 中调用
func RegisterSource(typ string, f SourceFactory) {
	regMu.Lock()
	defer regMu.Unlock()
	if _, dup := sourceReg[typ]; dup {
		panic("connector: duplicate source type " + typ)
	}
	sourceReg[typ] = f
}

// RegisterSink 注册一种目标类型
func RegisterSink(typ string, f SinkFactory) {
	regMu.Lock()
	defer regMu.Unlock()
	if _, dup := sinkReg[typ]; dup {
		panic("connector: duplicate sink type " + typ)
	}
	sinkReg[typ] = f
}

// NewSource 按类型构造源
func NewSource(typ string, cfg model.SourceConfig) (Source, error) {
	regMu.RLock()
	f, ok := sourceReg[typ]
	regMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("connector: unknown source type %q", typ)
	}
	return f(cfg)
}

// NewSink 按类型构造目标
func NewSink(typ string, cfg model.SinkConfig) (Sink, error) {
	regMu.RLock()
	f, ok := sinkReg[typ]
	regMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("connector: unknown sink type %q", typ)
	}
	return f(cfg)
}

// RegisteredSources 已注册的源类型，供 API 暴露能力清单
func RegisteredSources() []string {
	regMu.RLock()
	defer regMu.RUnlock()
	out := make([]string, 0, len(sourceReg))
	for k := range sourceReg {
		out = append(out, k)
	}
	return out
}

// RegisteredSinks 已注册的目标类型
func RegisteredSinks() []string {
	regMu.RLock()
	defer regMu.RUnlock()
	out := make([]string, 0, len(sinkReg))
	for k := range sinkReg {
		out = append(out, k)
	}
	return out
}
