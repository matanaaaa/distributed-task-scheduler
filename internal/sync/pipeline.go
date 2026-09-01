// Package sync 是同步管道：读 -> 转换 -> 批量幂等写 -> 推进断点。
//
// 这里替代了原来的"解压任务包、跑 run.sh、打包 result.zip"执行链路。
package sync

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/meta"
	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
	"github.com/matanaaaa/distributed-task-scheduler/internal/syncerr"
	"github.com/matanaaaa/distributed-task-scheduler/internal/transform"
)

// Result 一个分片的执行结果
type Result struct {
	RowsRead    int64
	RowsWritten int64
	RowsFailed  int64
	Checkpoint  model.Watermark
}

// Pipeline 执行单个分片任务
type Pipeline struct {
	meta *meta.Store
	// badRowLimit 单个分片最多记录多少条坏数据，防止整批脏数据把元数据库灌满
	badRowLimit int
}

func NewPipeline(m *meta.Store) *Pipeline {
	return &Pipeline{meta: m, badRowLimit: 1000}
}

// RunTask 执行一个分片。
//
// 断点语义：每成功写入一批就把 checkpoint 推进到该批最后一行的位置。
// 进程被 kill 时已提交的批次不会重做——重新领取后从 checkpoint 之后继续。
// 边界上可能重复一批，由目标端幂等 upsert 吸收。
func (p *Pipeline) RunTask(ctx context.Context, job *model.Job, run *model.JobRun, task *model.SyncTask) (Result, error) {
	var res Result
	res.Checkpoint = task.Checkpoint

	src, err := connector.NewSource(job.SourceType, job.SourceConfig)
	if err != nil {
		return res, err
	}
	defer src.Close()

	sink, err := connector.NewSink(job.SinkType, job.SinkConfig)
	if err != nil {
		return res, err
	}
	defer sink.Close()

	rng := connector.ReadRange{From: run.WatermarkFrom, To: run.WatermarkTo}

	reader, err := src.Open(ctx, job, task.Shard(), rng, task.Checkpoint)
	if err != nil {
		return res, err
	}
	defer reader.Close()

	// 同一分片内所有 sync_now 用同一时刻，避免同批数据时间戳参差
	syncNow := time.Now().UTC()

	for {
		if err := ctx.Err(); err != nil {
			// 主动取消（关机）不是失败：已写入的批次和 checkpoint 都已落库，
			// 租约到期后 watchdog 会把这个分片重新入队接着跑
			return res, err
		}

		batch, err := reader.Next(ctx)
		if err != nil {
			return res, fmt.Errorf("read: %w", err)
		}
		if batch == nil {
			break
		}
		res.RowsRead += int64(batch.Len())

		outBatch, srcRows, badTransform := p.transformBatch(job, batch, syncNow)
		if len(badTransform) > 0 {
			res.RowsFailed += int64(len(badTransform))
			if err := p.recordBadRows(ctx, job, run, task, badTransform); err != nil {
				log.Printf("[sync] record transform bad rows failed: task=%s err=%v", task.ID, err)
			}
		}

		if outBatch.Len() > 0 {
			wr, err := sink.Write(ctx, job, outBatch)
			res.RowsWritten += wr.Written
			res.RowsFailed += wr.Failed

			if len(wr.BadRows) > 0 {
				recs := p.badRowsFromSink(job, run, task, wr.BadRows, srcRows)
				if rerr := p.recordBadRows(ctx, job, run, task, recs); rerr != nil {
					log.Printf("[sync] record sink bad rows failed: task=%s err=%v", task.ID, rerr)
				}
			}

			if err != nil {
				// 可重试错误：把已推进的进度先落库再抛出，
				// 这样重试能从断点继续而不是从分片头重来
				_ = p.meta.SaveProgress(ctx, task.ID, res.Checkpoint, res.RowsRead, res.RowsWritten, res.RowsFailed)
				return res, fmt.Errorf("write: %w", err)
			}
		}

		// 只有整批处理完才推进断点
		res.Checkpoint = batch.Cursor
		if err := p.meta.SaveProgress(ctx, task.ID, res.Checkpoint, res.RowsRead, res.RowsWritten, res.RowsFailed); err != nil {
			// 断点写不进去就必须停：继续跑下去一旦崩溃会重复同步一大段
			return res, fmt.Errorf("save checkpoint: %w", err)
		}
	}

	return res, nil
}

// transformBatch 把源批转换成目标批。
//
// 同时返回与目标批一一对应的源行切片：Sink 报坏行时只给批内下标，
// 要靠这个切片回查源端业务主键。转换失败的行不会进目标批，
// 所以两边下标必须在这里就对齐。
func (p *Pipeline) transformBatch(job *model.Job, batch *connector.Batch, syncNow time.Time) (*connector.Batch, []connector.Row, []model.SyncErrorRecord) {
	out := &connector.Batch{
		Rows:   make([]connector.Row, 0, batch.Len()),
		Cursor: batch.Cursor,
	}
	srcRows := make([]connector.Row, 0, batch.Len())
	var bad []model.SyncErrorRecord

	for _, srcRow := range batch.Rows {
		dst, err := transform.Apply(job.Rules, srcRow, syncNow)
		if err != nil {
			// 转换失败一定是配置或数据问题，重试无意义，直接记账跳过
			_, code := syncerr.Classify(err)
			bad = append(bad, model.SyncErrorRecord{
				SourceRecordID: p.sourceRecordID(job, srcRow),
				ErrorType:      model.ErrorNonRetryable,
				ErrorCode:      code,
				ErrorMsg:       err.Error(),
				RawRow:         rawRow(srcRow),
			})
			continue
		}
		out.Rows = append(out.Rows, dst)
		srcRows = append(srcRows, srcRow)
	}

	return out, srcRows, bad
}

// badRowsFromSink 把 Sink 报回的坏行下标翻译成带溯源信息的记录
func (p *Pipeline) badRowsFromSink(job *model.Job, run *model.JobRun, task *model.SyncTask, badRows []connector.BadRow, srcRows []connector.Row) []model.SyncErrorRecord {
	out := make([]model.SyncErrorRecord, 0, len(badRows))

	for _, br := range badRows {
		var srcRow connector.Row
		if br.Index >= 0 && br.Index < len(srcRows) {
			srcRow = srcRows[br.Index]
		}
		_, code := syncerr.Classify(br.Err)

		out = append(out, model.SyncErrorRecord{
			SourceRecordID: p.sourceRecordID(job, srcRow),
			ErrorType:      model.ErrorNonRetryable,
			ErrorCode:      code,
			ErrorMsg:       br.Err.Error(),
			RawRow:         rawRow(srcRow),
		})
	}
	return out
}

// recordBadRows 补齐上下文后落库
func (p *Pipeline) recordBadRows(ctx context.Context, job *model.Job, run *model.JobRun, task *model.SyncTask, recs []model.SyncErrorRecord) error {
	if len(recs) == 0 {
		return nil
	}
	if len(recs) > p.badRowLimit {
		recs = recs[:p.badRowLimit]
	}
	now := time.Now().UTC()
	for i := range recs {
		recs[i].RunID = run.ID
		recs[i].TaskID = task.ID
		recs[i].JobID = job.ID
		recs[i].SourceSystem = job.SourceSystem
		recs[i].ObjectType = job.ObjectType
		recs[i].CreatedAt = now
	}
	return p.meta.InsertErrorRecords(ctx, recs)
}

// sourceRecordID 取源记录的业务标识，用于溯源三元组
func (p *Pipeline) sourceRecordID(job *model.Job, row connector.Row) string {
	if row == nil || job.SourceIDColumn == "" {
		return ""
	}
	return transform.ToString(row[job.SourceIDColumn])
}

// rawRow 把源行转成可 JSON 序列化的形式，供人工修数后重放
func rawRow(row connector.Row) map[string]any {
	if row == nil {
		return nil
	}
	out := make(map[string]any, len(row))
	for k, v := range row {
		switch t := v.(type) {
		case []byte:
			out[k] = string(t)
		default:
			out[k] = t
		}
	}
	return out
}
