package sync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/meta"
	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
	"github.com/matanaaaa/distributed-task-scheduler/internal/transform"
)

// reconSampleLimit 差异键抽样上限
const reconSampleLimit = 100

// Reconciler 在 Run 结束后比对源端与目标端。
//
// 同步链路自己报告的 rows_written 只能说明"我写了多少"，
// 不能说明"目标端现在到底对不对"。对账是从两端各查一次真实数字，
// 这是唯一能发现静默丢数据的手段。
//
// 两档深度：
//
//	count 每次都跑，只比总数，最便宜，能抓住"整片分片没跑"这类问题
//	field 只在全量 Run 后跑，按业务键归并比对，输出 missing/extra/mismatch
//
// 增量 Run 不做深档：源端窗口里只有少量变更行，目标端是全量累计，
// 两者本来就不该逐键相等，比了也没有意义。
type Reconciler struct {
	meta *meta.Store
}

func NewReconciler(m *meta.Store) *Reconciler {
	return &Reconciler{meta: m}
}

// Check 执行对账并落库，返回最深那一档的结果
func (r *Reconciler) Check(ctx context.Context, job *model.Job, run *model.JobRun) (*model.Reconciliation, error) {
	src, err := connector.NewSource(job.SourceType, job.SourceConfig)
	if err != nil {
		return nil, err
	}
	defer src.Close()

	sink, err := connector.NewSink(job.SinkType, job.SinkConfig)
	if err != nil {
		return nil, err
	}
	defer sink.Close()

	countRC, err := r.checkCount(ctx, job, run, src, sink)
	if err != nil {
		return countRC, err
	}

	if job.SyncMode != model.SyncModeFull {
		return countRC, nil
	}

	fieldRC, err := r.checkFields(ctx, job, run, src, sink)
	if err != nil {
		// 深档失败不掩盖已经拿到的 count 结果
		log.Printf("[reconcile] field-level check failed: run=%s err=%v", run.ID, err)
		return countRC, err
	}
	return fieldRC, nil
}

// checkCount count 档：只比两端总数
func (r *Reconciler) checkCount(ctx context.Context, job *model.Job, run *model.JobRun, src connector.Source, sink connector.Sink) (*model.Reconciliation, error) {
	rc := r.newRecord(job, run, model.ReconCount)

	rng := connector.ReadRange{From: run.WatermarkFrom, To: run.WatermarkTo}

	srcCount, err := src.Count(ctx, job, rng)
	if err != nil {
		return r.fail(ctx, rc, fmt.Errorf("source count: %w", err))
	}
	tgtCount, err := sink.Count(ctx, job)
	if err != nil {
		return r.fail(ctx, rc, fmt.Errorf("target count: %w", err))
	}

	rc.SourceCount = srcCount
	rc.TargetCount = tgtCount

	// 只有全量模式两端总数才可比：增量的源端 count 只覆盖本次窗口，
	// 目标端是累计总量，直接相减没有意义，所以只记录数字不判定差异
	if job.SyncMode == model.SyncModeFull {
		if diff := srcCount - tgtCount; diff > 0 {
			rc.MissingCount = diff
			rc.Result = model.ReconMismatch
		} else if diff < 0 {
			rc.ExtraCount = -diff
			rc.Result = model.ReconMismatch
		}
		r.excuseBadRows(rc, run)
	}

	if err := r.meta.SaveReconciliation(ctx, rc); err != nil {
		return nil, err
	}
	return rc, nil
}

// checkFields field 档：按业务键归并比对，得到 missing / extra / mismatch
func (r *Reconciler) checkFields(ctx context.Context, job *model.Job, run *model.JobRun, src connector.Source, sink connector.Sink) (*model.Reconciliation, error) {
	rc := r.newRecord(job, run, model.ReconField)

	keyCol, err := reconKeyColumn(job)
	if err != nil {
		return r.fail(ctx, rc, err)
	}
	fields := comparableFields(job)
	if len(fields) == 0 {
		return r.fail(ctx, rc, fmt.Errorf("no comparable fields: all transform rules are non-deterministic"))
	}

	rng := connector.ReadRange{From: run.WatermarkFrom, To: run.WatermarkTo}

	srcReader, err := src.OpenSorted(ctx, job, rng)
	if err != nil {
		return r.fail(ctx, rc, fmt.Errorf("open source stream: %w", err))
	}
	defer srcReader.Close()

	tgtReader, err := sink.OpenSorted(ctx, job, fields)
	if err != nil {
		return r.fail(ctx, rc, fmt.Errorf("open target stream: %w", err))
	}
	defer tgtReader.Close()

	// 源端要先过一遍转换再算指纹：目标端存的是转换后的值
	// （phone 已脱敏、类型已转换），拿原始值比一定全都 mismatch
	srcIter := r.transformedIter(ctx, srcReader, job, keyCol, fields)
	tgtIter := plainIter(ctx, tgtReader, keyCol, fields)

	diff, err := mergeCompare(srcIter, tgtIter, reconSampleLimit)
	if err != nil {
		return r.fail(ctx, rc, err)
	}

	rc.MissingCount = diff.Missing
	rc.ExtraCount = diff.Extra
	rc.MismatchCount = diff.Mismatch
	if diff.Missing > 0 || diff.Extra > 0 || diff.Mismatch > 0 {
		rc.Result = model.ReconMismatch
		rc.Detail = &model.ReconDetail{
			MissingKeys:  diff.MissingKeys,
			ExtraKeys:    diff.ExtraKeys,
			MismatchKeys: diff.MismatchKeys,
			Truncated:    diff.Truncated,
		}
	}
	r.excuseBadRows(rc, run)

	if err := r.meta.SaveReconciliation(ctx, rc); err != nil {
		return nil, err
	}
	return rc, nil
}

// transformedIter 源端迭代器：转换后再算指纹
func (r *Reconciler) transformedIter(ctx context.Context, reader connector.RowReader, job *model.Job, keyCol string, fields []string) keyIter {
	// 对账只比内容，不关心同步时刻，用固定零值时间避免 sync_now 干扰
	var fixedTime time.Time
	return batchedIter(ctx, reader, func(row connector.Row) (*keyChecksum, error) {
		dst, err := transform.Apply(job.Rules, row, fixedTime)
		if err != nil {
			return nil, fmt.Errorf("transform during reconciliation: %w", err)
		}
		return &keyChecksum{
			Key:      transform.ToString(dst[keyCol]),
			Checksum: checksumOf(dst, fields),
		}, nil
	})
}

// plainIter 目标端迭代器：直接算指纹
func plainIter(ctx context.Context, reader connector.RowReader, keyCol string, fields []string) keyIter {
	return batchedIter(ctx, reader, func(row connector.Row) (*keyChecksum, error) {
		return &keyChecksum{
			Key:      transform.ToString(row[keyCol]),
			Checksum: checksumOf(row, fields),
		}, nil
	})
}

// batchedIter 把批量游标适配成逐行拉取的 keyIter
func batchedIter(ctx context.Context, reader connector.RowReader, conv func(connector.Row) (*keyChecksum, error)) keyIter {
	var (
		buf []connector.Row
		pos int
	)
	return func() (*keyChecksum, error) {
		for pos >= len(buf) {
			batch, err := reader.Next(ctx)
			if err != nil {
				return nil, err
			}
			if batch == nil {
				return nil, nil
			}
			buf, pos = batch.Rows, 0
			if len(buf) == 0 {
				// 空批次不代表结束，继续拉下一批
				continue
			}
		}
		row := buf[pos]
		pos++
		return conv(row)
	}
}

// checksumOf 按固定字段顺序算行指纹。
//
// 顺序必须稳定，所以字段列表由调用方按规则顺序传入，
// 绝不能遍历 map 的 key —— 那样两端指纹永远对不上。
func checksumOf(row connector.Row, fields []string) string {
	var sb strings.Builder
	for _, f := range fields {
		v, ok := row[f]
		if !ok || v == nil {
			// 用不可能出现在正常值里的记号区分 NULL 与空串，
			// 否则 NULL 和 '' 会被判成一致
			sb.WriteString("\x00NULL\x00")
		} else {
			sb.WriteString(transform.ToString(v))
		}
		sb.WriteString("\x1f")
	}
	sum := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(sum[:])
}

// comparableFields 参与指纹比较的目标列。
//
// 排除 sync_now：它每次执行都不同，比了必然 mismatch。
// 其余映射列都参与，包括脱敏和类型转换后的值。
func comparableFields(job *model.Job) []string {
	out := make([]string, 0, len(job.Rules))
	for _, rule := range job.Rules {
		if rule.Op == model.OpSyncNow {
			continue
		}
		out = append(out, rule.To)
	}
	return out
}

// reconKeyColumn 对账用的业务键列，取目标端唯一键
func reconKeyColumn(job *model.Job) (string, error) {
	if len(job.SinkConfig.UniqueKey) != 1 {
		return "", fmt.Errorf("field-level reconciliation requires exactly one unique_key column, got %v",
			job.SinkConfig.UniqueKey)
	}
	return job.SinkConfig.UniqueKey[0], nil
}

// excuseBadRows 本次被跳过的坏数据会造成已知的数量缺口，不算异常
func (r *Reconciler) excuseBadRows(rc *model.Reconciliation, run *model.JobRun) {
	if rc.Result != model.ReconMismatch || run.RowsFailed <= 0 {
		return
	}
	if rc.ExtraCount > 0 || rc.MismatchCount > 0 {
		// 多余行和字段不一致跟坏数据无关，不能用坏数据解释掉
		return
	}
	if rc.MissingCount > run.RowsFailed {
		return
	}
	rc.Result = model.ReconOK
	rc.ErrorReason = fmt.Sprintf(
		"missing %d rows explained by %d bad rows recorded in sync_error_records",
		rc.MissingCount, run.RowsFailed)
}

func (r *Reconciler) newRecord(job *model.Job, run *model.JobRun, mode model.ReconMode) *model.Reconciliation {
	return &model.Reconciliation{
		ID:        uuid.NewString(),
		RunID:     run.ID,
		JobID:     job.ID,
		Mode:      mode,
		Result:    model.ReconOK,
		CheckedAt: time.Now().UTC(),
	}
}

func (r *Reconciler) fail(ctx context.Context, rc *model.Reconciliation, cause error) (*model.Reconciliation, error) {
	rc.Result = model.ReconError
	rc.ErrorReason = cause.Error()
	if err := r.meta.SaveReconciliation(ctx, rc); err != nil {
		return nil, err
	}
	return rc, cause
}
