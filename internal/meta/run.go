package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
)

const runColumns = `id, job_id, trigger_type, status, sync_mode,
	watermark_from, watermark_from_id, watermark_to, watermark_to_id,
	shard_total, shard_done, shard_failed,
	rows_read, rows_written, rows_failed,
	error_reason, started_at, finished_at`

func (s *Store) CreateRun(ctx context.Context, r *model.JobRun) error {
	const q = `INSERT INTO job_runs (` + runColumns + `)
		VALUES (?,?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?, ?,?,?)`

	_, err := s.db.ExecContext(ctx, q,
		r.ID, r.JobID, string(r.TriggerType), string(r.Status), string(r.SyncMode),
		r.WatermarkFrom.Value, r.WatermarkFrom.ID, r.WatermarkTo.Value, r.WatermarkTo.ID,
		r.ShardTotal, r.ShardDone, r.ShardFailed,
		r.RowsRead, r.RowsWritten, r.RowsFailed,
		truncate(r.ErrorReason, 1024), r.StartedAt, nullTime(r.FinishedAt),
	)
	if err != nil {
		return fmt.Errorf("meta: insert run: %w", err)
	}
	return nil
}

func (s *Store) GetRun(ctx context.Context, id string) (*model.JobRun, error) {
	const q = `SELECT ` + runColumns + ` FROM job_runs WHERE id=?`
	return scanRun(s.db.QueryRowContext(ctx, q, id))
}

func (s *Store) ListRunsByJob(ctx context.Context, jobID string, limit int) ([]model.JobRun, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	const q = `SELECT ` + runColumns + ` FROM job_runs
		WHERE job_id=? ORDER BY started_at DESC LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, jobID, limit)
	if err != nil {
		return nil, fmt.Errorf("meta: list runs: %w", err)
	}
	defer rows.Close()

	var out []model.JobRun
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// SetRunShardTotal 在 Split 完成、分片任务落库后记录分片总数
func (s *Store) SetRunShardTotal(ctx context.Context, runID string, total int) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE job_runs SET shard_total=? WHERE id=?`, total, runID)
	if err != nil {
		return fmt.Errorf("meta: set run shard_total: %w", err)
	}
	return nil
}

// RunProgress 一次执行的分片聚合结果
type RunProgress struct {
	Total  int
	Done   int
	Failed int

	RowsRead    int64
	RowsWritten int64
	RowsFailed  int64
}

// AggregateRun 从 sync_tasks 聚合出执行进度。
//
// 刻意不用"增量计数器累加"：多个 worker 并发结束分片时累加会漂移，
// 重试与重入队还会让同一分片被计多次。每次重新聚合是幂等的，
// 无论调用多少次、什么顺序，结果都收敛到同一个真相。
func (s *Store) AggregateRun(ctx context.Context, runID string) (RunProgress, error) {
	// 终态名用占位符绑定 model 常量，不在 SQL 里写死字符串，
	// 这样"哪些状态算完成、哪些算失败"仍然只有领域层一个出处
	const q = `SELECT
			COUNT(*)                          AS total,
			COALESCE(SUM(status = ?), 0)      AS done,
			COALESCE(SUM(status = ?), 0)      AS failed,
			COALESCE(SUM(rows_read), 0)       AS rows_read,
			COALESCE(SUM(rows_written), 0)    AS rows_written,
			COALESCE(SUM(rows_failed), 0)     AS rows_failed
		FROM sync_tasks WHERE run_id = ?`

	var p RunProgress
	err := s.db.QueryRowContext(ctx, q,
		string(model.StatusSuccess), string(model.StatusDead), runID,
	).Scan(
		&p.Total, &p.Done, &p.Failed,
		&p.RowsRead, &p.RowsWritten, &p.RowsFailed,
	)
	if err != nil {
		return p, fmt.Errorf("meta: aggregate run: %w", err)
	}
	return p, nil
}

// ConvergeRun 每个分片结束后调用，把执行进度与状态收敛到当前真相。
//
// 状态由 model.DeriveRunStatus 在领域层推导，不再写在 SQL 的 CASE 里；
// 落库时用 WHERE status=<刚读到的状态> 防并发覆盖。
func (s *Store) ConvergeRun(ctx context.Context, runID string) (*model.JobRun, error) {
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	prog, err := s.AggregateRun(ctx, runID)
	if err != nil {
		return nil, err
	}

	// 统计字段与状态无关，任何时候都可以直接刷新
	const updStats = `UPDATE job_runs
		SET shard_done=?, shard_failed=?, rows_read=?, rows_written=?, rows_failed=?
		WHERE id=?`
	if _, err := s.db.ExecContext(ctx, updStats,
		prog.Done, prog.Failed, prog.RowsRead, prog.RowsWritten, prog.RowsFailed, runID,
	); err != nil {
		return nil, fmt.Errorf("meta: update run stats: %w", err)
	}

	derived := model.DeriveRunStatus(prog.Total, prog.Done, prog.Failed)
	if derived == run.Status {
		// 还在跑，或者已经是这个终态，无需转换
		return s.GetRun(ctx, runID)
	}

	// 影响 0 行说明另一个分片抢先收敛了，重新读一次即为最新状态，
	// 这是并发竞争而不是错误
	if _, err := s.applyRunTransition(ctx, runID, run.Status, derived,
		"finished_at=NOW(3)"); err != nil {
		// 已到终态又推导出别的终态：分片终态不可逆，正常情况不该发生，
		// 说明数据被外部改动或有真实 bug，直接暴露出来
		return nil, err
	}
	return s.GetRun(ctx, runID)
}

// applyRunTransition 是 JobRun 状态转换的唯一入口，与分片侧同构：
// 领域层状态机判合法性，WHERE status=? 防并发覆盖。
// 返回是否真的改动了行，供调用方区分"我改的"与"别人抢先改的"。
func (s *Store) applyRunTransition(ctx context.Context, runID string, from, to model.Status, setFragment string, setArgs ...any) (bool, error) {
	if err := model.CheckRunTransition(from, to); err != nil {
		return false, err
	}

	var sb strings.Builder
	sb.WriteString("UPDATE job_runs SET status=?")
	if setFragment != "" {
		sb.WriteString(", ")
		sb.WriteString(setFragment)
	}
	sb.WriteString(" WHERE id=? AND status=?")

	args := make([]any, 0, len(setArgs)+3)
	args = append(args, string(to))
	args = append(args, setArgs...)
	args = append(args, runID, string(from))

	res, err := s.db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return false, fmt.Errorf("meta: run transition %s -> %s: %w", from, to, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// FailRun 直接把整个 Run 判失败，用于 Split 阶段就出错、分片还没建出来的情况
func (s *Store) FailRun(ctx context.Context, runID, reason string) error {
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	// 已是终态就当成幂等成功，不去覆盖已有结论
	if model.IsRunTerminal(run.Status) {
		return nil
	}

	_, err = s.applyRunTransition(ctx, runID, run.Status, model.StatusFailed,
		"error_reason=?, finished_at=NOW(3)", truncate(reason, 1024))
	return err
}

func scanRun(sc rowScanner) (*model.JobRun, error) {
	var (
		r           model.JobRun
		triggerType string
		status      string
		syncMode    string
		finishedAt  sql.NullTime
	)

	err := sc.Scan(
		&r.ID, &r.JobID, &triggerType, &status, &syncMode,
		&r.WatermarkFrom.Value, &r.WatermarkFrom.ID, &r.WatermarkTo.Value, &r.WatermarkTo.ID,
		&r.ShardTotal, &r.ShardDone, &r.ShardFailed,
		&r.RowsRead, &r.RowsWritten, &r.RowsFailed,
		&r.ErrorReason, &r.StartedAt, &finishedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("meta: scan run: %w", err)
	}

	r.TriggerType = model.TriggerType(triggerType)
	r.Status = model.Status(status)
	r.SyncMode = model.SyncMode(syncMode)
	r.FinishedAt = timePtr(finishedAt)

	return &r, nil
}
