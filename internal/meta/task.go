package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
)

const taskColumns = `id, run_id, job_id,
	shard_index, shard_lo, shard_hi,
	status, priority, attempt, retry_count,
	checkpoint, checkpoint_id,
	rows_read, rows_written, rows_failed,
	error_reason, started_at, finished_at, created_at, updated_at`

// CreateTasks 批量落库分片任务。
// 单条多值 INSERT，避免 N 个分片打 N 次往返。
func (s *Store) CreateTasks(ctx context.Context, tasks []model.SyncTask) error {
	if len(tasks) == 0 {
		return nil
	}

	const cols = 20
	placeholder := "(" + strings.TrimSuffix(strings.Repeat("?,", cols), ",") + ")"

	var sb strings.Builder
	sb.WriteString(`INSERT INTO sync_tasks (` + taskColumns + `) VALUES `)
	args := make([]any, 0, len(tasks)*cols)

	for i := range tasks {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(placeholder)

		t := &tasks[i]
		args = append(args,
			t.ID, t.RunID, t.JobID,
			t.ShardIndex, t.ShardLo, t.ShardHi,
			string(t.Status), t.Priority, t.Attempt, t.RetryCount,
			t.Checkpoint.Value, t.Checkpoint.ID,
			t.RowsRead, t.RowsWritten, t.RowsFailed,
			truncate(t.ErrorReason, 1024), nullTime(t.StartedAt), nullTime(t.FinishedAt),
			t.CreatedAt, t.UpdatedAt,
		)
	}

	if _, err := s.db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("meta: insert tasks: %w", err)
	}
	return nil
}

func (s *Store) GetTask(ctx context.Context, id string) (*model.SyncTask, error) {
	const q = `SELECT ` + taskColumns + ` FROM sync_tasks WHERE id=?`
	return scanTask(s.db.QueryRowContext(ctx, q, id))
}

func (s *Store) ListTasksByRun(ctx context.Context, runID string) ([]model.SyncTask, error) {
	const q = `SELECT ` + taskColumns + ` FROM sync_tasks
		WHERE run_id=? ORDER BY shard_index`

	rows, err := s.db.QueryContext(ctx, q, runID)
	if err != nil {
		return nil, fmt.Errorf("meta: list tasks: %w", err)
	}
	defer rows.Close()

	var out []model.SyncTask
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}

// applyTaskTransition 是分片状态转换的唯一入口。
//
// 职责切分：
//   - 合法性由领域层状态机 model.CheckTaskTransition 判定，
//     转换规则只在 model/state.go 里维护一份
//   - 并发安全由 WHERE status=<刚读到的状态> 保证：期间被别人改过，
//     影响行数就是 0，说明发生了并发抢占而不是规则非法
//
// 两种失败因此可以区分：ErrIllegalTransition 是调用方逻辑错，
// ErrConcurrentUpdate 是竞争，通常重新读取状态后就不用再管。
func (s *Store) applyTaskTransition(ctx context.Context, id string, to model.Status, setFragment string, setArgs ...any) (*model.SyncTask, error) {
	cur, err := s.GetTask(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := model.CheckTaskTransition(cur.Status, to); err != nil {
		return nil, err
	}

	var sb strings.Builder
	sb.WriteString("UPDATE sync_tasks SET status=?, updated_at=NOW(3)")
	if setFragment != "" {
		sb.WriteString(", ")
		sb.WriteString(setFragment)
	}
	sb.WriteString(" WHERE id=? AND status=?")

	args := make([]any, 0, len(setArgs)+3)
	args = append(args, string(to))
	args = append(args, setArgs...)
	args = append(args, id, string(cur.Status))

	res, err := s.db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("meta: task transition %s -> %s: %w", cur.Status, to, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, fmt.Errorf("%w: task=%s expected=%s target=%s",
			ErrConcurrentUpdate, id, cur.Status, to)
	}

	return s.GetTask(ctx, id)
}

// ClaimTask 领取任务：queued -> running，attempt 加一。
//
// 条件更新保证同一分片只会被一个 worker 领走：并发场景下后到的那个
// 影响行数为 0，拿到 ErrNotClaimable 后直接跳过。
//
// 返回领取后的任务，其中 Checkpoint 即断点续跑起点。
func (s *Store) ClaimTask(ctx context.Context, id string) (*model.SyncTask, error) {
	t, err := s.applyTaskTransition(ctx, id, model.StatusRunning,
		"attempt = attempt + 1, started_at = NOW(3)")
	if err == nil {
		return t, nil
	}

	// 状态不对或被人抢先，对调用方都是"这个任务现在不该我跑"
	var illegal *model.ErrIllegalTransition
	if errors.As(err, &illegal) || errors.Is(err, ErrConcurrentUpdate) {
		return nil, fmt.Errorf("%w: %v", ErrNotClaimable, err)
	}
	return nil, err
}

// SaveProgress 运行过程中周期性写断点与行数，不改状态。
// Worker 中途被 kill 时，已提交的批次不会丢，重跑从 checkpoint 之后继续。
func (s *Store) SaveProgress(ctx context.Context, id string, ckpt model.Watermark, rowsRead, rowsWritten, rowsFailed int64) error {
	// 不改状态，只是把进度写下来，所以不走状态机；
	// WHERE status=running 是并发守卫：任务已被 watchdog 抢回时不该再写进度
	const q = `UPDATE sync_tasks
		SET checkpoint=?, checkpoint_id=?,
			rows_read=?, rows_written=?, rows_failed=?,
			updated_at=NOW(3)
		WHERE id=? AND status=?`

	_, err := s.db.ExecContext(ctx, q,
		ckpt.Value, ckpt.ID, rowsRead, rowsWritten, rowsFailed, id,
		string(model.StatusRunning))
	if err != nil {
		return fmt.Errorf("meta: save progress: %w", err)
	}
	return nil
}

// MarkTaskSuccess running -> success
func (s *Store) MarkTaskSuccess(ctx context.Context, id string, ckpt model.Watermark, rowsRead, rowsWritten, rowsFailed int64) error {
	_, err := s.applyTaskTransition(ctx, id, model.StatusSuccess,
		"error_reason='', checkpoint=?, checkpoint_id=?, rows_read=?, rows_written=?, rows_failed=?, finished_at=NOW(3)",
		ckpt.Value, ckpt.ID, rowsRead, rowsWritten, rowsFailed)
	return err
}

// MarkTaskFailed running -> failed
func (s *Store) MarkTaskFailed(ctx context.Context, id, reason string) error {
	_, err := s.applyTaskTransition(ctx, id, model.StatusFailed,
		"error_reason=?", truncate(reason, 1024))
	return err
}

// MarkTaskRetrying failed -> retrying，返回累计重试次数
func (s *Store) MarkTaskRetrying(ctx context.Context, id, reason string) (int, error) {
	t, err := s.applyTaskTransition(ctx, id, model.StatusRetrying,
		"retry_count=retry_count+1, error_reason=?", truncate(reason, 1024))
	if err != nil {
		return 0, err
	}
	return t.RetryCount, nil
}

// RequeueTask 重新入队。
// retrying -> queued 是退避后的正常重试；
// running  -> queued 是 watchdog 发现租约超时后的抢回。
// 两条路径都在状态机表里，这里不用自己判断来源。
func (s *Store) RequeueTask(ctx context.Context, id string) error {
	_, err := s.applyTaskTransition(ctx, id, model.StatusQueued, "")
	return err
}

// MarkTaskDead failed -> dead，重试次数耗尽或坏数据，已进 DLQ
func (s *Store) MarkTaskDead(ctx context.Context, id, reason string) error {
	_, err := s.applyTaskTransition(ctx, id, model.StatusDead,
		"error_reason=?, finished_at=NOW(3)", truncate(reason, 1024))
	return err
}

func scanTask(sc rowScanner) (*model.SyncTask, error) {
	var (
		t          model.SyncTask
		status     string
		startedAt  sql.NullTime
		finishedAt sql.NullTime
	)

	err := sc.Scan(
		&t.ID, &t.RunID, &t.JobID,
		&t.ShardIndex, &t.ShardLo, &t.ShardHi,
		&status, &t.Priority, &t.Attempt, &t.RetryCount,
		&t.Checkpoint.Value, &t.Checkpoint.ID,
		&t.RowsRead, &t.RowsWritten, &t.RowsFailed,
		&t.ErrorReason, &startedAt, &finishedAt, &t.CreatedAt, &t.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("meta: scan task: %w", err)
	}

	t.Status = model.Status(status)
	t.StartedAt = timePtr(startedAt)
	t.FinishedAt = timePtr(finishedAt)

	return &t, nil
}
