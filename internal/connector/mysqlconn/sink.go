package mysqlconn

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
	"github.com/matanaaaa/distributed-task-scheduler/internal/syncerr"
)

// Sink 往 MySQL 表做幂等批量写入
type Sink struct {
	cfg model.SinkConfig
	db  *sql.DB
}

func NewSink(cfg model.SinkConfig) (*Sink, error) {
	if cfg.Table == "" {
		return nil, fmt.Errorf("mysqlconn: sink table is required")
	}
	if _, err := quoteIdent(cfg.Table); err != nil {
		return nil, err
	}
	if len(cfg.UniqueKey) == 0 {
		return nil, fmt.Errorf("mysqlconn: sink unique_key is required for idempotent upsert")
	}
	db, err := connector.SharedDB(driverName, cfg.DSN, 8)
	if err != nil {
		return nil, err
	}
	return &Sink{cfg: cfg, db: db}, nil
}

func (s *Sink) Close() error { return nil }

// Write 批量幂等 upsert。
//
// 幂等来自目标表上的业务唯一键 + ON DUPLICATE KEY UPDATE：
// 同一批数据重复写入只会覆盖成相同内容，不会产生重复行。
// 这是 at-least-once 投递能保证最终一致的前提——队列可以重复投，
// 写入端必须能吸收重复。
//
// SET 子句只更新映射列，不含唯一键本身，也不含目标端自有字段，
// 所以营销系统的 send_status / unsubscribed 不会被同步冲掉。
func (s *Sink) Write(ctx context.Context, job *model.Job, batch *connector.Batch) (connector.WriteResult, error) {
	var res connector.WriteResult
	if batch.Len() == 0 {
		return res, nil
	}

	stmt, err := s.buildUpsert(job, batch.Len())
	if err != nil {
		return res, err
	}
	cols := targetColumns(job)
	args := flattenArgs(cols, batch.Rows)

	if _, err := s.db.ExecContext(ctx, stmt, args...); err == nil {
		// MySQL 的 RowsAffected 对 upsert 语义特殊（插入记 1、更新记 2、无变化记 0），
		// 拿它当写入行数会失真，直接用批大小
		res.Written = int64(batch.Len())
		return res, nil
	} else if syncerr.IsRetryable(err) {
		// 基础设施类错误：整批交回上层重试，不逐行拆
		return res, fmt.Errorf("mysqlconn: batch upsert: %w", err)
	}

	// 非重试类错误：整批失败但不知道是哪几行有问题，
	// 逐行重写一遍把坏行隔离出来，好行照常落库。
	// 只在出错时才降级，正常路径仍是批量。
	return s.writeRowByRow(ctx, job, batch, cols)
}

// writeRowByRow 逐行写入以隔离坏数据
func (s *Sink) writeRowByRow(ctx context.Context, job *model.Job, batch *connector.Batch, cols []string) (connector.WriteResult, error) {
	var res connector.WriteResult

	stmt, err := s.buildUpsert(job, 1)
	if err != nil {
		return res, err
	}

	for i, row := range batch.Rows {
		args := flattenArgs(cols, []connector.Row{row})
		_, err := s.db.ExecContext(ctx, stmt, args...)
		if err == nil {
			res.Written++
			continue
		}
		if syncerr.IsRetryable(err) {
			// 逐行阶段又碰到基础设施问题：整批交回重试，
			// 已写入的行靠幂等 upsert 保证重跑安全
			return res, fmt.Errorf("mysqlconn: row upsert (index=%d): %w", i, err)
		}
		res.Failed++
		res.BadRows = append(res.BadRows, connector.BadRow{
			Index: i,
			Row:   row,
			Err:   err,
		})
	}
	return res, nil
}

// Count 统计目标端行数，用于对账
func (s *Sink) Count(ctx context.Context, job *model.Job) (int64, error) {
	tbl, err := quoteIdent(s.cfg.Table)
	if err != nil {
		return 0, err
	}
	filter := "1=1"
	if strings.TrimSpace(s.cfg.Filter) != "" {
		filter = "(" + s.cfg.Filter + ")"
	}

	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tbl, filter)

	var n int64
	if err := s.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("mysqlconn: sink count: %w", err)
	}
	return n, nil
}

// buildUpsert 拼出 n 行的 upsert 语句
func (s *Sink) buildUpsert(job *model.Job, n int) (string, error) {
	cols := targetColumns(job)
	if len(cols) == 0 {
		return "", fmt.Errorf("mysqlconn: job %s has no target columns", job.Name)
	}
	tbl, err := quoteIdent(s.cfg.Table)
	if err != nil {
		return "", err
	}
	quotedCols, err := quoteIdents(cols)
	if err != nil {
		return "", err
	}

	// 唯一键列不参与 UPDATE：它们是匹配依据，更新自己没有意义
	uk := make(map[string]struct{}, len(s.cfg.UniqueKey))
	for _, k := range s.cfg.UniqueKey {
		uk[k] = struct{}{}
	}

	sets := make([]string, 0, len(cols))
	for _, c := range cols {
		if _, isKey := uk[c]; isKey {
			continue
		}
		q, err := quoteIdent(c)
		if err != nil {
			return "", err
		}
		sets = append(sets, fmt.Sprintf("%s=VALUES(%s)", q, q))
	}
	if len(sets) == 0 {
		return "", fmt.Errorf("mysqlconn: job %s maps only unique key columns, nothing to update", job.Name)
	}

	rowPlaceholder := "(" + strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"
	values := strings.TrimSuffix(strings.Repeat(rowPlaceholder+",", n), ",")

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
		tbl, strings.Join(quotedCols, ", "), values, strings.Join(sets, ", "),
	), nil
}

// targetColumns 目标列取自转换规则的 To 字段。
//
// 刻意不从 Row 的 key 里取：map 遍历顺序随机，会导致每批生成的
// 列顺序不一致，参数就和列错位了。规则是有序切片，顺序稳定。
func targetColumns(job *model.Job) []string {
	out := make([]string, 0, len(job.Rules))
	for _, r := range job.Rules {
		out = append(out, r.To)
	}
	return out
}

// flattenArgs 按列顺序把多行摊平成参数列表
func flattenArgs(cols []string, rows []connector.Row) []any {
	args := make([]any, 0, len(cols)*len(rows))
	for _, row := range rows {
		for _, c := range cols {
			args = append(args, row[c])
		}
	}
	return args
}
