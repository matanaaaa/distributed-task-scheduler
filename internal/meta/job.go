package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
)

const jobColumns = `id, name, description,
	source_system, object_type, source_id_column,
	source_type, source_config, sink_type, sink_config, transform_config,
	sync_mode, watermark_column, watermark_value, watermark_id,
	shard_column, shard_count, batch_size, read_qps,
	priority, enabled, created_at, updated_at`

// transformDoc 是 transform_config 列的 JSON 包装，
// 用对象而非裸数组，方便以后加别的转换级配置
type transformDoc struct {
	Rules []model.TransformRule `json:"rules"`
}

func (s *Store) CreateJob(ctx context.Context, j *model.Job) error {
	srcCfg, err := json.Marshal(j.SourceConfig)
	if err != nil {
		return fmt.Errorf("meta: marshal source_config: %w", err)
	}
	sinkCfg, err := json.Marshal(j.SinkConfig)
	if err != nil {
		return fmt.Errorf("meta: marshal sink_config: %w", err)
	}
	rules, err := json.Marshal(transformDoc{Rules: j.Rules})
	if err != nil {
		return fmt.Errorf("meta: marshal transform_config: %w", err)
	}

	const q = `INSERT INTO jobs (` + jobColumns + `)
		VALUES (?,?,?, ?,?,?, ?,?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?)`

	_, err = s.db.ExecContext(ctx, q,
		j.ID, j.Name, j.Description,
		j.SourceSystem, j.ObjectType, j.SourceIDColumn,
		j.SourceType, srcCfg, j.SinkType, sinkCfg, rules,
		string(j.SyncMode), j.WatermarkColumn, j.Watermark.Value, j.Watermark.ID,
		j.ShardColumn, j.ShardCount, j.BatchSize, j.ReadQPS,
		j.Priority, j.Enabled, j.CreatedAt, j.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("meta: insert job: %w", err)
	}
	return nil
}

func (s *Store) UpdateJob(ctx context.Context, j *model.Job) error {
	srcCfg, err := json.Marshal(j.SourceConfig)
	if err != nil {
		return fmt.Errorf("meta: marshal source_config: %w", err)
	}
	sinkCfg, err := json.Marshal(j.SinkConfig)
	if err != nil {
		return fmt.Errorf("meta: marshal sink_config: %w", err)
	}
	rules, err := json.Marshal(transformDoc{Rules: j.Rules})
	if err != nil {
		return fmt.Errorf("meta: marshal transform_config: %w", err)
	}

	const q = `UPDATE jobs SET
		name=?, description=?,
		source_system=?, object_type=?, source_id_column=?,
		source_type=?, source_config=?, sink_type=?, sink_config=?, transform_config=?,
		sync_mode=?, watermark_column=?,
		shard_column=?, shard_count=?, batch_size=?, read_qps=?,
		priority=?, enabled=?, updated_at=?
		WHERE id=?`

	res, err := s.db.ExecContext(ctx, q,
		j.Name, j.Description,
		j.SourceSystem, j.ObjectType, j.SourceIDColumn,
		j.SourceType, srcCfg, j.SinkType, sinkCfg, rules,
		string(j.SyncMode), j.WatermarkColumn,
		j.ShardColumn, j.ShardCount, j.BatchSize, j.ReadQPS,
		j.Priority, j.Enabled, time.Now().UTC(),
		j.ID,
	)
	if err != nil {
		return fmt.Errorf("meta: update job: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// 可能是 id 不存在，也可能是内容完全没变；用一次存在性检查区分
		if _, gerr := s.GetJob(ctx, j.ID); gerr != nil {
			return gerr
		}
	}
	return nil
}

func (s *Store) GetJob(ctx context.Context, id string) (*model.Job, error) {
	const q = `SELECT ` + jobColumns + ` FROM jobs WHERE id=?`
	return scanJob(s.db.QueryRowContext(ctx, q, id))
}

func (s *Store) GetJobByName(ctx context.Context, name string) (*model.Job, error) {
	const q = `SELECT ` + jobColumns + ` FROM jobs WHERE name=?`
	return scanJob(s.db.QueryRowContext(ctx, q, name))
}

func (s *Store) ListJobs(ctx context.Context, enabledOnly bool) ([]model.Job, error) {
	q := `SELECT ` + jobColumns + ` FROM jobs`
	if enabledOnly {
		q += ` WHERE enabled=1`
	}
	q += ` ORDER BY created_at DESC`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("meta: list jobs: %w", err)
	}
	defer rows.Close()

	var out []model.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

func (s *Store) DeleteJob(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE id=?`, id)
	if err != nil {
		return fmt.Errorf("meta: delete job: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) SetJobEnabled(ctx context.Context, id string, enabled bool) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET enabled=?, updated_at=? WHERE id=?`,
		enabled, time.Now().UTC(), id)
	if err != nil {
		return fmt.Errorf("meta: set job enabled: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		if _, gerr := s.GetJob(ctx, id); gerr != nil {
			return gerr
		}
	}
	return nil
}

// AdvanceWatermark 推进作业水位，只在整个 JobRun 成功后调用。
//
// WHERE 条件里带了"只能往前"的保护：一个跑得慢的旧 Run 晚于新 Run 结束时，
// 不能把水位往回拽，否则会造成重复同步甚至漏数据。
func (s *Store) AdvanceWatermark(ctx context.Context, jobID string, wm model.Watermark) error {
	const q = `UPDATE jobs SET watermark_value=?, watermark_id=?, updated_at=?
		WHERE id=?
		  AND (watermark_value < ? OR (watermark_value = ? AND watermark_id < ?))`

	_, err := s.db.ExecContext(ctx, q,
		wm.Value, wm.ID, time.Now().UTC(),
		jobID,
		wm.Value, wm.Value, wm.ID,
	)
	if err != nil {
		return fmt.Errorf("meta: advance watermark: %w", err)
	}
	// 影响 0 行是正常情况：说明水位已经不落后了，不视为错误
	return nil
}

// rowScanner 让 QueryRow 与 Rows 共用同一套扫描逻辑
type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(sc rowScanner) (*model.Job, error) {
	var (
		j        model.Job
		srcCfg   []byte
		sinkCfg  []byte
		rulesRaw []byte
		syncMode string
	)

	err := sc.Scan(
		&j.ID, &j.Name, &j.Description,
		&j.SourceSystem, &j.ObjectType, &j.SourceIDColumn,
		&j.SourceType, &srcCfg, &j.SinkType, &sinkCfg, &rulesRaw,
		&syncMode, &j.WatermarkColumn, &j.Watermark.Value, &j.Watermark.ID,
		&j.ShardColumn, &j.ShardCount, &j.BatchSize, &j.ReadQPS,
		&j.Priority, &j.Enabled, &j.CreatedAt, &j.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("meta: scan job: %w", err)
	}

	j.SyncMode = model.SyncMode(syncMode)

	if err := json.Unmarshal(srcCfg, &j.SourceConfig); err != nil {
		return nil, fmt.Errorf("meta: unmarshal source_config (job=%s): %w", j.ID, err)
	}
	if err := json.Unmarshal(sinkCfg, &j.SinkConfig); err != nil {
		return nil, fmt.Errorf("meta: unmarshal sink_config (job=%s): %w", j.ID, err)
	}
	if len(rulesRaw) > 0 {
		var doc transformDoc
		if err := json.Unmarshal(rulesRaw, &doc); err != nil {
			return nil, fmt.Errorf("meta: unmarshal transform_config (job=%s): %w", j.ID, err)
		}
		j.Rules = doc.Rules
	}

	return &j, nil
}
