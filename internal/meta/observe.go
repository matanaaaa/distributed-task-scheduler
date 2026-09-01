package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
)

const reconColumns = `id, run_id, job_id, mode,
	source_count, target_count, missing_count, extra_count, mismatch_count,
	result, error_reason, detail, checked_at`

// SaveReconciliation 写入对账结果。
// 按 (run_id, mode) 做 upsert，同一次 Run 重复对账会覆盖而不是堆积。
func (s *Store) SaveReconciliation(ctx context.Context, rc *model.Reconciliation) error {
	var detail any
	if rc.Detail != nil {
		b, err := json.Marshal(rc.Detail)
		if err != nil {
			return fmt.Errorf("meta: marshal recon detail: %w", err)
		}
		detail = b
	}

	const q = `INSERT INTO reconciliations (` + reconColumns + `)
		VALUES (?,?,?,?, ?,?,?,?,?, ?,?,?,?)
		ON DUPLICATE KEY UPDATE
			source_count   = VALUES(source_count),
			target_count   = VALUES(target_count),
			missing_count  = VALUES(missing_count),
			extra_count    = VALUES(extra_count),
			mismatch_count = VALUES(mismatch_count),
			result         = VALUES(result),
			error_reason   = VALUES(error_reason),
			detail         = VALUES(detail),
			checked_at     = VALUES(checked_at)`

	_, err := s.db.ExecContext(ctx, q,
		rc.ID, rc.RunID, rc.JobID, string(rc.Mode),
		rc.SourceCount, rc.TargetCount, rc.MissingCount, rc.ExtraCount, rc.MismatchCount,
		string(rc.Result), truncate(rc.ErrorReason, 1024), detail, rc.CheckedAt,
	)
	if err != nil {
		return fmt.Errorf("meta: save reconciliation: %w", err)
	}
	return nil
}

func (s *Store) ListReconciliationsByRun(ctx context.Context, runID string) ([]model.Reconciliation, error) {
	const q = `SELECT ` + reconColumns + ` FROM reconciliations WHERE run_id=? ORDER BY mode`

	rows, err := s.db.QueryContext(ctx, q, runID)
	if err != nil {
		return nil, fmt.Errorf("meta: list reconciliations: %w", err)
	}
	defer rows.Close()

	var out []model.Reconciliation
	for rows.Next() {
		rc, err := scanRecon(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rc)
	}
	return out, rows.Err()
}

func scanRecon(sc rowScanner) (*model.Reconciliation, error) {
	var (
		rc     model.Reconciliation
		mode   string
		result string
		detail []byte
	)

	err := sc.Scan(
		&rc.ID, &rc.RunID, &rc.JobID, &mode,
		&rc.SourceCount, &rc.TargetCount, &rc.MissingCount, &rc.ExtraCount, &rc.MismatchCount,
		&result, &rc.ErrorReason, &detail, &rc.CheckedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("meta: scan reconciliation: %w", err)
	}

	rc.Mode = model.ReconMode(mode)
	rc.Result = model.ReconResult(result)
	if len(detail) > 0 {
		var d model.ReconDetail
		if err := json.Unmarshal(detail, &d); err != nil {
			return nil, fmt.Errorf("meta: unmarshal recon detail (run=%s): %w", rc.RunID, err)
		}
		rc.Detail = &d
	}

	return &rc, nil
}

// InsertErrorRecords 批量写入坏数据记录。
// non_retryable 的行落这里就不再重试，避免一条脏数据反复占用队列。
func (s *Store) InsertErrorRecords(ctx context.Context, recs []model.SyncErrorRecord) error {
	if len(recs) == 0 {
		return nil
	}

	const cols = 11
	placeholder := "(" + strings.TrimSuffix(strings.Repeat("?,", cols), ",") + ")"

	var sb strings.Builder
	sb.WriteString(`INSERT INTO sync_error_records
		(run_id, task_id, job_id,
		 source_system, object_type, source_record_id,
		 error_type, error_code, error_msg, raw_row, created_at) VALUES `)

	args := make([]any, 0, len(recs)*cols)
	now := time.Now().UTC()

	for i := range recs {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(placeholder)

		r := &recs[i]
		var rawRow any
		if len(r.RawRow) > 0 {
			b, err := json.Marshal(r.RawRow)
			if err != nil {
				return fmt.Errorf("meta: marshal raw_row: %w", err)
			}
			rawRow = b
		}
		createdAt := r.CreatedAt
		if createdAt.IsZero() {
			createdAt = now
		}

		args = append(args,
			r.RunID, r.TaskID, r.JobID,
			r.SourceSystem, r.ObjectType, r.SourceRecordID,
			string(r.ErrorType), string(r.ErrorCode), truncate(r.ErrorMsg, 1024),
			rawRow, createdAt,
		)
	}

	if _, err := s.db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("meta: insert error records: %w", err)
	}
	return nil
}

func (s *Store) ListErrorRecordsByRun(ctx context.Context, runID string, limit int) ([]model.SyncErrorRecord, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	const q = `SELECT id, run_id, task_id, job_id,
			source_system, object_type, source_record_id,
			error_type, error_code, error_msg, raw_row, created_at
		FROM sync_error_records WHERE run_id=? ORDER BY id LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, runID, limit)
	if err != nil {
		return nil, fmt.Errorf("meta: list error records: %w", err)
	}
	defer rows.Close()

	var out []model.SyncErrorRecord
	for rows.Next() {
		var (
			r       model.SyncErrorRecord
			errType string
			errCode string
			rawRow  []byte
		)
		if err := rows.Scan(
			&r.ID, &r.RunID, &r.TaskID, &r.JobID,
			&r.SourceSystem, &r.ObjectType, &r.SourceRecordID,
			&errType, &errCode, &r.ErrorMsg, &rawRow, &r.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("meta: scan error record: %w", err)
		}
		r.ErrorType = model.ErrorType(errType)
		r.ErrorCode = model.ErrorCode(errCode)
		if len(rawRow) > 0 {
			if err := json.Unmarshal(rawRow, &r.RawRow); err != nil {
				return nil, fmt.Errorf("meta: unmarshal raw_row (id=%d): %w", r.ID, err)
			}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
