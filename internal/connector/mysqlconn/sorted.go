package mysqlconn

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
)

// reconCollation 对账排序与比较统一使用的排序规则。
//
// 归并比较要求两端的顺序和 Go 里的字符串比较完全一致。
// 默认 utf8mb4_general_ci 是大小写不敏感的，MySQL 认为 'a' == 'A'，
// 而 Go 按字节比较认为不等，顺序一旦不一致，归并就会误判成
// 大量 missing 和 extra。强制 utf8mb4_bin 得到字节序，与 Go 对齐。
//
// 代价是可能放弃索引改走 filesort。对账是离线任务，这个代价可以接受。
const reconCollation = "utf8mb4_bin"

// OpenSorted 按业务键升序读取窗口内全部行
func (s *Source) OpenSorted(ctx context.Context, job *model.Job, rng connector.ReadRange) (connector.RowReader, error) {
	if job.SourceIDColumn == "" {
		return nil, fmt.Errorf("mysqlconn: job %s missing source_id_column, cannot reconcile", job.Name)
	}

	cols := neededColumns(job)
	quoted, err := quoteIdents(cols)
	if err != nil {
		return nil, err
	}
	tbl, err := quoteIdent(s.cfg.Table)
	if err != nil {
		return nil, err
	}
	keyCol, err := quoteIdent(job.SourceIDColumn)
	if err != nil {
		return nil, err
	}

	// 分片留空表示整个窗口；对账要看全量，不能只看某一片
	where, args, err := s.buildWhere(job, model.Shard{}, rng, model.Watermark{})
	if err != nil {
		return nil, err
	}

	return &sortedReader{
		db:         s.db,
		selectSQL:  strings.Join(quoted, ", "),
		table:      tbl,
		keyCol:     keyCol,
		keyName:    job.SourceIDColumn,
		where:      where,
		staticArgs: args,
		batchSize:  batchSizeOf(job),
	}, nil
}

// OpenSorted 按业务唯一键升序读取目标端指定列
func (s *Sink) OpenSorted(ctx context.Context, job *model.Job, columns []string) (connector.RowReader, error) {
	if len(s.cfg.UniqueKey) == 0 {
		return nil, fmt.Errorf("mysqlconn: sink unique_key is required to reconcile")
	}
	// 单列业务键才能做有序归并；复合唯一键需要拼接比较键，暂不支持
	if len(s.cfg.UniqueKey) > 1 {
		return nil, fmt.Errorf("mysqlconn: keyset reconciliation supports single-column unique_key only, got %v", s.cfg.UniqueKey)
	}
	keyName := s.cfg.UniqueKey[0]

	// 保证比较键本身在投影里
	sel := make([]string, 0, len(columns)+1)
	seen := map[string]struct{}{}
	for _, c := range append([]string{keyName}, columns...) {
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		sel = append(sel, c)
	}

	quoted, err := quoteIdents(sel)
	if err != nil {
		return nil, err
	}
	tbl, err := quoteIdent(s.cfg.Table)
	if err != nil {
		return nil, err
	}
	keyCol, err := quoteIdent(keyName)
	if err != nil {
		return nil, err
	}

	where := "1=1"
	if strings.TrimSpace(s.cfg.Filter) != "" {
		where = "(" + s.cfg.Filter + ")"
	}

	return &sortedReader{
		db:        s.db,
		selectSQL: strings.Join(quoted, ", "),
		table:     tbl,
		keyCol:    keyCol,
		keyName:   keyName,
		where:     where,
		batchSize: batchSizeOf(job),
	}, nil
}

func batchSizeOf(job *model.Job) int {
	if job.BatchSize > 0 {
		return job.BatchSize
	}
	return 1000
}

// sortedReader 按业务键升序的 keyset 翻页游标，两端对账共用。
//
// 仍然用 keyset 而不是 LIMIT offset：对账要扫全表，
// offset 深翻页会让后半程越来越慢。
type sortedReader struct {
	db         *sql.DB
	selectSQL  string
	table      string
	keyCol     string
	keyName    string
	where      string
	staticArgs []any
	batchSize  int

	cursor    string
	hasCursor bool
	done      bool
}

func (r *sortedReader) Next(ctx context.Context) (*connector.Batch, error) {
	if r.done {
		return nil, nil
	}

	conds := r.where
	args := make([]any, 0, len(r.staticArgs)+1)
	args = append(args, r.staticArgs...)

	if r.hasCursor {
		// 比较也要带上同一个排序规则，否则和 ORDER BY 的顺序不一致，
		// 翻页会跳行或原地打转
		conds = fmt.Sprintf("%s AND %s COLLATE %s > ?", conds, r.keyCol, reconCollation)
		args = append(args, r.cursor)
	}

	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s COLLATE %s LIMIT %d",
		r.selectSQL, r.table, conds, r.keyCol, reconCollation, r.batchSize)

	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("mysqlconn: sorted read: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("mysqlconn: sorted columns: %w", err)
	}

	batch := &connector.Batch{Rows: make([]connector.Row, 0, r.batchSize)}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("mysqlconn: sorted scan: %w", err)
		}
		row := make(connector.Row, len(cols))
		for i, c := range cols {
			row[c] = vals[i]
		}
		batch.Rows = append(batch.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysqlconn: sorted iterate: %w", err)
	}

	if len(batch.Rows) == 0 {
		r.done = true
		return nil, nil
	}

	last := batch.Rows[len(batch.Rows)-1]
	r.cursor = watermarkString(last[r.keyName])
	r.hasCursor = true
	batch.Cursor = model.Watermark{Value: r.cursor}

	if len(batch.Rows) < r.batchSize {
		r.done = true
	}
	return batch, nil
}

func (r *sortedReader) Close() error { return nil }
