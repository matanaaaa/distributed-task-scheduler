package mysqlconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
)

// Source 从 MySQL 表按分片读取数据
type Source struct {
	cfg model.SourceConfig
	db  *sql.DB
}

func NewSource(cfg model.SourceConfig) (*Source, error) {
	if cfg.Table == "" {
		return nil, fmt.Errorf("mysqlconn: source table is required")
	}
	if _, err := quoteIdent(cfg.Table); err != nil {
		return nil, err
	}
	db, err := connector.SharedDB(driverName, cfg.DSN, 8)
	if err != nil {
		return nil, err
	}
	return &Source{cfg: cfg, db: db}, nil
}

// Close 不关闭连接池：池按 DSN 共享，由进程退出时统一释放
func (s *Source) Close() error { return nil }

// CurrentWatermark 取当前最大复合水位 (watermark_column, shard_column)。
// 全量模式没有水位概念，返回零值。
func (s *Source) CurrentWatermark(ctx context.Context, job *model.Job) (model.Watermark, error) {
	if job.SyncMode != model.SyncModeIncremental {
		return model.Watermark{}, nil
	}
	if job.WatermarkColumn == "" {
		return model.Watermark{}, fmt.Errorf("mysqlconn: incremental job %s missing watermark_column", job.Name)
	}

	tbl, err := quoteIdent(s.cfg.Table)
	if err != nil {
		return model.Watermark{}, err
	}
	wmCol, err := quoteIdent(job.WatermarkColumn)
	if err != nil {
		return model.Watermark{}, err
	}
	idCol, err := quoteIdent(job.ShardColumn)
	if err != nil {
		return model.Watermark{}, err
	}

	q := fmt.Sprintf(
		"SELECT %s, %s FROM %s WHERE %s ORDER BY %s DESC, %s DESC LIMIT 1",
		wmCol, idCol, tbl, s.filterExpr(), wmCol, idCol,
	)

	// 扫进 any 再统一格式化：若直接扫进 []byte，驱动会把 DATETIME
	// 渲染成 RFC3339（带 T 和 Z），与游标推进时的格式不一致，
	// 同一个逻辑水位就有了两种字符串写法，回传比较必然出错
	var (
		wmRaw any
		id    int64
	)
	err = s.db.QueryRowContext(ctx, q).Scan(&wmRaw, &id)
	if errors.Is(err, sql.ErrNoRows) {
		// 空表：水位保持零值，本次没有数据可同步
		return model.Watermark{}, nil
	}
	if err != nil {
		return model.Watermark{}, fmt.Errorf("mysqlconn: read current watermark: %w", err)
	}
	return model.Watermark{Value: watermarkString(wmRaw), ID: id}, nil
}

// watermarkString 把水位列的值渲染成可安全回传给 MySQL 比较的字符串。
//
// DATETIME(3) 用 'YYYY-MM-DD HH:MM:SS.mmm' 而不是 RFC3339：
// 前者能被 MySQL 直接当日期字面量解析，且保留到毫秒，与列精度对齐。
// 整数型水位列（例如自增版本号）原样转字符串即可。
func watermarkString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case time.Time:
		return t.Format(watermarkTimeLayout)
	case []byte:
		return string(t)
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	default:
		return fmt.Sprint(t)
	}
}

const watermarkTimeLayout = "2006-01-02 15:04:05.000"

// Split 把作业切成分片，策略按同步模式分流。
//
// 全量：数据量大且沿主键均匀分布，按主键整数区间等分，多 worker 并行拉取。
//
// 增量：变更集通常只占全表极小一部分，此时按主键切片是有害的——
// 查询同时带主键区间和水位条件，MySQL 只会选一个索引，
// 走主键区间就意味着每个分片都要扫过整段主键再逐行过滤水位，
// 500 万行的表改了 100 行也会扫掉几十万行。所以增量退化为单流，
// 由 rowReader 按 (水位列, 主键) 做 keyset 翻页，只碰真正变更的行。
//
// 真到了增量集也需要并行的量级，该切的是水位区间（按时间分桶），
// 而不是主键区间。
func (s *Source) Split(ctx context.Context, job *model.Job, rng connector.ReadRange) ([]model.Shard, error) {
	if job.ShardColumn == "" {
		return nil, fmt.Errorf("mysqlconn: job %s missing shard_column", job.Name)
	}

	if job.SyncMode == model.SyncModeIncremental {
		return s.splitIncremental(ctx, job, rng)
	}
	return s.splitFull(ctx, job, rng)
}

// splitIncremental 增量模式单流。
// 窗口 (From, To] 已经限定了范围，分片区间留空即可。
func (s *Source) splitIncremental(ctx context.Context, job *model.Job, rng connector.ReadRange) ([]model.Shard, error) {
	n, err := s.Count(ctx, job, rng)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	return []model.Shard{{Index: 0}}, nil
}

// splitFull 全量模式按主键整数区间等分。
//
// 分片列必须是整数列（通常就是自增主键）：只有可比较可等分的数值
// 才能算出互不重叠且覆盖完整的区间。
//
// 上界固定在本次 MAX 而不是留开口：同步期间新插入的行归下一次 Run，
// 这样每个 Run 的数据范围是确定的，对账才有意义。
func (s *Source) splitFull(ctx context.Context, job *model.Job, rng connector.ReadRange) ([]model.Shard, error) {

	tbl, err := quoteIdent(s.cfg.Table)
	if err != nil {
		return nil, err
	}
	shardCol, err := quoteIdent(job.ShardColumn)
	if err != nil {
		return nil, err
	}

	where, args, err := s.buildWhere(job, model.Shard{}, rng, model.Watermark{})
	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s WHERE %s", shardCol, shardCol, tbl, where)

	var minV, maxV sql.NullInt64
	if err := s.db.QueryRowContext(ctx, q, args...).Scan(&minV, &maxV); err != nil {
		return nil, fmt.Errorf("mysqlconn: split min/max: %w", err)
	}

	// 窗口内没有数据：返回零个分片，Run 直接收敛为成功
	if !minV.Valid || !maxV.Valid {
		return nil, nil
	}

	return divideRange(minV.Int64, maxV.Int64, job.ShardCount), nil
}

// divideRange 把闭区间 [lo, hi] 等分成至多 count 个半开区间 [Lo, Hi)。
//
// 分成纯函数是因为边界最容易出错：区间重叠会重复同步，
// 留空会静默丢行，而丢行在生产上极难发现。
//
// 保证三条性质：
//   - 首片 Lo == lo，末片 Hi == hi+1，完整覆盖
//   - 相邻片首尾相接，既不重叠也不留空
//   - count 大于区间长度时退化为每片一个值，不产生空片
func divideRange(lo, hi int64, count int) []model.Shard {
	if hi < lo {
		return nil
	}
	span := hi - lo + 1
	if count <= 1 {
		count = 1
	}
	if int64(count) > span {
		count = int(span)
	}
	// 向上取整，保证最后一片能盖到 hi
	step := (span + int64(count) - 1) / int64(count)

	shards := make([]model.Shard, 0, count)
	for i := 0; i < count; i++ {
		sLo := lo + int64(i)*step
		if sLo > hi {
			break
		}
		sHi := sLo + step
		if sHi > hi+1 {
			sHi = hi + 1
		}
		shards = append(shards, model.Shard{
			Index: len(shards),
			Lo:    strconv.FormatInt(sLo, 10),
			Hi:    strconv.FormatInt(sHi, 10),
		})
	}
	return shards
}

// Count 统计窗口内源端记录数
func (s *Source) Count(ctx context.Context, job *model.Job, rng connector.ReadRange) (int64, error) {
	tbl, err := quoteIdent(s.cfg.Table)
	if err != nil {
		return 0, err
	}
	where, args, err := s.buildWhere(job, model.Shard{}, rng, model.Watermark{})
	if err != nil {
		return 0, err
	}

	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tbl, where)

	var n int64
	if err := s.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("mysqlconn: source count: %w", err)
	}
	return n, nil
}

// Open 打开分片读取游标
func (s *Source) Open(ctx context.Context, job *model.Job, shard model.Shard, rng connector.ReadRange, resume model.Watermark) (connector.RowReader, error) {
	cols := neededColumns(job)
	if len(cols) == 0 {
		return nil, fmt.Errorf("mysqlconn: job %s has no columns to read", job.Name)
	}
	quoted, err := quoteIdents(cols)
	if err != nil {
		return nil, err
	}
	tbl, err := quoteIdent(s.cfg.Table)
	if err != nil {
		return nil, err
	}
	shardCol, err := quoteIdent(job.ShardColumn)
	if err != nil {
		return nil, err
	}

	batchSize := job.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	var limiter *rate.Limiter
	if job.ReadQPS > 0 {
		// 限的是每秒查询批次数：每批一条查询，直接对应源库压力
		limiter = rate.NewLimiter(rate.Limit(job.ReadQPS), 1)
	}

	// 排序键决定走哪条索引，也决定游标怎么推进：
	//   全量 ORDER BY 主键        —— 沿主键顺序扫，配合主键区间分片
	//   增量 ORDER BY 水位, 主键  —— 走水位索引，只碰变更行
	orderBy := shardCol
	if s.isIncremental(job) {
		wmCol, err := quoteIdent(job.WatermarkColumn)
		if err != nil {
			return nil, err
		}
		orderBy = wmCol + ", " + shardCol
	}

	return &rowReader{
		src:         s,
		job:         job,
		shard:       shard,
		rng:         rng,
		cursor:      resume,
		incremental: s.isIncremental(job),
		selectSQL:   strings.Join(quoted, ", "),
		table:       tbl,
		orderBy:     orderBy,
		batchSize:   batchSize,
		limiter:     limiter,
	}, nil
}

// filterExpr 返回行过滤表达式，没有配置时返回恒真。
//
// Filter 是原样拼进 SQL 的管理员配置，不做参数化——这是有意的信任边界：
// 作业配置属于运维侧输入，创建作业的接口必须有鉴权保护。
func (s *Source) filterExpr() string {
	if strings.TrimSpace(s.cfg.Filter) == "" {
		return "1=1"
	}
	return "(" + s.cfg.Filter + ")"
}

// buildWhere 拼出 WHERE 条件与参数。
//
// 三部分叠加：
//  1. 行过滤（如 is_deleted = 0）
//  2. 分片区间 [Lo, Hi)
//  3. 增量窗口 (From, To]，复合比较避免同一时刻的行被漏掉或无限重复
//
// resume 非零时再叠加断点条件，用于中断后续跑。
func (s *Source) buildWhere(job *model.Job, shard model.Shard, rng connector.ReadRange, resume model.Watermark) (string, []any, error) {
	conds := []string{s.filterExpr()}
	var args []any

	shardCol, err := quoteIdent(job.ShardColumn)
	if err != nil {
		return "", nil, err
	}

	if shard.Lo != "" {
		conds = append(conds, shardCol+" >= ?")
		args = append(args, shard.Lo)
	}
	if shard.Hi != "" {
		conds = append(conds, shardCol+" < ?")
		args = append(args, shard.Hi)
	}

	if !s.isIncremental(job) {
		// 全量：分片内按主键翻页。主键唯一，严格大于即可，不存在并列
		if !resume.IsZero() {
			conds = append(conds, shardCol+" > ?")
			args = append(args, resume.ID)
		}
		return strings.Join(conds, " AND "), args, nil
	}

	wmCol, err := quoteIdent(job.WatermarkColumn)
	if err != nil {
		return "", nil, err
	}

	// 增量下界。断点一定不早于窗口下界，所以有断点时用断点，
	// 两者形式完全一致，都是复合比较。
	//
	// 为什么必须复合：只用 wm > ? 会漏掉与边界同一时刻的其余行，
	// 只用 wm >= ? 又会每次重复整个时刻。以主键打破并列后，
	// 边界既不漏也不会无限重复，剩下的一批重复由目标端幂等 upsert 吸收。
	lower := rng.From
	if !resume.IsZero() {
		lower = resume
	}
	if !lower.IsZero() {
		conds = append(conds, fmt.Sprintf("(%s > ? OR (%s = ? AND %s > ?))",
			wmCol, wmCol, shardCol))
		args = append(args, lower.Value, lower.Value, lower.ID)
	}

	// 上界 ..., To]：同样复合收紧，让下一次 Run 正好从这里接上
	if !rng.To.IsZero() {
		conds = append(conds, fmt.Sprintf("(%s < ? OR (%s = ? AND %s <= ?))",
			wmCol, wmCol, shardCol))
		args = append(args, rng.To.Value, rng.To.Value, rng.To.ID)
	}

	return strings.Join(conds, " AND "), args, nil
}

// isIncremental 判断是否走增量读取路径
func (s *Source) isIncremental(job *model.Job) bool {
	return job.SyncMode == model.SyncModeIncremental && job.WatermarkColumn != ""
}

// rowReader 分片读取游标，做 keyset 翻页
type rowReader struct {
	src   *Source
	job   *model.Job
	shard model.Shard
	rng   connector.ReadRange

	// cursor 已读到的位置，每批推进。
	// 全量模式下是 (主键, 主键)，增量模式下是 (水位值, 主键)。
	cursor      model.Watermark
	incremental bool

	selectSQL string
	table     string
	orderBy   string
	batchSize int
	limiter   *rate.Limiter

	done bool
}

// Next 读下一批。
//
// 用 keyset 翻页（WHERE shard_col > cursor ORDER BY shard_col LIMIT n）
// 而不是 LIMIT offset：offset 在大表深翻页时要扫过并丢弃前 offset 行，
// 越翻越慢；keyset 每次都走索引定位，翻到哪都是同样开销。
func (r *rowReader) Next(ctx context.Context) (*connector.Batch, error) {
	if r.done {
		return nil, nil
	}
	if r.limiter != nil {
		if err := r.limiter.Wait(ctx); err != nil {
			return nil, err
		}
	}

	where, args, err := r.src.buildWhere(r.job, r.shard, r.rng, r.cursor)
	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d",
		r.selectSQL, r.table, where, r.orderBy, r.batchSize)

	rows, err := r.src.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("mysqlconn: read batch: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("mysqlconn: read columns: %w", err)
	}

	batch := &connector.Batch{Rows: make([]connector.Row, 0, r.batchSize)}

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("mysqlconn: scan row: %w", err)
		}

		row := make(connector.Row, len(cols))
		for i, c := range cols {
			row[c] = vals[i]
		}
		batch.Rows = append(batch.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysqlconn: iterate rows: %w", err)
	}

	if len(batch.Rows) == 0 {
		r.done = true
		return nil, nil
	}

	// 用最后一行推进游标。排序键是什么，游标就必须是什么，
	// 否则下一批的 keyset 条件和排序不匹配，会漏行或死循环。
	last := batch.Rows[len(batch.Rows)-1]
	id, err := toInt64(last[r.job.ShardColumn])
	if err != nil {
		return nil, fmt.Errorf("mysqlconn: shard column %q is not an integer: %w", r.job.ShardColumn, err)
	}

	cursorValue := strconv.FormatInt(id, 10)
	if r.incremental {
		wmVal, ok := last[r.job.WatermarkColumn]
		if !ok {
			return nil, fmt.Errorf("mysqlconn: watermark column %q missing from result set", r.job.WatermarkColumn)
		}
		cursorValue = watermarkString(wmVal)
	}

	r.cursor = model.Watermark{Value: cursorValue, ID: id}
	batch.Cursor = r.cursor

	// 不足一批说明已经读到分片末尾，下次直接返回结束
	if len(batch.Rows) < r.batchSize {
		r.done = true
	}
	return batch, nil
}

func (r *rowReader) Close() error { return nil }

func toInt64(v any) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int32:
		return int64(t), nil
	case int:
		return int64(t), nil
	case uint64:
		return int64(t), nil
	case []byte:
		return strconv.ParseInt(string(t), 10, 64)
	case string:
		return strconv.ParseInt(t, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}
