package mysqlconn

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
)

// checkCoverage 校验分片集合完整覆盖 [lo, hi] 且无重叠无空隙
func checkCoverage(t *testing.T, shards []model.Shard, lo, hi int64) {
	t.Helper()
	if len(shards) == 0 {
		t.Fatal("no shards produced")
	}

	first, err := strconv.ParseInt(shards[0].Lo, 10, 64)
	if err != nil {
		t.Fatalf("bad Lo: %v", err)
	}
	if first != lo {
		t.Errorf("first shard Lo = %d, want %d", first, lo)
	}

	last, err := strconv.ParseInt(shards[len(shards)-1].Hi, 10, 64)
	if err != nil {
		t.Fatalf("bad Hi: %v", err)
	}
	if last != hi+1 {
		t.Errorf("last shard Hi = %d, want %d (hi+1)", last, hi+1)
	}

	for i := 1; i < len(shards); i++ {
		prevHi, _ := strconv.ParseInt(shards[i-1].Hi, 10, 64)
		curLo, _ := strconv.ParseInt(shards[i].Lo, 10, 64)
		if prevHi != curLo {
			t.Errorf("gap or overlap between shard %d (Hi=%d) and %d (Lo=%d)",
				i-1, prevHi, i, curLo)
		}
	}

	for i, s := range shards {
		if s.Index != i {
			t.Errorf("shard %d has Index = %d", i, s.Index)
		}
		l, _ := strconv.ParseInt(s.Lo, 10, 64)
		h, _ := strconv.ParseInt(s.Hi, 10, 64)
		if h <= l {
			t.Errorf("shard %d is empty: [%d, %d)", i, l, h)
		}
	}
}

func TestDivideRangeCoversExactly(t *testing.T) {
	cases := []struct {
		name  string
		lo    int64
		hi    int64
		count int
	}{
		{"even split", 1, 100, 4},
		{"uneven split", 1, 100, 3},
		{"uneven split 7", 1, 5000, 7},
		{"single shard", 1, 100, 1},
		{"zero count treated as one", 1, 100, 0},
		{"count exceeds span", 10, 12, 8},
		{"single row", 42, 42, 4},
		{"lo not one", 1000, 2500, 5},
		{"negative lo", -50, 50, 4},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			shards := divideRange(c.lo, c.hi, c.count)
			checkCoverage(t, shards, c.lo, c.hi)
		})
	}
}

func TestDivideRangeCountExceedsSpanProducesNoEmptyShards(t *testing.T) {
	// 3 个值要求切 8 片，只能切出 3 片，且每片恰好一个值
	shards := divideRange(10, 12, 8)
	if len(shards) != 3 {
		t.Fatalf("got %d shards, want 3", len(shards))
	}
	for i, s := range shards {
		lo, _ := strconv.ParseInt(s.Lo, 10, 64)
		hi, _ := strconv.ParseInt(s.Hi, 10, 64)
		if hi-lo != 1 {
			t.Errorf("shard %d spans %d values, want 1", i, hi-lo)
		}
	}
}

func TestDivideRangeEmptyWhenHiBelowLo(t *testing.T) {
	if got := divideRange(100, 99, 4); got != nil {
		t.Errorf("expected nil for empty range, got %v", got)
	}
}

func TestDivideRangeTotalSpanIsPreserved(t *testing.T) {
	// 所有分片覆盖的值总数必须等于区间长度，多一个少一个都是 bug
	const lo, hi = 1, 5000
	for count := 1; count <= 32; count++ {
		var total int64
		for _, s := range divideRange(lo, hi, count) {
			l, _ := strconv.ParseInt(s.Lo, 10, 64)
			h, _ := strconv.ParseInt(s.Hi, 10, 64)
			total += h - l
		}
		if want := int64(hi - lo + 1); total != want {
			t.Errorf("count=%d: covered %d values, want %d", count, total, want)
		}
	}
}

func TestQuoteIdentRejectsInjection(t *testing.T) {
	bad := []string{
		"customer; DROP TABLE x",
		"customer`",
		"cust omer",
		"customer'",
		"",
		"--comment",
		"tbl WHERE 1=1",
	}
	for _, s := range bad {
		if _, err := quoteIdent(s); err == nil {
			t.Errorf("quoteIdent(%q) should have been rejected", s)
		}
	}

	good := map[string]string{
		"customer":     "`customer`",
		"crm.customer": "`crm`.`customer`",
		"customer_no":  "`customer_no`",
		"Col123":       "`Col123`",
	}
	for in, want := range good {
		got, err := quoteIdent(in)
		if err != nil {
			t.Errorf("quoteIdent(%q) unexpected error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("quoteIdent(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNeededColumnsDedupesAndSkipsWatermarkForFullSync(t *testing.T) {
	job := &model.Job{
		ShardColumn:     "id",
		WatermarkColumn: "updated_at",
		SourceIDColumn:  "customer_no",
		SyncMode:        model.SyncModeFull,
		Rules: []model.TransformRule{
			{From: "customer_no", To: "customer_no", Op: model.OpCopy},
			{From: "phone", To: "phone_masked", Op: model.OpMaskPhone},
			{From: "phone", To: "phone_hash", Op: model.OpSHA256},
			{To: "synced_at", Op: model.OpSyncNow},
		},
	}

	cols := neededColumns(job)

	// phone 被两条规则引用，只应出现一次；sync_now 没有源列不应引入空列
	want := []string{"id", "customer_no", "phone"}
	if len(cols) != len(want) {
		t.Fatalf("neededColumns = %v, want %v", cols, want)
	}
	for i := range want {
		if cols[i] != want[i] {
			t.Errorf("neededColumns[%d] = %q, want %q", i, cols[i], want[i])
		}
	}

	// 增量模式下水位列必须被读出来
	job.SyncMode = model.SyncModeIncremental
	cols = neededColumns(job)
	found := false
	for _, c := range cols {
		if c == "updated_at" {
			found = true
		}
		if c == "" {
			t.Error("empty column name leaked into needed columns")
		}
	}
	if !found {
		t.Error("incremental sync must read the watermark column")
	}
}

// buildWhere 不访问数据库，可以直接构造 Source 断言生成的 SQL 形状
func testSource(filter string) *Source {
	return &Source{cfg: model.SourceConfig{Table: "customer", Filter: filter}}
}

func fullJob() *model.Job {
	return &model.Job{
		Name:        "full",
		SyncMode:    model.SyncModeFull,
		ShardColumn: "id",
	}
}

func incrementalJob() *model.Job {
	return &model.Job{
		Name:            "incr",
		SyncMode:        model.SyncModeIncremental,
		ShardColumn:     "id",
		WatermarkColumn: "updated_at",
	}
}

func TestBuildWhereFullUsesShardRangeAndPKCursor(t *testing.T) {
	s := testSource("is_deleted = 0")
	shard := model.Shard{Index: 1, Lo: "100", Hi: "200"}
	resume := model.Watermark{Value: "150", ID: 150}

	where, args, err := s.buildWhere(fullJob(), shard, connector.ReadRange{}, resume)
	if err != nil {
		t.Fatalf("buildWhere error: %v", err)
	}

	for _, want := range []string{"(is_deleted = 0)", "`id` >= ?", "`id` < ?", "`id` > ?"} {
		if !strings.Contains(where, want) {
			t.Errorf("full where missing %q\ngot: %s", want, where)
		}
	}
	// 全量模式不该出现水位条件
	if strings.Contains(where, "updated_at") {
		t.Errorf("full sync must not reference watermark column\ngot: %s", where)
	}
	if len(args) != 3 {
		t.Errorf("full args = %v, want 3 (lo, hi, resume)", args)
	}
}

func TestBuildWhereIncrementalUsesCompositePredicate(t *testing.T) {
	s := testSource("is_deleted = 0")
	rng := connector.ReadRange{
		From: model.Watermark{Value: "2026-08-01 00:00:00.000", ID: 10},
		To:   model.Watermark{Value: "2026-08-13 00:00:00.000", ID: 999},
	}

	where, args, err := s.buildWhere(incrementalJob(), model.Shard{}, rng, model.Watermark{})
	if err != nil {
		t.Fatalf("buildWhere error: %v", err)
	}

	// 下界必须是复合比较：只用 > 会漏掉同一时刻的其余行
	wantLower := "(`updated_at` > ? OR (`updated_at` = ? AND `id` > ?))"
	if !strings.Contains(where, wantLower) {
		t.Errorf("incremental where missing composite lower bound\nwant substring: %s\ngot: %s", wantLower, where)
	}
	wantUpper := "(`updated_at` < ? OR (`updated_at` = ? AND `id` <= ?))"
	if !strings.Contains(where, wantUpper) {
		t.Errorf("incremental where missing composite upper bound\nwant substring: %s\ngot: %s", wantUpper, where)
	}

	// 增量单流，不该带主键分片区间，否则会走主键扫描而不是水位索引
	if strings.Contains(where, "`id` >= ?") || strings.Contains(where, "`id` < ?") {
		t.Errorf("incremental must not add shard range predicates\ngot: %s", where)
	}

	// 参数顺序：下界三个 + 上界三个
	want := []any{
		"2026-08-01 00:00:00.000", "2026-08-01 00:00:00.000", int64(10),
		"2026-08-13 00:00:00.000", "2026-08-13 00:00:00.000", int64(999),
	}
	if len(args) != len(want) {
		t.Fatalf("incremental args = %v, want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("args[%d] = %v, want %v", i, args[i], want[i])
		}
	}
}

func TestBuildWhereIncrementalResumeReplacesLowerBound(t *testing.T) {
	// 断点一定不早于窗口下界，续跑时应当用断点而不是原下界，
	// 否则每次重试都会从窗口头部重新扫一遍
	s := testSource("")
	rng := connector.ReadRange{
		From: model.Watermark{Value: "2026-08-01 00:00:00.000", ID: 10},
		To:   model.Watermark{Value: "2026-08-13 00:00:00.000", ID: 999},
	}
	resume := model.Watermark{Value: "2026-08-10 12:00:00.000", ID: 500}

	_, args, err := s.buildWhere(incrementalJob(), model.Shard{}, rng, resume)
	if err != nil {
		t.Fatalf("buildWhere error: %v", err)
	}

	if args[0] != resume.Value || args[2] != resume.ID {
		t.Errorf("resume did not replace lower bound: args = %v", args)
	}
	if args[0] == rng.From.Value {
		t.Error("lower bound still uses window start instead of checkpoint")
	}
}

func TestWatermarkStringFormatsDatetimeForMySQL(t *testing.T) {
	// 必须是 MySQL 能直接当日期字面量解析的形式，且保留毫秒。
	// RFC3339 那种带 T 和 Z 的写法回传比较会出错。
	ts := time.Date(2026, 8, 13, 10, 30, 45, 123000000, time.UTC)
	got := watermarkString(ts)
	want := "2026-08-13 10:30:45.123"
	if got != want {
		t.Errorf("watermarkString(time) = %q, want %q", got, want)
	}
	if strings.ContainsAny(got, "TZ") {
		t.Errorf("watermark must not be RFC3339: %q", got)
	}

	if got := watermarkString([]byte("2026-08-13 10:30:45.123")); got != want {
		t.Errorf("watermarkString([]byte) = %q, want %q", got, want)
	}
	if got := watermarkString(int64(42)); got != "42" {
		t.Errorf("watermarkString(int64) = %q, want 42", got)
	}
	if got := watermarkString(nil); got != "" {
		t.Errorf("watermarkString(nil) = %q, want empty", got)
	}
}
