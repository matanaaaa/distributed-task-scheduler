package sync

import (
	"testing"

	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
)

func TestChecksumIsOrderStable(t *testing.T) {
	fields := []string{"customer_no", "name", "region"}

	// map 的遍历顺序是随机的，指纹却必须只由字段列表顺序决定，
	// 否则同一行反复算会得到不同结果，对账全表误报
	row := connector.Row{"customer_no": "C1", "name": "张三", "region": "华东"}
	first := checksumOf(row, fields)
	for i := 0; i < 50; i++ {
		if got := checksumOf(row, fields); got != first {
			t.Fatalf("checksum not stable across calls: %s vs %s", got, first)
		}
	}

	// 两端字段顺序一致时，[]byte 与 string 表示应得到同一指纹
	rowBytes := connector.Row{
		"customer_no": []byte("C1"),
		"name":        []byte("张三"),
		"region":      []byte("华东"),
	}
	if got := checksumOf(rowBytes, fields); got != first {
		t.Errorf("driver []byte and string must hash identically:\n%s\n%s", got, first)
	}
}

func TestChecksumDistinguishesNullFromEmpty(t *testing.T) {
	fields := []string{"email"}

	nullRow := connector.Row{"email": nil}
	emptyRow := connector.Row{"email": ""}
	missingRow := connector.Row{}

	nullSum := checksumOf(nullRow, fields)
	emptySum := checksumOf(emptyRow, fields)

	if nullSum == emptySum {
		t.Error("NULL and empty string must not hash the same: they differ in business meaning")
	}
	// 列缺失与列为 NULL 视为同一种"没有值"
	if got := checksumOf(missingRow, fields); got != nullSum {
		t.Error("missing column and NULL should hash identically")
	}
}

func TestChecksumDetectsFieldShift(t *testing.T) {
	// 相邻字段内容互换必须产生不同指纹，
	// 否则分隔符设计有问题（例如直接拼接会让 "ab"+"c" 等于 "a"+"bc"）
	fields := []string{"a", "b"}
	x := checksumOf(connector.Row{"a": "ab", "b": "c"}, fields)
	y := checksumOf(connector.Row{"a": "a", "b": "bc"}, fields)
	if x == y {
		t.Error("concatenation is ambiguous: field boundaries are not separated")
	}
}

func TestComparableFieldsExcludesSyncNow(t *testing.T) {
	job := &model.Job{
		Rules: []model.TransformRule{
			{From: "customer_no", To: "customer_no", Op: model.OpCopy},
			{From: "phone", To: "phone_masked", Op: model.OpMaskPhone},
			{From: "is_deleted", To: "is_deleted", Op: model.OpCast, CastTo: model.CastInt},
			{To: "source_channel", Op: model.OpConst, Value: "crm"},
			// 每次执行都不同，参与比较必然全表 mismatch
			{To: "synced_at", Op: model.OpSyncNow},
		},
	}

	got := comparableFields(job)
	want := []string{"customer_no", "phone_masked", "is_deleted", "source_channel"}

	if len(got) != len(want) {
		t.Fatalf("comparableFields = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("comparableFields[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	for _, f := range got {
		if f == "synced_at" {
			t.Error("sync_now target must be excluded from comparison")
		}
	}
}

func TestReconKeyColumnRequiresSingleKey(t *testing.T) {
	single := &model.Job{SinkConfig: model.SinkConfig{UniqueKey: []string{"customer_no"}}}
	got, err := reconKeyColumn(single)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "customer_no" {
		t.Errorf("reconKeyColumn = %q, want customer_no", got)
	}

	for _, bad := range [][]string{nil, {"a", "b"}} {
		job := &model.Job{SinkConfig: model.SinkConfig{UniqueKey: bad}}
		if _, err := reconKeyColumn(job); err == nil {
			t.Errorf("unique_key %v should be rejected for field-level reconciliation", bad)
		}
	}
}

func TestExcuseBadRows(t *testing.T) {
	r := &Reconciler{}

	// 缺口能被坏数据解释：不算异常
	rc := &model.Reconciliation{Result: model.ReconMismatch, MissingCount: 3}
	r.excuseBadRows(rc, &model.JobRun{RowsFailed: 5})
	if rc.Result != model.ReconOK {
		t.Error("missing count within bad-row count should be excused")
	}
	if rc.ErrorReason == "" {
		t.Error("excused mismatch must record why")
	}

	// 缺口大于坏数据数：仍然是问题
	rc = &model.Reconciliation{Result: model.ReconMismatch, MissingCount: 10}
	r.excuseBadRows(rc, &model.JobRun{RowsFailed: 2})
	if rc.Result != model.ReconMismatch {
		t.Error("missing count beyond bad rows must stay a mismatch")
	}

	// 字段不一致与坏数据无关，不能被解释掉
	rc = &model.Reconciliation{Result: model.ReconMismatch, MismatchCount: 7}
	r.excuseBadRows(rc, &model.JobRun{RowsFailed: 100})
	if rc.Result != model.ReconMismatch {
		t.Error("field mismatch must never be excused by bad rows")
	}

	// 目标端多余行同理
	rc = &model.Reconciliation{Result: model.ReconMismatch, ExtraCount: 1}
	r.excuseBadRows(rc, &model.JobRun{RowsFailed: 100})
	if rc.Result != model.ReconMismatch {
		t.Error("extra rows must never be excused by bad rows")
	}
}
