package transform

import (
	"testing"
	"time"

	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
)

func TestMaskPhone(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"13812345678", "138****5678"},
		{"1381234567", "138***4567"},
		{"", ""},
		// 长度不足以保留前3后4时整体打星，绝不原样透出
		{"1234567", "*******"},
		{"123", "***"},
	}
	for _, c := range cases {
		if got := maskPhone(c.in); got != c.want {
			t.Errorf("maskPhone(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestToStringHandlesDriverBytes(t *testing.T) {
	// go-sql-driver 把 VARCHAR 给成 []byte，这里必须能正确转换
	if got := ToString([]byte("C00000001")); got != "C00000001" {
		t.Errorf("ToString([]byte) = %q, want C00000001", got)
	}
	if got := ToString(nil); got != "" {
		t.Errorf("ToString(nil) = %q, want empty", got)
	}
	if got := ToString(int64(42)); got != "42" {
		t.Errorf("ToString(int64) = %q, want 42", got)
	}
}

func TestApplyMapsAndMasks(t *testing.T) {
	rules := []model.TransformRule{
		{From: "customer_no", To: "customer_no", Op: model.OpCopy},
		{From: "name", To: "name", Op: model.OpCopy},
		{From: "phone", To: "phone_masked", Op: model.OpMaskPhone},
		{From: "phone", To: "phone_hash", Op: model.OpSHA256},
		{To: "synced_at", Op: model.OpSyncNow},
	}
	now := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	src := connector.Row{
		"customer_no": []byte("C00000001"),
		"name":        []byte("客户_1"),
		"phone":       []byte("13812345678"),
		// 源行里多出来的列不应出现在目标行
		"id_card": []byte("310100000000000001"),
	}

	dst, err := Apply(rules, src, now)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}

	if got := dst["customer_no"]; got != "C00000001" {
		t.Errorf("customer_no = %v (%T), want string C00000001", got, got)
	}
	if got := dst["phone_masked"]; got != "138****5678" {
		t.Errorf("phone_masked = %v, want 138****5678", got)
	}
	if dst["phone_hash"] == "" || dst["phone_hash"] == "13812345678" {
		t.Errorf("phone_hash not hashed: %v", dst["phone_hash"])
	}
	if got := dst["synced_at"]; got != now {
		t.Errorf("synced_at = %v, want %v", got, now)
	}

	// 关键断言：未映射的列不得泄漏到目标端，
	// 这是"目标端自有字段不被覆盖"和"敏感字段不外流"的基础
	if _, leaked := dst["id_card"]; leaked {
		t.Error("id_card leaked into target row")
	}
	if len(dst) != 5 {
		t.Errorf("target row has %d columns, want 5: %v", len(dst), dst)
	}
}

func TestApplyMissingColumnIsReported(t *testing.T) {
	rules := []model.TransformRule{
		{From: "not_exist", To: "x", Op: model.OpCopy},
	}
	_, err := Apply(rules, connector.Row{"a": 1}, time.Now())
	if err == nil {
		t.Fatal("expected error for missing source column")
	}
	var missing *ErrMissingColumn
	if !asErr(err, &missing) {
		t.Fatalf("expected *ErrMissingColumn, got %T: %v", err, err)
	}
}

func TestValidateRules(t *testing.T) {
	cases := []struct {
		name    string
		rules   []model.TransformRule
		wantErr bool
	}{
		{"empty", nil, true},
		{"missing target", []model.TransformRule{{From: "a", Op: model.OpCopy}}, true},
		{"copy without source", []model.TransformRule{{To: "a", Op: model.OpCopy}}, true},
		{"sync_now needs no source", []model.TransformRule{{To: "a", Op: model.OpSyncNow}}, false},
		{"unknown op", []model.TransformRule{{From: "a", To: "b", Op: "nope"}}, true},
		{"duplicate target", []model.TransformRule{
			{From: "a", To: "x", Op: model.OpCopy},
			{From: "b", To: "x", Op: model.OpCopy},
		}, true},
		{"valid", []model.TransformRule{{From: "a", To: "b", Op: model.OpCopy}}, false},
	}
	for _, c := range cases {
		err := ValidateRules(c.rules)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: ValidateRules error = %v, wantErr = %v", c.name, err, c.wantErr)
		}
	}
}

// asErr 是 errors.As 的薄封装，避免测试文件里重复 import
func asErr[T any](err error, target *T) bool {
	for err != nil {
		if v, ok := err.(T); ok {
			*target = v
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestApplyConst(t *testing.T) {
	rules := []model.TransformRule{
		{To: "customer_no", From: "customer_no", Op: model.OpCopy},
		{To: "source_channel", Op: model.OpConst, Value: "crm"},
	}
	dst, err := Apply(rules, connector.Row{"customer_no": []byte("C1")}, time.Now())
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	if got := dst["source_channel"]; got != "crm" {
		t.Errorf("source_channel = %v, want crm", got)
	}
}

func TestCastValue(t *testing.T) {
	cases := []struct {
		name string
		in   any
		to   model.CastType
		want any
	}{
		{"bytes to int", []byte("42"), model.CastInt, int64(42)},
		{"string to int", "42", model.CastInt, int64(42)},
		{"int64 passthrough", int64(7), model.CastInt, int64(7)},
		// DECIMAL 列常以 "3.0" 形式出现，不该判成坏数据
		{"decimal string to int", []byte("3.0"), model.CastInt, int64(3)},
		{"bool to int", true, model.CastInt, int64(1)},
		{"bytes to float", []byte("3.5"), model.CastFloat, 3.5},
		{"int to float", int64(3), model.CastFloat, float64(3)},
		{"tinyint 1 to bool", []byte("1"), model.CastBool, true},
		{"tinyint 0 to bool", []byte("0"), model.CastBool, false},
		{"yes to bool", "yes", model.CastBool, true},
		{"empty to bool", []byte(""), model.CastBool, false},
		{"int to string", int64(42), model.CastString, "42"},
	}
	for _, c := range cases {
		got, err := castValue(c.in, c.to)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", c.name, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s: castValue(%v, %s) = %v (%T), want %v (%T)",
				c.name, c.in, c.to, got, got, c.want, c.want)
		}
	}
}

func TestCastNullStaysNull(t *testing.T) {
	// NULL 不能被悄悄变成 0 或空串：
	// "没有值"和"值是零"在业务上不是一回事
	for _, to := range []model.CastType{
		model.CastInt, model.CastFloat, model.CastBool, model.CastString,
	} {
		got, err := castValue(nil, to)
		if err != nil {
			t.Errorf("cast nil to %s: unexpected error %v", to, err)
		}
		if got != nil {
			t.Errorf("cast nil to %s = %v, want nil", to, got)
		}
	}
}

func TestCastFailureIsReported(t *testing.T) {
	if _, err := castValue([]byte("abc"), model.CastInt); err == nil {
		t.Error("expected error casting 'abc' to int")
	}
	if _, err := castValue([]byte("maybe"), model.CastBool); err == nil {
		t.Error("expected error casting 'maybe' to bool")
	}

	rules := []model.TransformRule{
		{From: "age", To: "age", Op: model.OpCast, CastTo: model.CastInt},
	}
	_, err := Apply(rules, connector.Row{"age": []byte("not-a-number")}, time.Now())
	if err == nil {
		t.Fatal("expected Apply to fail on bad cast")
	}
	var castErr *ErrCastFailed
	if !asErr(err, &castErr) {
		t.Fatalf("expected *ErrCastFailed, got %T: %v", err, err)
	}
	if castErr.Column != "age" || castErr.To != model.CastInt {
		t.Errorf("cast error lost context: %+v", castErr)
	}
}

func TestValidateRulesConstAndCast(t *testing.T) {
	cases := []struct {
		name    string
		rule    model.TransformRule
		wantErr bool
	}{
		{"const without value", model.TransformRule{To: "a", Op: model.OpConst}, true},
		{"const with value", model.TransformRule{To: "a", Op: model.OpConst, Value: "x"}, false},
		{"cast without source", model.TransformRule{To: "a", Op: model.OpCast, CastTo: model.CastInt}, true},
		{"cast without type", model.TransformRule{From: "b", To: "a", Op: model.OpCast}, true},
		{"cast with bad type", model.TransformRule{From: "b", To: "a", Op: model.OpCast, CastTo: "datetime"}, true},
		{"cast valid", model.TransformRule{From: "b", To: "a", Op: model.OpCast, CastTo: model.CastInt}, false},
	}
	for _, c := range cases {
		err := ValidateRules([]model.TransformRule{c.rule})
		if (err != nil) != c.wantErr {
			t.Errorf("%s: error = %v, wantErr = %v", c.name, err, c.wantErr)
		}
	}
}
