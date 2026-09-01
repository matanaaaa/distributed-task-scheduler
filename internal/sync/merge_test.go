package sync

import (
	"errors"
	"testing"
)

func kc(key, sum string) keyChecksum { return keyChecksum{Key: key, Checksum: sum} }

func TestMergeCompareIdentical(t *testing.T) {
	rows := []keyChecksum{kc("C1", "a"), kc("C2", "b"), kc("C3", "c")}
	d, err := mergeCompare(sliceIter(rows), sliceIter(rows), 10)
	if err != nil {
		t.Fatalf("mergeCompare error: %v", err)
	}
	if d.Missing != 0 || d.Extra != 0 || d.Mismatch != 0 {
		t.Errorf("identical sides should have no diff, got %+v", d)
	}
}

func TestMergeCompareDetectsAllThreeKinds(t *testing.T) {
	// C2 目标端缺失     -> missing
	// C4 源端没有       -> extra
	// C3 指纹不同       -> mismatch
	src := []keyChecksum{kc("C1", "a"), kc("C2", "b"), kc("C3", "c")}
	tgt := []keyChecksum{kc("C1", "a"), kc("C3", "DIFFERENT"), kc("C4", "d")}

	d, err := mergeCompare(sliceIter(src), sliceIter(tgt), 10)
	if err != nil {
		t.Fatalf("mergeCompare error: %v", err)
	}

	if d.Missing != 1 || d.Extra != 1 || d.Mismatch != 1 {
		t.Errorf("got missing=%d extra=%d mismatch=%d, want 1/1/1",
			d.Missing, d.Extra, d.Mismatch)
	}
	if len(d.MissingKeys) != 1 || d.MissingKeys[0] != "C2" {
		t.Errorf("MissingKeys = %v, want [C2]", d.MissingKeys)
	}
	if len(d.ExtraKeys) != 1 || d.ExtraKeys[0] != "C4" {
		t.Errorf("ExtraKeys = %v, want [C4]", d.ExtraKeys)
	}
	if len(d.MismatchKeys) != 1 || d.MismatchKeys[0] != "C3" {
		t.Errorf("MismatchKeys = %v, want [C3]", d.MismatchKeys)
	}
}

func TestMergeCompareEmptyTarget(t *testing.T) {
	// 目标端整个是空的：全部算漏同步，一条不能少报
	src := []keyChecksum{kc("C1", "a"), kc("C2", "b"), kc("C3", "c")}
	d, err := mergeCompare(sliceIter(src), sliceIter(nil), 10)
	if err != nil {
		t.Fatalf("mergeCompare error: %v", err)
	}
	if d.Missing != 3 || d.Extra != 0 || d.Mismatch != 0 {
		t.Errorf("got %+v, want missing=3", d)
	}
}

func TestMergeCompareEmptySource(t *testing.T) {
	tgt := []keyChecksum{kc("C1", "a"), kc("C2", "b")}
	d, err := mergeCompare(sliceIter(nil), sliceIter(tgt), 10)
	if err != nil {
		t.Fatalf("mergeCompare error: %v", err)
	}
	if d.Extra != 2 || d.Missing != 0 {
		t.Errorf("got %+v, want extra=2", d)
	}
}

func TestMergeCompareBothEmpty(t *testing.T) {
	d, err := mergeCompare(sliceIter(nil), sliceIter(nil), 10)
	if err != nil {
		t.Fatalf("mergeCompare error: %v", err)
	}
	if d.Missing != 0 || d.Extra != 0 || d.Mismatch != 0 {
		t.Errorf("both empty should have no diff, got %+v", d)
	}
}

func TestMergeCompareLeadingAndTrailingGaps(t *testing.T) {
	// 差异出现在两端边界，最容易被归并写错的位置
	src := []keyChecksum{kc("A", "1"), kc("M", "2"), kc("Z", "3")}
	tgt := []keyChecksum{kc("M", "2")}

	d, err := mergeCompare(sliceIter(src), sliceIter(tgt), 10)
	if err != nil {
		t.Fatalf("mergeCompare error: %v", err)
	}
	if d.Missing != 2 {
		t.Errorf("missing = %d, want 2 (A and Z)", d.Missing)
	}
	if len(d.MissingKeys) != 2 || d.MissingKeys[0] != "A" || d.MissingKeys[1] != "Z" {
		t.Errorf("MissingKeys = %v, want [A Z]", d.MissingKeys)
	}
}

func TestMergeCompareSampleLimitTruncates(t *testing.T) {
	src := make([]keyChecksum, 0, 50)
	for i := 0; i < 50; i++ {
		// 定宽键保证字节序与数值序一致
		src = append(src, kc(string(rune('A'+i/26))+string(rune('a'+i%26)), "x"))
	}
	d, err := mergeCompare(sliceIter(src), sliceIter(nil), 5)
	if err != nil {
		t.Fatalf("mergeCompare error: %v", err)
	}
	// 计数必须是全量，只有抽样列表被截断
	if d.Missing != 50 {
		t.Errorf("missing count = %d, want 50 (counts must not be truncated)", d.Missing)
	}
	if len(d.MissingKeys) != 5 {
		t.Errorf("MissingKeys length = %d, want 5", len(d.MissingKeys))
	}
	if !d.Truncated {
		t.Error("Truncated must be set when samples are dropped")
	}
}

func TestMergeCompareDefaultsSampleLimit(t *testing.T) {
	src := make([]keyChecksum, 0, 200)
	for i := 0; i < 200; i++ {
		src = append(src, kc(string(rune('A'+i/100))+string(rune('a'+i%100/10))+string(rune('0'+i%10)), "x"))
	}
	d, err := mergeCompare(sliceIter(src), sliceIter(nil), 0)
	if err != nil {
		t.Fatalf("mergeCompare error: %v", err)
	}
	if len(d.MissingKeys) != 100 {
		t.Errorf("default sample limit should be 100, got %d", len(d.MissingKeys))
	}
}

func TestMergeComparePropagatesIterError(t *testing.T) {
	// 读取中途出错必须直接抛出，不能把半截结果当成对账结论
	want := errors.New("source read failed")
	calls := 0
	failing := func() (*keyChecksum, error) {
		calls++
		if calls > 2 {
			return nil, want
		}
		v := kc("C"+string(rune('0'+calls)), "x")
		return &v, nil
	}

	_, err := mergeCompare(failing, sliceIter(nil), 10)
	if !errors.Is(err, want) {
		t.Fatalf("expected iterator error to propagate, got %v", err)
	}
}
