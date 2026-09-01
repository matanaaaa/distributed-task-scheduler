package syncerr

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
	"github.com/matanaaaa/distributed-task-scheduler/internal/transform"
)

func TestClassifyRetryableMySQLErrors(t *testing.T) {
	cases := []struct {
		num  uint16
		code model.ErrorCode
	}{
		{1213, model.CodeDBDeadlock},    // 死锁：重试就好
		{1205, model.CodeDBTimeout},     // 锁等待超时
		{1040, model.CodeDBConnFailure}, // 连接数打满
		{1290, model.CodeDBReadOnly},    // 只读，通常主从切换中
	}
	for _, c := range cases {
		err := &mysql.MySQLError{Number: c.num, Message: "x"}
		typ, code := Classify(err)
		if typ != model.ErrorRetryable {
			t.Errorf("mysql %d: type = %s, want retryable", c.num, typ)
		}
		if code != c.code {
			t.Errorf("mysql %d: code = %s, want %s", c.num, code, c.code)
		}
	}
}

func TestClassifyNonRetryableMySQLErrors(t *testing.T) {
	cases := []struct {
		num  uint16
		code model.ErrorCode
	}{
		{1406, model.CodeValueTooLong},    // 字段超长，重试一万次也一样
		{1048, model.CodeMissingRequired}, // 非空列写 NULL
		{1366, model.CodeTypeMismatch},    // 值格式不对
		{1146, model.CodeIllegalValue},    // 表不存在，配置错
	}
	for _, c := range cases {
		err := &mysql.MySQLError{Number: c.num, Message: "x"}
		typ, code := Classify(err)
		if typ != model.ErrorNonRetryable {
			t.Errorf("mysql %d: type = %s, want non_retryable", c.num, typ)
		}
		if code != c.code {
			t.Errorf("mysql %d: code = %s, want %s", c.num, code, c.code)
		}
	}
}

func TestClassifyThroughWrappedError(t *testing.T) {
	// 实际调用链里错误一定是被 fmt.Errorf 包过的，分类必须能穿透
	inner := &mysql.MySQLError{Number: 1213, Message: "deadlock"}
	wrapped := fmt.Errorf("mysqlconn: batch upsert: %w", inner)
	if !IsRetryable(wrapped) {
		t.Error("wrapped deadlock must still be retryable")
	}

	tooLong := fmt.Errorf("outer: %w", &mysql.MySQLError{Number: 1406})
	if !IsNonRetryable(tooLong) {
		t.Error("wrapped data-too-long must still be non-retryable")
	}
}

func TestClassifyTransformErrors(t *testing.T) {
	err := fmt.Errorf("wrap: %w", &transform.ErrMissingColumn{Column: "phone", Op: "copy"})
	typ, code := Classify(err)
	if typ != model.ErrorNonRetryable {
		t.Errorf("missing column: type = %s, want non_retryable", typ)
	}
	if code != model.CodeMissingRequired {
		t.Errorf("missing column: code = %s, want %s", code, model.CodeMissingRequired)
	}

	if !IsNonRetryable(&transform.ErrUnknownOp{Op: "bogus"}) {
		t.Error("unknown transform op must be non-retryable")
	}
}

func TestClassifyTimeouts(t *testing.T) {
	typ, code := Classify(context.DeadlineExceeded)
	if typ != model.ErrorRetryable || code != model.CodeDBTimeout {
		t.Errorf("deadline exceeded = (%s,%s), want (retryable,db_timeout)", typ, code)
	}

	typ, code = Classify(errors.New("dial tcp 10.0.0.1:3306: connect: connection refused"))
	if typ != model.ErrorRetryable || code != model.CodeDBConnFailure {
		t.Errorf("connection refused = (%s,%s), want (retryable,db_conn_failure)", typ, code)
	}
}

func TestUnknownErrorDefaultsToRetryable(t *testing.T) {
	// 不认识的错误保守当作可重试：重试预算耗尽后会进 DLQ，
	// 不会被静默丢弃，比反过来漏掉真正的瞬时故障更安全
	typ, code := Classify(errors.New("something nobody has seen before"))
	if typ != model.ErrorRetryable {
		t.Errorf("unknown error type = %s, want retryable", typ)
	}
	if code != model.CodeUnknown {
		t.Errorf("unknown error code = %s, want unknown", code)
	}
}

func TestIsCanceled(t *testing.T) {
	if !IsCanceled(fmt.Errorf("wrap: %w", context.Canceled)) {
		t.Error("wrapped context.Canceled must be detected")
	}
	if IsCanceled(context.DeadlineExceeded) {
		t.Error("deadline exceeded is not cancellation")
	}
}

func TestClassifyCastFailureIsNonRetryable(t *testing.T) {
	err := fmt.Errorf("write: %w", &transform.ErrCastFailed{
		Column: "age",
		To:     "int",
		Value:  "abc",
		Cause:  errors.New("invalid syntax"),
	})
	typ, code := Classify(err)
	if typ != model.ErrorNonRetryable {
		t.Errorf("cast failure type = %s, want non_retryable", typ)
	}
	if code != model.CodeTypeMismatch {
		t.Errorf("cast failure code = %s, want %s", code, model.CodeTypeMismatch)
	}
}
