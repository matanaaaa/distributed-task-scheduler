// Package syncerr 把底层错误归类成"值得重试"与"重试也没用"。
//
// 这是同步语义下 Retry/DLQ 的分岔点。原来的任务执行器对所有失败
// 一律指数退避重试，在数据同步场景下是错的：目标端字段超长、必填为空
// 这类坏数据，重试一万次结果都一样，只会把队列和重试预算耗光，
// 让真正该重试的网络抖动排不上队。
package syncerr

import (
	"context"
	"database/sql/driver"
	"errors"
	"net"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
	"github.com/matanaaaa/distributed-task-scheduler/internal/transform"
)

// MySQL 服务端错误号中值得重试的一类：锁竞争、连接耗尽、主从切换
var retryableMySQLErrs = map[uint16]model.ErrorCode{
	1040: model.CodeDBConnFailure, // ER_CON_COUNT_ERROR      连接数打满
	1053: model.CodeDBConnFailure, // ER_SERVER_SHUTDOWN      正在关闭
	1152: model.CodeDBConnFailure, // ER_ABORTING_CONNECTION  连接被中断
	1205: model.CodeDBTimeout,     // ER_LOCK_WAIT_TIMEOUT    锁等待超时
	1213: model.CodeDBDeadlock,    // ER_LOCK_DEADLOCK        死锁，重试即可
	1290: model.CodeDBReadOnly,    // ER_OPTION_PREVENTS_...  只读，通常是切换中
	1614: model.CodeDBDeadlock,    // ER_XA_RBDEADLOCK
}

// MySQL 服务端错误号中属于数据/配置问题的一类：重试不会变好
var nonRetryableMySQLErrs = map[uint16]model.ErrorCode{
	1048: model.CodeMissingRequired, // ER_BAD_NULL_ERROR        非空列写入 NULL
	1054: model.CodeIllegalValue,    // ER_BAD_FIELD_ERROR       列不存在，配置错
	1062: model.CodeIllegalValue,    // ER_DUP_ENTRY             唯一键冲突，说明 upsert 键配错
	1146: model.CodeIllegalValue,    // ER_NO_SUCH_TABLE         表不存在，配置错
	1264: model.CodeIllegalValue,    // ER_WARN_DATA_OUT_OF_RANGE
	1265: model.CodeTypeMismatch,    // ER_WARN_DATA_TRUNCATED
	1292: model.CodeTypeMismatch,    // ER_TRUNCATED_WRONG_VALUE 日期/数字格式不对
	1366: model.CodeTypeMismatch,    // ER_TRUNCATED_WRONG_VALUE_FOR_FIELD
	1406: model.CodeValueTooLong,    // ER_DATA_TOO_LONG         字段超长
}

// Classify 判断错误类型与细分原因
func Classify(err error) (model.ErrorType, model.ErrorCode) {
	if err == nil {
		return model.ErrorRetryable, model.CodeUnknown
	}

	// 转换层错误一律是配置或数据问题
	var missingCol *transform.ErrMissingColumn
	if errors.As(err, &missingCol) {
		return model.ErrorNonRetryable, model.CodeMissingRequired
	}
	var unknownOp *transform.ErrUnknownOp
	if errors.As(err, &unknownOp) {
		return model.ErrorNonRetryable, model.CodeIllegalValue
	}
	// 类型转换失败：源端这一行的值本身不合法，重试多少次都一样
	var castFailed *transform.ErrCastFailed
	if errors.As(err, &castFailed) {
		return model.ErrorNonRetryable, model.CodeTypeMismatch
	}

	// MySQL 服务端返回的编号最准，优先看它
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		if code, ok := retryableMySQLErrs[myErr.Number]; ok {
			return model.ErrorRetryable, code
		}
		if code, ok := nonRetryableMySQLErrs[myErr.Number]; ok {
			return model.ErrorNonRetryable, code
		}
		// 未收录的服务端错误：保守当作可重试，
		// 真正的坏数据会在重试耗尽后进 DLQ，不会被静默丢掉
		return model.ErrorRetryable, model.CodeUnknown
	}

	// 超时与连接层问题都值得重试
	if errors.Is(err, context.DeadlineExceeded) {
		return model.ErrorRetryable, model.CodeDBTimeout
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysql.ErrInvalidConn) {
		return model.ErrorRetryable, model.CodeDBConnFailure
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return model.ErrorRetryable, model.CodeDBTimeout
		}
		return model.ErrorRetryable, model.CodeDBConnFailure
	}

	// 兜底：驱动有些连接问题只体现在文本里
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "server has gone away"),
		strings.Contains(msg, "invalid connection"),
		strings.Contains(msg, "no such host"):
		return model.ErrorRetryable, model.CodeDBConnFailure
	case strings.Contains(msg, "i/o timeout"),
		strings.Contains(msg, "deadline exceeded"):
		return model.ErrorRetryable, model.CodeDBTimeout
	}

	// 完全不认识的错误：当作可重试，让重试预算和 DLQ 去兜底
	return model.ErrorRetryable, model.CodeUnknown
}

// IsRetryable 是否值得重试
func IsRetryable(err error) bool {
	t, _ := Classify(err)
	return t == model.ErrorRetryable
}

// IsNonRetryable 是否属于坏数据/配置错误
func IsNonRetryable(err error) bool {
	return !IsRetryable(err)
}

// ctxCanceled 单独判断主动取消：这不是失败，是关机
func IsCanceled(err error) bool {
	return errors.Is(err, context.Canceled)
}
