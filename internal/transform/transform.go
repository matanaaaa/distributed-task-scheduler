// Package transform 按作业配置的规则把源行映射成目标行。
//
// 只有出现在规则里的目标列才会被写出，所以目标端自有的业务字段
// （例如营销系统的 send_status / unsubscribed）天然不会被同步覆盖。
package transform

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
)

// ErrMissingColumn 规则引用了源行里不存在的列，属于配置错误，重试无意义
type ErrMissingColumn struct {
	Column string
	Op     model.TransformOp
}

func (e *ErrMissingColumn) Error() string {
	return fmt.Sprintf("transform: source column %q not found (op=%s)", e.Column, e.Op)
}

// ErrUnknownOp 未知算子
type ErrUnknownOp struct {
	Op model.TransformOp
}

func (e *ErrUnknownOp) Error() string {
	return fmt.Sprintf("transform: unknown op %q", e.Op)
}

// ErrCastFailed 类型转换失败。这是典型的坏数据而非基础设施故障，
// 重试不会让它变好，所以归类为 non_retryable 并落坏数据表。
type ErrCastFailed struct {
	Column string
	To     model.CastType
	Value  string
	Cause  error
}

func (e *ErrCastFailed) Error() string {
	return fmt.Sprintf("transform: cannot cast column %q value %q to %s: %v",
		e.Column, e.Value, e.To, e.Cause)
}

func (e *ErrCastFailed) Unwrap() error { return e.Cause }

// Apply 把一行源数据按规则转换成目标行。
// syncNow 由调用方统一传入，保证同一批数据的 sync_now 值一致。
func Apply(rules []model.TransformRule, src connector.Row, syncNow time.Time) (connector.Row, error) {
	dst := make(connector.Row, len(rules))

	for _, r := range rules {
		switch r.Op {
		case model.OpSyncNow:
			dst[r.To] = syncNow

		case model.OpCopy:
			v, ok := src[r.From]
			if !ok {
				return nil, &ErrMissingColumn{Column: r.From, Op: r.Op}
			}
			dst[r.To] = normalize(v)

		case model.OpMaskPhone:
			v, ok := src[r.From]
			if !ok {
				return nil, &ErrMissingColumn{Column: r.From, Op: r.Op}
			}
			dst[r.To] = maskPhone(ToString(v))

		case model.OpSHA256:
			v, ok := src[r.From]
			if !ok {
				return nil, &ErrMissingColumn{Column: r.From, Op: r.Op}
			}
			dst[r.To] = sha256Hex(ToString(v))

		case model.OpConst:
			dst[r.To] = r.Value

		case model.OpCast:
			v, ok := src[r.From]
			if !ok {
				return nil, &ErrMissingColumn{Column: r.From, Op: r.Op}
			}
			casted, err := castValue(v, r.CastTo)
			if err != nil {
				return nil, &ErrCastFailed{Column: r.From, To: r.CastTo, Value: ToString(v), Cause: err}
			}
			dst[r.To] = casted

		default:
			return nil, &ErrUnknownOp{Op: r.Op}
		}
	}

	return dst, nil
}

// ValidateRules 在作业创建时校验规则，把配置错误挡在入口而不是运行时
func ValidateRules(rules []model.TransformRule) error {
	if len(rules) == 0 {
		return fmt.Errorf("transform: at least one rule is required")
	}
	seen := make(map[string]struct{}, len(rules))

	for i, r := range rules {
		if r.To == "" {
			return fmt.Errorf("transform: rule[%d] missing target column", i)
		}
		if _, dup := seen[r.To]; dup {
			return fmt.Errorf("transform: rule[%d] duplicate target column %q", i, r.To)
		}
		seen[r.To] = struct{}{}

		switch r.Op {
		case model.OpSyncNow:
			// 无源列
		case model.OpConst:
			// 无源列，但值不能省，否则等于往目标列写空
			if r.Value == "" {
				return fmt.Errorf("transform: rule[%d] op const requires a value", i)
			}
		case model.OpCast:
			if r.From == "" {
				return fmt.Errorf("transform: rule[%d] op cast requires source column", i)
			}
			if !r.CastTo.Valid() {
				return fmt.Errorf("transform: rule[%d] op cast has invalid cast_to %q (int/float/bool/string)", i, r.CastTo)
			}
		case model.OpCopy, model.OpMaskPhone, model.OpSHA256:
			if r.From == "" {
				return fmt.Errorf("transform: rule[%d] op %s requires source column", i, r.Op)
			}
		default:
			return &ErrUnknownOp{Op: r.Op}
		}
	}
	return nil
}

// ToString 把驱动返回的值转成字符串。
//
// go-sql-driver 在扫进 any 时会把 VARCHAR 给成 []byte 而不是 string，
// 直接断言 string 会静默失败，这是这类同步代码最常见的坑之一。
func ToString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		return string(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		if t {
			return "1"
		}
		return "0"
	case time.Time:
		return t.Format("2006-01-02 15:04:05.000")
	default:
		return fmt.Sprint(t)
	}
}

// normalize 把 []byte 收敛成 string，避免同一份数据在目标端因类型差异表现不一致
func normalize(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

// maskPhone 保留前 3 后 4，中间打星。
// 长度不足以脱敏时整体打星，绝不原样透出。
func maskPhone(s string) string {
	r := []rune(s)
	if len(r) == 0 {
		return ""
	}
	if len(r) <= 7 {
		return string(repeatRune('*', len(r)))
	}
	masked := make([]rune, 0, len(r))
	masked = append(masked, r[:3]...)
	masked = append(masked, repeatRune('*', len(r)-7)...)
	masked = append(masked, r[len(r)-4:]...)
	return string(masked)
}

func repeatRune(c rune, n int) []rune {
	if n <= 0 {
		return nil
	}
	out := make([]rune, n)
	for i := range out {
		out[i] = c
	}
	return out
}

func sha256Hex(s string) string {
	if s == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// castValue 把源值转成目标类型。
//
// NULL 原样保留为 nil，不擅自变成 0 或空字符串：
// "没有值"和"值是零"在业务上是两件事，同步过程不该替业务做这个决定。
func castValue(v any, to model.CastType) (any, error) {
	if v == nil {
		return nil, nil
	}

	switch to {
	case model.CastString:
		return ToString(v), nil

	case model.CastInt:
		switch t := v.(type) {
		case int64:
			return t, nil
		case int32:
			return int64(t), nil
		case int:
			return int64(t), nil
		case float64:
			return int64(t), nil
		case bool:
			if t {
				return int64(1), nil
			}
			return int64(0), nil
		}
		s := strings.TrimSpace(ToString(v))
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			// 允许 "3.0" 这类来自 DECIMAL 列的写法
			if f, ferr := strconv.ParseFloat(s, 64); ferr == nil {
				return int64(f), nil
			}
			return nil, err
		}
		return n, nil

	case model.CastFloat:
		switch t := v.(type) {
		case float64:
			return t, nil
		case float32:
			return float64(t), nil
		case int64:
			return float64(t), nil
		case int:
			return float64(t), nil
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(ToString(v)), 64)
		if err != nil {
			return nil, err
		}
		return f, nil

	case model.CastBool:
		if b, ok := v.(bool); ok {
			return b, nil
		}
		// MySQL 的 TINYINT(1) 常以 0/1 出现，同时兼容 true/false/yes/no
		switch strings.ToLower(strings.TrimSpace(ToString(v))) {
		case "1", "true", "t", "yes", "y":
			return true, nil
		case "0", "false", "f", "no", "n", "":
			return false, nil
		}
		return nil, fmt.Errorf("not a boolean")

	default:
		return nil, fmt.Errorf("unsupported cast target %q", to)
	}
}
