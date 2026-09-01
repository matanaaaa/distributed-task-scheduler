// Package mysqlconn 实现 MySQL 源与 MySQL 目标。
//
// 通过 init 注册进 connector 的类型注册表，使用方只需空导入本包。
package mysqlconn

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
)

// Type 注册用的类型名
const Type = "mysql"

const driverName = "mysql"

func init() {
	connector.RegisterSource(Type, func(cfg model.SourceConfig) (connector.Source, error) {
		return NewSource(cfg)
	})
	connector.RegisterSink(Type, func(cfg model.SinkConfig) (connector.Sink, error) {
		return NewSink(cfg)
	})
}

// identRe 库名/表名/列名只允许字母数字下划线。
//
// 这些标识符来自作业配置，无法用占位符参数化，只能拼进 SQL，
// 所以必须先做白名单校验再反引号包裹，否则就是注入口子。
var identRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// quoteIdent 校验并反引号包裹标识符，支持 db.table 形式
func quoteIdent(s string) (string, error) {
	if s == "" {
		return "", fmt.Errorf("mysqlconn: empty identifier")
	}
	parts := strings.Split(s, ".")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if !identRe.MatchString(p) {
			return "", fmt.Errorf("mysqlconn: illegal identifier %q", s)
		}
		out = append(out, "`"+p+"`")
	}
	return strings.Join(out, "."), nil
}

// quoteIdents 批量包裹
func quoteIdents(cols []string) ([]string, error) {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		q, err := quoteIdent(c)
		if err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, nil
}

// neededColumns 算出真正要 SELECT 的列，避免 SELECT *。
// 包含：分片列、水位列、源记录标识列，以及所有转换规则引用到的源列。
func neededColumns(job *model.Job) []string {
	seen := make(map[string]struct{})
	var out []string

	add := func(c string) {
		if c == "" {
			return
		}
		if _, dup := seen[c]; dup {
			return
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}

	add(job.ShardColumn)
	if job.SyncMode == model.SyncModeIncremental {
		add(job.WatermarkColumn)
	}
	add(job.SourceIDColumn)
	for _, r := range job.Rules {
		add(r.From)
	}
	return out
}
