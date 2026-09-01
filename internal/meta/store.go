// Package meta 是同步平台的元数据存储层。
//
// 这里保存平台的"真相"：作业配置、执行历史、分片状态、断点、对账结果。
// Redis 只承担队列与运行时协调（inflight、租约、幂等锁），
// 重启 Redis 不会丢失同步配置和历史记录。
package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// ErrNotFound 目标记录不存在
var ErrNotFound = errors.New("meta: record not found")

// ErrNotClaimable 任务当前状态不允许被领取（已被别的 worker 领走或已终态）
var ErrNotClaimable = errors.New("meta: task not claimable")

// ErrConcurrentUpdate 条件更新影响 0 行：读到状态之后有人抢先改了它。
//
// 与 model.ErrIllegalTransition 的区别很重要：
// 后者是调用方的转换规则用错了，前者只是并发竞争，重新读一次即可。
var ErrConcurrentUpdate = errors.New("meta: concurrent state update")

type Store struct {
	db *sql.DB
}

// Open 打开元数据库连接池。
// DSN 必须带 parseTime=true，否则 DATETIME 无法扫进 time.Time。
func Open(dsn string, maxOpenConns int) (*Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("meta: open db: %w", err)
	}
	if maxOpenConns <= 0 {
		maxOpenConns = 16
	}
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxOpenConns)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return &Store{db: db}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *Store) Close() error { return s.db.Close() }

// DB 暴露底层连接池，供需要自行组织事务的调用方使用
func (s *Store) DB() *sql.DB { return s.db }

// nullTime 把可空时间指针转成可直接传给 driver 的值
func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}

// timePtr 把扫描出来的可空时间转回指针
func timePtr(nt sql.NullTime) *time.Time {
	if !nt.Valid {
		return nil
	}
	v := nt.Time
	return &v
}

// truncate 防止超长错误信息撑爆 VARCHAR(1024)
func truncate(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max]
}
