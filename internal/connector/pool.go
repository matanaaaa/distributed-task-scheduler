package connector

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
)

var (
	poolMu sync.Mutex
	pools  = map[string]*sql.DB{}
)

// SharedDB 按 driver+DSN 复用连接池。
//
// 一个 worker 上会并发跑 N 个分片，如果每个分片各开一个连接池，
// 对 CRM 生产库的连接数就被放大 N 倍——这是同步工具最容易犯的错，
// 也是源库 DBA 最反感的行为。按 DSN 复用把它压回单个池，
// 再由池自己的 MaxOpenConns 兜住上限。
func SharedDB(driver, dsn string, maxOpen int) (*sql.DB, error) {
	if dsn == "" {
		return nil, fmt.Errorf("connector: empty dsn")
	}
	key := driver + "|" + dsn

	poolMu.Lock()
	defer poolMu.Unlock()

	if db, ok := pools[key]; ok {
		return db, nil
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("connector: open %s: %w", driver, err)
	}
	if maxOpen <= 0 {
		maxOpen = 8
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	pools[key] = db
	return db, nil
}

// CloseSharedPools 关闭所有共享连接池，进程退出时调用
func CloseSharedPools() {
	poolMu.Lock()
	defer poolMu.Unlock()
	for k, db := range pools {
		_ = db.Close()
		delete(pools, k)
	}
}
