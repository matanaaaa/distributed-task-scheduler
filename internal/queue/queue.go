// Package queue 收敛所有 Redis 键名与入队操作。
//
// 键名原来分散在 worker 和 store 两个包里各写一份，
// 任何一处写错都不会编译失败，只会安静地投到一个没人消费的队列上。
// 集中到一处后，API 与 worker 引用的一定是同一个键。
package queue

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const (
	// High 高优先级待办队列
	High = "queue:high"
	// Normal 普通待办队列
	Normal = "queue:normal"
	// Inflight 已领取但未确认的任务。BRPOPLPUSH 的落点，进程崩了靠它兜住
	Inflight = "queue:inflight"
	// DLQ 死信队列，重试耗尽或坏数据导致判死的分片
	DLQ = "queue:dlq"
	// Processing 租约有序集合，score 为租约到期时间戳
	Processing = "z:processing"
	// MetricsHash 指标计数哈希
	MetricsHash = "metrics:tasks"
	// LockPrefix 分片幂等锁前缀
	LockPrefix = "lock:task:"
)

// ForPriority 把优先级映射到队列名，未知优先级一律走普通队列
func ForPriority(priority string) string {
	if priority == "high" {
		return High
	}
	return Normal
}

// Queue 是 Redis 队列的薄封装
type Queue struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Queue {
	return &Queue{rdb: rdb}
}

// Enqueue 把分片任务投进对应优先级队列，实现 sync.Enqueuer
func (q *Queue) Enqueue(ctx context.Context, priority string, taskID string) error {
	return q.rdb.RPush(ctx, ForPriority(priority), taskID).Err()
}

// Ping 探活
func (q *Queue) Ping(ctx context.Context) error {
	return q.rdb.Ping(ctx).Err()
}

// Client 暴露底层客户端
func (q *Queue) Client() *redis.Client {
	return q.rdb
}
