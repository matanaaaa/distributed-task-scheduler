package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"
)

const (
	dlqQueue        = "queue:dlq"
)

func (w *Worker) onTaskFailed(ctx context.Context, msg QueueMsg, cause error) {
	taskID := msg.TaskID
	taskKey := "task:" + taskID

	// 1) retry_count++
	// HINCRBY 会在字段不存在时按 0 处理，非常适合最小实现
	retryCount, err := w.RDB.HIncrBy(ctx, taskKey, "retry_count", 1).Result()
	if err != nil {
		log.Printf("[worker] retry HINCRBY failed: task_id=%s err=%v", taskID, err)
		return
	}

	maxRetry := int64(w.maxRetry)

	// 记录失败原因
	now := time.Now().UTC().Format(time.RFC3339)
	_ = w.RDB.HSet(ctx, taskKey, map[string]any{
		"error_reason": fmt.Sprintf("%v", cause),
		"updated_at":   now,
	}).Err()

	if retryCount <= maxRetry {
		// 2) 进入 retrying 状态
		_ = w.RDB.HSet(ctx, taskKey, map[string]any{
			"status":   "retrying",
			"phase":    "retrying",
			"msg":      fmt.Sprintf("retrying (%d/%d): %v", retryCount, maxRetry, cause),
			"progress": "0",
			"updated_at": now,
		}).Err()

		// 3) backoff（指数退避，上限 10s）
		d := retryBackoff(w.retryBase, int(retryCount), 10*time.Second)
		log.Printf("[worker] retry scheduled: task_id=%s retry=%d/%d backoff=%s",
			taskID, retryCount, maxRetry, d)

		time.Sleep(d)

		// 4) 重新入队（按 priority 回 high/normal）
		if err := w.enqueue(ctx, msg); err != nil {
			log.Printf("[worker] enqueue retry failed: task_id=%s err=%v", taskID, err)
		}
		return
	}

	// 5) 超过 max_retry：进 DLQ + 标记 dead
	raw, _ := json.Marshal(msg)
	if err := w.RDB.LPush(ctx, dlqQueue, string(raw)).Err(); err != nil {
		log.Printf("[worker] push to dlq failed: task_id=%s err=%v", taskID, err)
		return
	}

	_ = w.RDB.HSet(ctx, taskKey, map[string]any{
		"status":      "dead",
		"phase":       "dead",
		"msg":         fmt.Sprintf("moved to dlq after %d retries: %v", retryCount-1, cause),
		"progress":    "100",
		"finished_at": now,
		"updated_at":  now,
	}).Err()

	log.Printf("[worker] moved to dlq: task_id=%s retry_count=%d", taskID, retryCount)
}

func (w *Worker) enqueue(ctx context.Context, msg QueueMsg) error {
	queue := "queue:normal"
	if msg.Priority == "high" {
		queue = "queue:high"
	}

	raw, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return w.RDB.LPush(ctx, queue, string(raw)).Err()
}

func retryBackoff(base time.Duration, attempt int, max time.Duration) time.Duration {
	// attempt=1 -> 1s, attempt=2 -> 2s, attempt=3 -> 4s ...
	sec := float64(base) * math.Pow(2, float64(attempt-1))
	d := time.Duration(sec)
	if d > max {
		return max
	}
	return d
}
