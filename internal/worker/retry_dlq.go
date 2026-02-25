package worker

import (
	"context"
	"fmt"
	"log"
	"time"
)

const (
	dlqQueue        = "queue:dlq"
)

func (w *Worker) onTaskFailed(ctx context.Context, taskID string, cause error) {
	taskKey := "task:" + taskID

	// failed_total +1（每次失败 attempt 计一次）
	_ = w.RDB.HIncrBy(ctx, "metrics:tasks", "failed_total", 1).Err()

	// retry_count++
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

	// 读 priority（队列只存 taskID，所以从 task hash 取）
	pri, _ := w.RDB.HGet(ctx, taskKey, "priority").Result()
	if pri != "high" {
		pri = "normal"
	}

	if retryCount <= maxRetry {
		// 进入 retrying 状态
		_ = w.RDB.HSet(ctx, taskKey, map[string]any{
			"status":     "retrying",
			"phase":      "retrying",
			"msg":        fmt.Sprintf("retrying (%d/%d): %v", retryCount, maxRetry, cause),
			"progress":   "0",
			"updated_at": now,
		}).Err()

		// backoff（指数退避，上限 10s）
		d := retryBackoff(w.retryBase, int(retryCount), 10*time.Second)
		log.Printf("[worker] retry scheduled: task_id=%s retry=%d/%d backoff=%s",
			taskID, retryCount, maxRetry, d)

		taskKeyLocal := taskKey
		retryCountLocal := retryCount
		maxRetryLocal := maxRetry

		go func(taskID, pri, taskKey string, retryCount, maxRetry int64, d time.Duration) {
			time.Sleep(d)

			_ = w.RDB.HSet(context.Background(), taskKey, map[string]any{
				"status":     "queued",
				"phase":      "queued",
				"msg":        fmt.Sprintf("requeued (%d/%d)", retryCount, maxRetry),
				"updated_at": time.Now().UTC().Format(time.RFC3339),
			}).Err()

			if err := w.enqueueTaskID(context.Background(), taskID, pri); err != nil {
				log.Printf("[worker] enqueue retry failed: task_id=%s err=%v", taskID, err)
			}
		}(taskID, pri, taskKeyLocal, retryCountLocal, maxRetryLocal, d)

		return
	}

	// 超过 max_retry：进 DLQ（建议 DLQ 也只存 taskID）
	if err := w.RDB.RPush(ctx, dlqQueue, taskID).Err(); err != nil {
		log.Printf("[worker] push to dlq failed: task_id=%s err=%v", taskID, err)
		return
	}

	_ = w.RDB.HIncrBy(ctx, "metrics:tasks", "dead_total", 1).Err()

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

func (w *Worker) enqueueTaskID(ctx context.Context, taskID string, priority string) error {
	queue := qNormal
	if priority == "high" {
		queue = qHigh
	}

	return w.RDB.RPush(ctx, queue, taskID).Err()
}

func retryBackoff(base time.Duration, attempt int, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// base * 2^(attempt-1)
	d := base * time.Duration(1<<uint(attempt-1))
	if d > max {
		return max
	}
	return d
}