package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
	"github.com/matanaaaa/distributed-task-scheduler/internal/queue"
	"github.com/matanaaaa/distributed-task-scheduler/internal/syncerr"
)

const dlqQueue = queue.DLQ

// onTaskFailed 决定一次失败该重试还是直接判死。
//
// 这是从"任务执行器"换到"数据同步"之后语义变化最大的地方。
// 原来的实现对所有失败一律指数退避重试，在同步场景下是错的：
//
//	目标端字段超长、必填列为空、表不存在 —— 重试一万次结果一样，
//	只会耗光重试预算，让真正该重试的网络抖动排不上队。
//
// 所以先按错误类型分流：基础设施问题退避重试，数据/配置问题直接进 DLQ。
func (w *Worker) onTaskFailed(ctx context.Context, taskID string, cause error) {
	_ = w.RDB.HIncrBy(ctx, "metrics:tasks", "failed_total", 1).Err()

	task, err := w.meta.GetTask(ctx, taskID)
	if err != nil {
		log.Printf("[worker] onTaskFailed: load task failed: task=%s err=%v", taskID, err)
		return
	}

	// 还没真正开始执行就失败（领取阶段出错）：直接退回队列重来，
	// 不消耗重试预算，因为这一次根本没碰到数据
	if task.Status == model.StatusQueued {
		log.Printf("[worker] task failed before claim, requeue without retry budget: task=%s", taskID)
		w.requeueAfter(ctx, taskID, task.Priority, w.retryBase)
		return
	}

	// 保证进入 failed 状态，MarkTaskRetrying / MarkTaskDead 都以此为前置
	if task.Status == model.StatusRunning {
		if err := w.meta.MarkTaskFailed(ctx, taskID, cause.Error()); err != nil {
			log.Printf("[worker] mark failed error: task=%s err=%v", taskID, err)
			return
		}
	}

	errType, errCode := syncerr.Classify(cause)

	// 数据或配置问题：重试没有意义，直接判死
	if errType == model.ErrorNonRetryable {
		log.Printf("[worker] non-retryable failure, straight to DLQ: task=%s code=%s err=%v",
			taskID, errCode, cause)
		w.moveToDLQ(ctx, task, fmt.Sprintf("non-retryable (%s): %v", errCode, cause))
		return
	}

	retryCount, err := w.meta.MarkTaskRetrying(ctx, taskID, cause.Error())
	if err != nil {
		log.Printf("[worker] mark retrying error: task=%s err=%v", taskID, err)
		return
	}

	if retryCount > w.maxRetry {
		log.Printf("[worker] retry budget exhausted: task=%s retry=%d/%d",
			taskID, retryCount, w.maxRetry)
		w.moveToDLQ(ctx, task,
			fmt.Sprintf("retry exhausted after %d attempts (%s): %v", retryCount, errCode, cause))
		return
	}

	backoff := retryBackoff(w.retryBase, retryCount, 10*time.Second)
	log.Printf("[worker] retryable failure, will retry: task=%s code=%s retry=%d/%d backoff=%s",
		taskID, errCode, retryCount, w.maxRetry, backoff)

	w.requeueAfter(ctx, taskID, task.Priority, backoff)
}

// requeueAfter 退避后把任务放回队列。
//
// 元数据侧先回到 queued，再投 Redis：顺序反了的话，
// worker 可能在状态还是 retrying 时就领到任务，条件更新会拒绝它。
func (w *Worker) requeueAfter(ctx context.Context, taskID, priority string, delay time.Duration) {
	go func() {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}

		bg := context.Background()
		if err := w.meta.RequeueTask(bg, taskID); err != nil {
			log.Printf("[worker] meta requeue failed: task=%s err=%v", taskID, err)
			return
		}
		if err := w.Enqueue(bg, priority, taskID); err != nil {
			log.Printf("[worker] redis requeue failed: task=%s err=%v", taskID, err)
		}
	}()
}

// moveToDLQ 判死并收敛所属 Run
func (w *Worker) moveToDLQ(ctx context.Context, task *model.SyncTask, reason string) {
	if err := w.RDB.RPush(ctx, dlqQueue, task.ID).Err(); err != nil {
		log.Printf("[worker] push dlq failed: task=%s err=%v", task.ID, err)
	}
	if err := w.meta.MarkTaskDead(ctx, task.ID, reason); err != nil {
		log.Printf("[worker] mark dead failed: task=%s err=%v", task.ID, err)
		return
	}
	_ = w.RDB.HIncrBy(ctx, "metrics:tasks", "dead_total", 1).Err()

	log.Printf("[worker] moved to DLQ: task=%s run=%s reason=%s", task.ID, task.RunID, reason)

	// 有分片判死后 Run 才能收敛成 failed 或 partial，
	// 否则这次执行会永远停在 running
	job, err := w.meta.GetJob(ctx, task.JobID)
	if err != nil {
		log.Printf("[worker] load job for finalize failed: job=%s err=%v", task.JobID, err)
		return
	}
	w.finalizeRun(ctx, job, task.RunID)
}

// Enqueue 把任务投进对应优先级队列
func (w *Worker) Enqueue(ctx context.Context, priority string, taskID string) error {
	return w.RDB.RPush(ctx, queue.ForPriority(priority), taskID).Err()
}

// retryBackoff 指数退避 base * 2^(attempt-1)，上限 max
func retryBackoff(base time.Duration, attempt int, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 30 {
		return max
	}
	d := base * time.Duration(1<<uint(attempt-1))
	if d > max || d <= 0 {
		return max
	}
	return d
}
