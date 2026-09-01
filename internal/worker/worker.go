// Package worker 消费分片同步任务。
//
// 队列可靠性机制沿用原有设计，这部分与业务无关，换了 payload 依然成立：
//   - BRPOPLPUSH 把任务从待办队列原子搬到 inflight，进程崩了任务不丢
//   - z:processing 记租约到期时间，watchdog 负责把超时任务抢回队列
//   - lock:task:{id} 幂等锁，防止同一分片被两个 worker 同时执行
//   - 失败按错误类型决定重试或直接进 DLQ
//
// 变化的是执行体：从"解压任务包跑 run.sh"换成"跑同步管道"，
// 以及状态落点从 Redis 散字段换成 MySQL 元数据表。
package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/matanaaaa/distributed-task-scheduler/internal/meta"
	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
	"github.com/matanaaaa/distributed-task-scheduler/internal/queue"
	syncpipe "github.com/matanaaaa/distributed-task-scheduler/internal/sync"
)

const (
	qHigh     = queue.High
	qNormal   = queue.Normal
	qInflight = queue.Inflight
	zProc     = queue.Processing
)

type Worker struct {
	RDB  *redis.Client
	meta *meta.Store

	pipeline   *syncpipe.Pipeline
	reconciler *syncpipe.Reconciler

	workerID string

	concurrency int
	lockTTL     time.Duration
	maxRetry    int
	retryBase   time.Duration
}

func New(rdb *redis.Client, m *meta.Store) *Worker {
	host, _ := os.Hostname()

	return &Worker{
		RDB:         rdb,
		meta:        m,
		pipeline:    syncpipe.NewPipeline(m),
		reconciler:  syncpipe.NewReconciler(m),
		workerID:    fmt.Sprintf("%s-%d", host, os.Getpid()),
		concurrency: 4,
		lockTTL:     300 * time.Second,
		maxRetry:    3,
		retryBase:   1 * time.Second,
	}
}

func (w *Worker) SetConcurrency(n int) {
	if n <= 0 {
		n = 1
	}
	w.concurrency = n
}

func (w *Worker) SetLockTTL(d time.Duration) {
	if d <= 0 {
		d = 300 * time.Second
	}
	w.lockTTL = d
}

func (w *Worker) SetRetry(max int, base time.Duration) {
	if max < 0 {
		max = 0
	}
	if base <= 0 {
		base = 1 * time.Second
	}
	w.maxRetry = max
	w.retryBase = base
}

// visibilityTimeout 租约时长，与幂等锁 TTL 对齐：
// 锁还在就说明执行者还活着，租约不应该先到期
func (w *Worker) visibilityTimeout() time.Duration {
	return w.lockTTL
}

// claimOne 先高优后普通，各用 1 秒短超时轮询。
// BRPOPLPUSH 保证"出队"和"进 inflight"是一个原子动作。
func (w *Worker) claimOne(ctx context.Context) (taskID string, fromQ string, err error) {
	v, err := w.RDB.BRPopLPush(ctx, qHigh, qInflight, 1*time.Second).Result()
	if err == nil {
		return v, qHigh, nil
	}
	if !errors.Is(err, redis.Nil) {
		return "", "", err
	}

	v, err = w.RDB.BRPopLPush(ctx, qNormal, qInflight, 1*time.Second).Result()
	if err == nil {
		return v, qNormal, nil
	}
	if errors.Is(err, redis.Nil) {
		return "", "", nil
	}
	return "", "", err
}

func (w *Worker) markProcessing(ctx context.Context, taskID string) error {
	deadline := time.Now().Add(w.visibilityTimeout()).Unix()
	return w.RDB.ZAdd(ctx, zProc, redis.Z{
		Score:  float64(deadline),
		Member: taskID,
	}).Err()
}

// ackQueueState 从 inflight 与租约集合中摘除，成功失败都要调用，
// 否则 inflight 会堆积、watchdog 会误重投
func (w *Worker) ackQueueState(ctx context.Context, taskID string) {
	_ = w.RDB.LRem(ctx, qInflight, 1, taskID).Err()
	_ = w.RDB.ZRem(ctx, zProc, taskID).Err()
}

func (w *Worker) Run(ctx context.Context) error {
	log.Printf("[worker] started. worker_id=%s concurrency=%d lock_ttl=%s max_retry=%d",
		w.workerID, w.concurrency, w.lockTTL, w.maxRetry)

	jobs := make(chan string, w.concurrency*2)
	var wg sync.WaitGroup

	go w.watchdog(ctx)

	for i := 0; i < w.concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			for taskID := range jobs {
				err := w.handleOneSafely(ctx, taskID)

				// ack 必须区分所有权与放弃两种情形，不能无条件执行：
				// 误 ack 会删掉别人的租约，或让本该被重投的任务彻底消失
				bg := context.Background()

				switch {
				case errors.Is(err, errLockNotAcquired):
					// 分片归别的 worker，我们没有任何状态要清理。
					// 这里若 ack，ZREM 会摘掉真正执行者的租约。
					continue

				case errors.Is(err, context.Canceled), errors.Is(err, errLeaseLost):
					// 主动放弃：立刻还回队列，不等租约自然到期。
					// checkpoint 已落库，重新领取后从断点继续。
					log.Printf("[worker] consumer-%d giving up task, requeueing: task=%s err=%v",
						idx, taskID, err)
					w.abandonTask(bg, taskID)

				case err != nil:
					log.Printf("[worker] consumer-%d task failed: task=%s err=%v", idx, taskID, err)
					w.ackQueueState(bg, taskID)
					w.onTaskFailed(bg, taskID, err)

				default:
					w.ackQueueState(bg, taskID)
				}
			}
		}(i)
	}

	defer func() {
		close(jobs)
		wg.Wait()
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		taskID, fromQ, err := w.claimOne(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("[worker] claim error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if taskID == "" {
			continue
		}

		if err := w.markProcessing(ctx, taskID); err != nil {
			log.Printf("[worker] markProcessing failed, requeue: task=%s err=%v", taskID, err)
			w.ackQueueState(context.Background(), taskID)
			_ = w.RDB.RPush(context.Background(), fromQ, taskID).Err()
			continue
		}

		// 背压点：jobs 满了这里会阻塞，不会无限领取任务堆在内存里
		select {
		case jobs <- taskID:
		case <-ctx.Done():
			// 已经领进 inflight 但还没交给任何 consumer，
			// 停机前主动还回去，否则要干等一个租约周期
			w.abandonTask(context.Background(), taskID)
			return ctx.Err()
		}
	}
}

// handleOneSafely 把 panic 收敛成普通任务失败。
//
// 一个 worker 进程跑着多个分片，某条脏数据触发的 panic 不该让
// 其余分片一起陪葬。转成 error 之后走正常的重试/DLQ 流程，
// 真是代码 bug 的话会在重试耗尽后进 DLQ，不会被静默吞掉。
func (w *Worker) handleOneSafely(ctx context.Context, taskID string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic while handling task: %v\n%s", r, debug.Stack())
		}
	}()
	return w.handleOne(ctx, taskID)
}

// handleOne 执行一个分片同步任务
func (w *Worker) handleOne(ctx context.Context, taskID string) error {
	// 幂等锁：即使队列重复投递，同一分片也只会有一个执行者
	ok, err := w.acquireTaskLock(ctx, taskID, w.lockTTL)
	if err != nil {
		return fmt.Errorf("acquire task lock: %w", err)
	}
	if !ok {
		// 返回哨兵而不是 nil：调用方要靠它知道"这不是我的任务"，
		// 从而跳过 ack，不去动别人的 inflight 与租约
		log.Printf("[worker] skip task (lock held by another worker): task=%s", taskID)
		return errLockNotAcquired
	}
	defer w.releaseTaskLock(context.Background(), taskID)

	// 执行期间持续续租。taskCtx 会在失去租约时被 fence 掉，
	// 让管道尽快停手，避免与接手的 worker 交错写入。
	taskCtx, cancelTask := context.WithCancel(ctx)
	defer cancelTask()

	fenced := false
	stopHeartbeat := w.startHeartbeat(taskCtx, taskID, func() {
		fenced = true
		cancelTask()
	})
	defer stopHeartbeat()

	_ = w.RDB.HIncrBy(ctx, "metrics:tasks", "processing", 1).Err()
	defer func() {
		_ = w.RDB.HIncrBy(context.Background(), "metrics:tasks", "processing", -1).Err()
	}()

	// queued -> running，同时 attempt++。条件更新，状态不对就领不到。
	task, err := w.meta.ClaimTask(ctx, taskID)
	if err != nil {
		if errors.Is(err, meta.ErrNotClaimable) {
			// 已被别处推进或已终态，不是错误
			log.Printf("[worker] task not claimable, skip: task=%s (%v)", taskID, err)
			return nil
		}
		return fmt.Errorf("claim task: %w", err)
	}

	run, err := w.meta.GetRun(ctx, task.RunID)
	if err != nil {
		return fmt.Errorf("load run: %w", err)
	}
	job, err := w.meta.GetJob(ctx, task.JobID)
	if err != nil {
		return fmt.Errorf("load job: %w", err)
	}

	log.Printf("[worker] start shard: job=%s run=%s task=%s shard=%d [%s,%s) attempt=%d resume_from=%d",
		job.Name, run.ID, task.ID, task.ShardIndex, task.ShardLo, task.ShardHi,
		task.Attempt, task.Checkpoint.ID)

	res, err := w.pipeline.RunTask(taskCtx, job, run, task)
	if err != nil {
		// 失去租约导致的中断要报成 errLeaseLost，让调用方走"放弃并还回队列"
		// 而不是"失败并消耗重试预算"——这不是任务的错
		if fenced {
			return errLeaseLost
		}
		if errors.Is(err, context.Canceled) {
			return err
		}
		// running -> failed，重试与否交给 onTaskFailed 按错误类型判断
		if merr := w.meta.MarkTaskFailed(context.Background(), task.ID, err.Error()); merr != nil {
			log.Printf("[worker] mark failed error: task=%s err=%v", task.ID, merr)
		}
		return err
	}

	if err := w.meta.MarkTaskSuccess(ctx, task.ID, res.Checkpoint,
		res.RowsRead, res.RowsWritten, res.RowsFailed); err != nil {
		return fmt.Errorf("mark success: %w", err)
	}
	// 行级吞吐是同步平台最核心的指标，单独计数
	if pipe := w.RDB.Pipeline(); pipe != nil {
		pipe.HIncrBy(ctx, "metrics:tasks", "success_total", 1)
		pipe.HIncrBy(ctx, "metrics:tasks", "rows_read_total", res.RowsRead)
		pipe.HIncrBy(ctx, "metrics:tasks", "rows_written_total", res.RowsWritten)
		pipe.HIncrBy(ctx, "metrics:tasks", "rows_failed_total", res.RowsFailed)
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("[worker] metrics update failed: task=%s err=%v", task.ID, err)
		}
	}

	log.Printf("[worker] done shard: task=%s read=%d written=%d failed=%d checkpoint=%d",
		task.ID, res.RowsRead, res.RowsWritten, res.RowsFailed, res.Checkpoint.ID)

	w.finalizeRun(ctx, job, task.RunID)
	return nil
}

// finalizeRun 由分片结束事件驱动 Run 收敛。
//
// Run 状态不是累加出来的，而是每次都从 sync_tasks 重新聚合，
// 所以并发调用、重复调用都收敛到同一结果。
//
// 全部分片成功时才推进作业水位：只要有一片最终失败，
// 水位就不能前进，否则那一片的数据会被永久跳过。
func (w *Worker) finalizeRun(ctx context.Context, job *model.Job, runID string) {
	run, err := w.meta.ConvergeRun(ctx, runID)
	if err != nil {
		log.Printf("[worker] converge run failed: run=%s err=%v", runID, err)
		return
	}
	if !model.IsRunTerminal(run.Status) {
		return
	}

	log.Printf("[worker] run finished: run=%s status=%s shards=%d/%d read=%d written=%d failed=%d",
		run.ID, run.Status, run.ShardDone, run.ShardTotal,
		run.RowsRead, run.RowsWritten, run.RowsFailed)

	if run.Status == model.StatusSuccess && !run.WatermarkTo.IsZero() {
		if err := w.meta.AdvanceWatermark(ctx, job.ID, run.WatermarkTo); err != nil {
			log.Printf("[worker] advance watermark failed: job=%s err=%v", job.ID, err)
		}
	}

	// 对账。多个分片同时结束时可能重复对账一次，
	// 但结果按 (run_id, mode) upsert，重复执行不会产生脏数据。
	rc, err := w.reconciler.Check(ctx, job, run)
	if err != nil {
		log.Printf("[worker] reconcile error: run=%s err=%v", run.ID, err)
		return
	}
	log.Printf("[worker] reconciliation: run=%s mode=%s result=%s source=%d target=%d missing=%d extra=%d",
		run.ID, rc.Mode, rc.Result, rc.SourceCount, rc.TargetCount, rc.MissingCount, rc.ExtraCount)
}

// watchdog 把租约超时的分片抢回队列。
//
// 这是 worker 进程被 kill 之后任务还能完成的唯一保障：
// 崩溃的 worker 不会 ack，任务留在 inflight 里，租约到期后由这里捞回来。
func (w *Worker) watchdog(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		now := time.Now().Unix()
		ids, err := w.RDB.ZRangeByScore(ctx, zProc, &redis.ZRangeBy{
			Min:   "-inf",
			Max:   fmt.Sprintf("%d", now),
			Count: 50,
		}).Result()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[watchdog] zrangebyscore error: %v", err)
			continue
		}

		for _, taskID := range ids {
			// 优先级现在存在 MySQL 里，读不到就按普通队列走
			targetQ := qNormal
			if t, err := w.meta.GetTask(ctx, taskID); err == nil {
				if t.Priority == "high" {
					targetQ = qHigh
				}
				// 元数据侧也要退回 queued，否则条件更新会拒绝下一次领取
				if err := w.meta.RequeueTask(ctx, taskID); err != nil {
					log.Printf("[watchdog] meta requeue failed: task=%s err=%v", taskID, err)
				}
			} else {
				log.Printf("[watchdog] load task failed: task=%s err=%v", taskID, err)
			}

			res, err := requeueScript.Run(ctx, w.RDB,
				[]string{zProc, qInflight, targetQ, queue.MetricsHash},
				taskID, "timeout_requeue_total",
			).Result()
			if err != nil {
				log.Printf("[watchdog] requeue script error: task=%s err=%v", taskID, err)
				continue
			}
			log.Printf("[watchdog] lease expired: task=%s -> %s (%v)", taskID, targetQ, res)
		}
	}
}
