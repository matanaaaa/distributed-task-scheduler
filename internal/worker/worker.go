package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
	"sync"

	"github.com/redis/go-redis/v9"
)

type Worker struct {
	RDB         *redis.Client
	apiBaseURL  string
	dataDir     string
	httpTimeout time.Duration

	workerID string

	// configurable
	concurrency int
	lockTTL     time.Duration
	maxRetry    int
	retryBase   time.Duration

	execImage   string
	execTimeout time.Duration

	// unzip security boundaries
	unzipMaxBytes      int64 // total uncompressed limit
	unzipEntryMaxBytes int64 // per entry uncompressed limit
}

const (
	qHigh     = "queue:high"
	qNormal   = "queue:normal"
	qInflight = "queue:inflight"
	zProc     = "z:processing"
)

func New(rdb *redis.Client, apiBaseURL, dataDir string, httpTimeout time.Duration) *Worker {
	host, _ := os.Hostname()
	wid := fmt.Sprintf("%s-%d", host, os.Getpid())

	return &Worker{
		RDB:         rdb,
		apiBaseURL:  apiBaseURL,
		dataDir:     dataDir,
		httpTimeout: httpTimeout,
		workerID:    wid,

		concurrency: 4,
		lockTTL:     300 * time.Second,
		maxRetry:    3,
		retryBase:   1 * time.Second,
		execImage:   "ubuntu:22.04",
		execTimeout: 30 * time.Second,
		unzipMaxBytes:      128 << 20,
		unzipEntryMaxBytes: 32 << 20,
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

func (w *Worker) SetExec(image string, timeout time.Duration) {
	if image != "" {
		w.execImage = image
	}
	if timeout > 0 {
		w.execTimeout = timeout
	}
}

func (w *Worker) SetUnzipLimits(maxTotal, maxEntry int64) {
	if maxTotal <= 0 {
		maxTotal = 128 << 20
	}
	if maxEntry <= 0 {
		maxEntry = 32 << 20
	}

	if maxEntry > maxTotal {
		maxEntry = maxTotal
	}
	w.unzipMaxBytes = maxTotal
	w.unzipEntryMaxBytes = maxEntry
}

func (w *Worker) visibilityTimeout() time.Duration {
    // 直接跟锁 TTL 对齐
    return w.lockTTL
}

func (w *Worker) claimOne(ctx context.Context) (taskID string, fromQ string, err error) {
	// 先 high（短超时）
	v, err := w.RDB.BRPopLPush(ctx, qHigh, qInflight, 1*time.Second).Result()
	if err == nil {
		return v, qHigh, nil
	}
	if err != redis.Nil {
		return "", "", err
	}

	// 再 normal（短超时）
	v, err = w.RDB.BRPopLPush(ctx, qNormal, qInflight, 1*time.Second).Result()
	if err == nil {
		return v, qNormal, nil
	}
	if err == redis.Nil {
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

func (w *Worker) ackQueueState(ctx context.Context, taskID string) {
	_ = w.RDB.LRem(ctx, qInflight, 1, taskID).Err()
	_ = w.RDB.ZRem(ctx, zProc, taskID).Err()
}


func (w *Worker) Run(ctx context.Context) error {
	concurrency := w.concurrency

	log.Printf("[worker] started. waiting for tasks... api=%s worker_id=%s concurrency=%d",
		w.apiBaseURL, w.workerID, concurrency)

	jobs := make(chan string, concurrency*2)
	var wg sync.WaitGroup

	go w.watchdog(ctx)
	// consumers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			log.Printf("[worker] consumer-%d started", idx)

			for taskID := range jobs {
				log.Printf("[worker] consumer-%d handling task_id=%s", idx, taskID)

				if err := w.handleOne(ctx, taskID); err != nil {
					log.Printf("[worker] consumer-%d task failed: task_id=%s err=%v", idx, taskID, err)

					// 失败也要 ack（防 inflight 堆积 / watchdog 误重投）
					w.ackQueueState(context.Background(), taskID)

					w.onTaskFailed(ctx, taskID, err)
					continue
				}

				// 成功也要 ack
				w.ackQueueState(context.Background(), taskID)
			}

			log.Printf("[worker] consumer-%d stopped", idx)
		}(i)
	}

	// producer
	for {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		default:
		}

		taskID, fromQ, err := w.claimOne(ctx)
		if err != nil {
			if ctx.Err() != nil {
				close(jobs)
				wg.Wait()
				return ctx.Err()
			}
			log.Printf("[worker] BRPOPLPUSH error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if taskID == "" {
			// short-timeout miss
			continue
		}

		log.Printf("[worker] claimed task_id=%s from %s -> %s", taskID, fromQ, qInflight)

		// 立刻写 zset deadline
		if err := w.markProcessing(ctx, taskID); err != nil {
			log.Printf("[worker] markProcessing failed: task_id=%s err=%v", taskID, err)

			// 防止 inflight 卡住：清理后 requeue 回原队列
			w.ackQueueState(context.Background(), taskID)
			_ = w.RDB.RPush(context.Background(), fromQ, taskID).Err()
			continue
		}

		// 背压点：jobs 满了会阻塞 producer
		select {
		case jobs <- taskID:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		}
	}
}

func (w *Worker) handleOne(ctx context.Context, taskID string) error {
	taskKey := "task:" + taskID

	// === 幂等锁：防并发重复执行 ===
	ok, err := w.acquireTaskLock(ctx, taskID, w.lockTTL)
	if err != nil {
		return fmt.Errorf("acquire task lock failed: %w", err)
	}
	if !ok {
		// 抢不到锁：说明同一任务正在被其它 worker 跑
		log.Printf("[worker] skip task (lock not acquired): task_id=%s", taskID)
		return nil
	}
	// 用 Background，避免 ctx cancel 导致锁无法释放
	defer w.releaseTaskLock(context.Background(), taskID)

	// metrics（拿到锁才算真正开始处理）
	_ = w.RDB.HIncrBy(ctx, "metrics:tasks", "processing", 1).Err()
	defer func() {
		_ = w.RDB.HIncrBy(context.Background(), "metrics:tasks", "processing", -1).Err()
	}()

	// 故障注入：仅用于验证 retry/DLQ（默认关闭）
	if os.Getenv("ENABLE_FAIL_TEST") == "1" {
		t, _ := w.RDB.HGet(ctx, taskKey, "task_type").Result()
		if t == "fail" {
			return fmt.Errorf("forced fail for testing")
		}
	}


	// 1) 上报 running（HTTP）
	_ = w.reportStatus(taskID, statusPayload{
		Phase:    "running",
		Progress: 10,
		Msg:      "picked by worker",
		Status:   "running",
	})

	// 方便验证观察
	time.Sleep(1 * time.Second)

	// 2) 同步更新 Redis 状态
	now := time.Now().UTC().Format(time.RFC3339)
	_ = w.RDB.HSet(ctx, taskKey, map[string]any{
		"status":     "running",
		"phase":      "running",
		"progress":   "10",
		"msg":        "picked by worker",
		"updated_at": now,
	}).Err()

	// 3) 执行任务
	time.Sleep(1 * time.Second)

	attemptKey := "attempt"
	attemptN, aerr := w.RDB.HIncrBy(ctx, taskKey, attemptKey, 1).Result()
	if aerr != nil {
		attemptN = 1
	}
	attempt := int(attemptN)
	resultZipPath, err := w.buildResultZipFromTaskPackage(ctx, taskID, attempt)

	if err != nil {
		_ = w.reportStatus(taskID, statusPayload{
			Phase:    "completed_failed",
			Progress: 100,
			Msg:      err.Error(),
			Status:   "failed",
		})
		now = time.Now().UTC().Format(time.RFC3339)
		_ = w.RDB.HSet(ctx, taskKey, map[string]any{
			"status":     "failed",
			"phase":      "completed_failed",
			"progress":   "100",
			"msg":        err.Error(),
			"updated_at": now,
		}).Err()
		
		// 上传失败 zip
		if resultZipPath != "" {
			_ = w.reportStatus(taskID, statusPayload{
				Phase:    "uploading_result",
				Progress: 90,
				Msg:      "uploading result.zip (failed run)",
				Status:   "failed",
			})
			_ = w.RDB.HSet(ctx, taskKey, map[string]any{
			"msg": "failed, but result.zip uploaded",
			"updated_at": time.Now().UTC().Format(time.RFC3339),
			}).Err()

			log.Printf("[worker] about to upload (failed): task_id=%s path=%s", taskID, resultZipPath)
			if upErr := w.uploadResult(taskID, resultZipPath, attempt); upErr != nil {
				upErr = fmt.Errorf("upload result failed: %w", upErr)

				_ = w.reportStatus(taskID, statusPayload{
					Phase:    "completed_failed",
					Progress: 100,
					Msg:      upErr.Error(),
					Status:   "failed",
				})
				now = time.Now().UTC().Format(time.RFC3339)
				_ = w.RDB.HSet(ctx, taskKey, map[string]any{
					"status":     "failed",
					"phase":      "completed_failed",
					"progress":   "100",
					"msg":        upErr.Error(),
					"updated_at": now,
				}).Err()

				// 上传失败：保留本地 zip
				return upErr
			}

			// 上传成功：删本地 zip
			_ = removeIfExists(resultZipPath)
		}

		// build 的原始错误往上抛（用于 retry/DLQ 逻辑）
		return err
	}

	// 方便验证观察
	time.Sleep(1 * time.Second)

	// 5) 上报 uploading
	_ = w.reportStatus(taskID, statusPayload{
		Phase:    "uploading",
		Progress: 90,
		Msg:      "uploading result.zip",
		Status:   "running",
	})

	// 6) 上传 result.zip
	log.Printf("[worker] about to upload: task_id=%s path=%s", taskID, resultZipPath)
	if err := w.uploadResult(taskID, resultZipPath, attempt); err != nil {
		err = fmt.Errorf("upload result failed: %w", err)
		_ = w.reportStatus(taskID, statusPayload{
			Phase:    "completed_failed",
			Progress: 100,
			Msg:      "upload result failed: " + err.Error(),
			Status:   "failed",
		})
		now = time.Now().UTC().Format(time.RFC3339)
		_ = w.RDB.HSet(ctx, taskKey, map[string]any{
			"status":     "failed",
			"phase":      "completed_failed",
			"progress":   "100",
			"msg":        "upload result failed: " + err.Error(),
			"updated_at": now,
		}).Err()
		// 失败：保留本地zip，方便排查
		return err
	}

	// 上传成功：再删除本地文件
	_ = removeIfExists(resultZipPath)

	// 7) 成功：上报 success + 更新 redis
	_ = w.reportStatus(taskID, statusPayload{
		Phase:    "completed_success",
		Progress: 100,
		Msg:      "done",
		Status:   "success",
	})
	now = time.Now().UTC().Format(time.RFC3339)
	_ = w.RDB.HSet(ctx, taskKey, map[string]any{
		"status":     "success",
		"phase":      "completed_success",
		"progress":   "100",
		"msg":        "done",
		"updated_at": now,
	}).Err()

	// success_total
	_ = w.RDB.HIncrBy(ctx, "metrics:tasks", "success_total", 1).Err()

	log.Printf("[worker] done task_id=%s result=%s", taskID, filepath.Base(resultZipPath))
	return nil
}

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

        // 批量拿超时 taskID
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
        if len(ids) == 0 {
            continue
        }

        for _, taskID := range ids {
            taskKey := "task:" + taskID

            // 读 priority
            pri, _ := w.RDB.HGet(ctx, taskKey, "priority").Result()
            if pri != "high" {
                pri = "normal"
            }
            targetQ := qNormal
            if pri == "high" {
                targetQ = qHigh
            }

            // 使用 Lua 脚本执行原子操作
            luaScript := `
                local task_id = KEYS[1]
                local zset_key = KEYS[2]
                local inflight_key = KEYS[3]
                local target_queue = KEYS[4]
                
                local task_exists = redis.call('ZREM', zset_key, task_id)
                if task_exists == 0 then
                    return "task not found in zset"
                end
                
                local inflight_exists = redis.call('LREM', inflight_key, 1, task_id)
                if inflight_exists == 0 then
                    return "task not in inflight"
                end
                
                redis.call('RPUSH', target_queue, task_id)
                redis.call('HINCRBY', 'metrics:tasks', 'timeout_requeue_total', 1)
                return "task requeued"
            `
            result, err := w.RDB.Eval(ctx, luaScript, []string{taskID, zProc, qInflight, targetQ}).Result()
            if err != nil {
                log.Printf("[watchdog] Lua script error: %v", err)
                continue
            }
            log.Printf("[watchdog] Lua script result: %s", result)
        }
    }
}