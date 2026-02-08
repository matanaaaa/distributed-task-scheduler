package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
	"sync"

	"github.com/redis/go-redis/v9"
)

type QueueMsg struct {
	TaskID    string `json:"task_id"`
	ZipName   string `json:"zip_name"`
	TaskType  string `json:"task_type,omitempty"`
	Priority  string `json:"priority,omitempty"`
}

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
}

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

func (w *Worker) Run(ctx context.Context) error {
	concurrency := w.concurrency

	log.Printf("[worker] started. waiting for tasks... api=%s worker_id=%s concurrency=%d",
		w.apiBaseURL, w.workerID, concurrency)

	jobs := make(chan QueueMsg, concurrency*2) // 有一点 buffer，形成背压但不至于太抖
	var wg sync.WaitGroup

	// consumers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			log.Printf("[worker] consumer-%d started", idx)

			for msg := range jobs {
				log.Printf("[worker] consumer-%d handling task_id=%s", idx, msg.TaskID)
				if err := w.handleOne(ctx, msg); err != nil {
					log.Printf("[worker] consumer-%d task failed: task_id=%s err=%v", idx, msg.TaskID, err)
					w.onTaskFailed(ctx, msg, err)
				}
			}

			log.Printf("[worker] consumer-%d stopped", idx)
		}(i)
	}

	// producer
	for {
		select {
		case <-ctx.Done():
			// 触发优雅退出：不再拉新任务，关闭 jobs，让 consumers 退出
			close(jobs)
			wg.Wait()
			return ctx.Err()
		default:
		}

		res, err := w.RDB.BLPop(ctx, 0, "queue:high", "queue:normal").Result()
		if err != nil {
			if ctx.Err() != nil {
				close(jobs)
				wg.Wait()
				return ctx.Err()
			}
			log.Printf("[worker] BLPOP error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if len(res) != 2 {
			continue
		}

		queueName := res[0]
		raw := res[1]

		var msg QueueMsg
		if err := json.Unmarshal([]byte(raw), &msg); err != nil {
			log.Printf("[worker] invalid queue msg: %v, raw=%s", err, raw)
			continue
		}
		if msg.TaskID == "" {
			log.Printf("[worker] missing task_id, raw=%s", raw)
			continue
		}

		log.Printf("[worker] picked task_id=%s from %s zip=%s", msg.TaskID, queueName, msg.ZipName)

		// 背压点：jobs 满了会阻塞 producer → 自然控吞吐
		select {
		case jobs <- msg:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		}
	}
}

func (w *Worker) handleOne(ctx context.Context, msg QueueMsg) error {
	taskID := msg.TaskID
	taskKey := "task:" + taskID

	// === 幂等锁：防并发重复执行 ===
	ok, err := w.acquireTaskLock(ctx, taskID, w.lockTTL)
	if err != nil {
		return fmt.Errorf("acquire task lock failed: %w", err)
	}
	if !ok {
		// 抢不到锁：说明同一任务正在被其它 worker 跑（或刚跑过/重试并发）
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

	attempt := 1 // 最小版先写 1；后面你要把 retryCount 传进来再改
	resultZipPath, err := w.buildResultZipFromTaskPackage(ctx, msg, attempt)

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
	if err := w.uploadResult(taskID, resultZipPath); err != nil {
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
