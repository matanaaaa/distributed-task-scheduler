package worker

import (
	"context"
	"encoding/json"
	"log"
	"path/filepath"
	"time"

	"github.com/redis/go-redis/v9"
)

type QueueMsg struct {
	TaskID  string `json:"task_id"`
	ZipName string `json:"zip_name"`
	TaskType string `json:"task_type,omitempty"`
	Priority string `json:"priority,omitempty"`
}

type Worker struct {
	RDB *redis.Client
	apiBaseURL  string
	dataDir     string
	httpTimeout time.Duration
}

func New(rdb *redis.Client, apiBaseURL, dataDir string, httpTimeout time.Duration) *Worker {
	return &Worker{
		RDB:         rdb,
		apiBaseURL:  apiBaseURL,
		dataDir:     dataDir,
		httpTimeout: httpTimeout,
	}
}


func (w *Worker) Run(ctx context.Context) error {
	log.Printf("[worker] started. waiting for tasks... api=%s", w.apiBaseURL)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 阻塞取任务：先 high 再 normal
		res, err := w.RDB.BLPop(ctx, 0, "queue:high", "queue:normal").Result()
		if err != nil {
			// ctx cancel 时 BLPop 也会返回 error
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("[worker] BLPOP error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		// res = [queueName, value]
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

		if err := w.handleOne(ctx, msg); err != nil {
			log.Printf("[worker] task failed: task_id=%s err=%v", msg.TaskID, err)
			// 失败也不退出，继续拉下一个
		}
	}
}

func (w *Worker) handleOne(ctx context.Context, msg QueueMsg) error {
	taskID := msg.TaskID
	taskKey := "task:" + taskID

	// 1) 上报 running（HTTP）
	_ = w.reportStatus(taskID, statusPayload{
		Phase:    "running",
		Progress: 10,
		Msg:      "picked by worker",
		Status:   "running",
	})
	//方便验证观察
	time.Sleep(5 * time.Second)

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
	time.Sleep(2 * time.Second)

	// 4) 生成假的 result.zip
	resultZipPath, err := w.buildFakeResultZip(taskID)
	if err != nil {
		_ = w.reportStatus(taskID, statusPayload{
			Phase:    "completed_failed",
			Progress: 100,
			Msg:      "build result zip failed: " + err.Error(),
			Status:   "failed",
		})
		now = time.Now().UTC().Format(time.RFC3339)
		_ = w.RDB.HSet(ctx, taskKey, map[string]any{
			"status":     "failed",
			"phase":      "completed_failed",
			"progress":   "100",
			"msg":        "build result zip failed: " + err.Error(),
			"updated_at": now,
		}).Err()
		return err
	}
	//上传成功：删除本地文件
	_ = removeIfExists(resultZipPath)

	//方便验证观察
	time.Sleep(5 * time.Second)

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
		return err
	}

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

	log.Printf("[worker] done task_id=%s result=%s", taskID, filepath.Base(resultZipPath))
	return nil
}