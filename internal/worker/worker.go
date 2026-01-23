package worker

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type QueueMsg struct {
	TaskID  string `json:"task_id"`
	ZipName string `json:"zip_name"`
}

type Worker struct {
	RDB *redis.Client
}

func New(rdb *redis.Client) *Worker {
	return &Worker{RDB: rdb}
}

func (w *Worker) Run(ctx context.Context) error {
	log.Println("[worker] started. waiting for tasks...")

	for {
		// 阻塞取任务：先 high 再 normal
		res, err := w.RDB.BLPop(ctx, 0, "queue:high", "queue:normal").Result()
		if err != nil {
			return err
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

		taskKey := "task:" + msg.TaskID
		now := time.Now().UTC().Format(time.RFC3339)

		// 1) running
		if err := w.RDB.HSet(ctx, taskKey, map[string]any{
			"status":     "running",
			"updated_at": now,
		}).Err(); err != nil {
			log.Printf("[worker] HSET running failed: %v task_id=%s", err, msg.TaskID)
			continue
		}

		log.Printf("[worker] picked task_id=%s from %s zip=%s", msg.TaskID, queueName, msg.ZipName)

		// 2) fake execute
		time.Sleep(2 * time.Second)

		// 3) success
		now = time.Now().UTC().Format(time.RFC3339)
		if err := w.RDB.HSet(ctx, taskKey, map[string]any{
			"status":     "success",
			"updated_at": now,
		}).Err(); err != nil {
			log.Printf("[worker] HSET success failed: %v task_id=%s", err, msg.TaskID)
			continue
		}

		log.Printf("[worker] done task_id=%s", msg.TaskID)
	}
}
