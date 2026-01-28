package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// 每个任务一个锁：lock:task:{id}
func (w *Worker) lockKey(taskID string) string {
	return fmt.Sprintf("lock:task:%s", taskID)
}

// SET lock:task:{id} <worker_id> NX EX <ttl>
func (w *Worker) acquireTaskLock(ctx context.Context, taskID string, ttl time.Duration) (bool, error) {
	key := w.lockKey(taskID)

	ok, err := w.RDB.SetNX(ctx, key, w.workerID, ttl).Result()
	if err != nil {
		return false, err
	}

	if ok {
		log.Printf("[worker] lock acquired task_id=%s key=%s ttl=%s owner=%s", taskID, key, ttl, w.workerID)
	} else {
		log.Printf("[worker] lock not acquired (skip) task_id=%s key=%s", taskID, key)
	}
	return ok, nil
}

// 安全释放：只有 value == workerID 才 DEL，避免删到别人的锁
var releaseLockLua = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`)

func (w *Worker) releaseTaskLock(ctx context.Context, taskID string) {
	key := w.lockKey(taskID)
	_, err := releaseLockLua.Run(ctx, w.RDB, []string{key}, w.workerID).Result()
	if err != nil {
		log.Printf("[worker] lock release error task_id=%s key=%s err=%v", taskID, key, err)
		return
	}
	log.Printf("[worker] lock released task_id=%s key=%s owner=%s", taskID, key, w.workerID)
}
