package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	rdb *redis.Client
}

func NewRedisStore(addr string) *RedisStore {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisStore{rdb: rdb}
}

func (s *RedisStore) Ping(ctx context.Context) error {
	return s.rdb.Ping(ctx).Err()
}

func taskKey(id string) string { return fmt.Sprintf("task:%s", id) }

func (s *RedisStore) SaveTask(ctx context.Context, id string, fields map[string]any) error {
	return s.rdb.HSet(ctx, taskKey(id), fields).Err()
}

func (s *RedisStore) UpdateTask(ctx context.Context, id string, fields map[string]any) error {
    return s.rdb.HSet(ctx, taskKey(id), fields).Err()
}

func (s *RedisStore) GetTask(ctx context.Context, id string) (map[string]string, error) {
	return s.rdb.HGetAll(ctx, taskKey(id)).Result()
}

type QueueMessage struct {
	TaskID   string `json:"task_id"`
	ZipName  string `json:"zip_name"`
	TaskType string `json:"task_type"`
	Priority string `json:"priority"`
}

func (s *RedisStore) Enqueue(ctx context.Context, priority string, msg QueueMessage) error {
	q := "queue:normal"
	if priority == "high" {
		q = "queue:high"
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	// RPUSH queue:* {json}
	return s.rdb.RPush(ctx, q, string(b)).Err()
}

func (s *RedisStore) TouchUpdatedAt(ctx context.Context, id string) error {
	return s.rdb.HSet(ctx, taskKey(id), "updated_at", time.Now().UTC().Format(time.RFC3339)).Err()
}

func (s *RedisStore) Client() *redis.Client {
	return s.rdb
}
