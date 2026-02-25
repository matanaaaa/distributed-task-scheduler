package store

import (
	"context"
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

func (s *RedisStore) Enqueue(ctx context.Context, priority string, id string) error {
  q := "queue:normal"
  if priority == "high" { q = "queue:high" }
  return s.rdb.RPush(ctx, q, id).Err()
}

func (s *RedisStore) TouchUpdatedAt(ctx context.Context, id string) error {
	return s.rdb.HSet(ctx, taskKey(id), "updated_at", time.Now().UTC().Format(time.RFC3339)).Err()
}

func (s *RedisStore) Client() *redis.Client {
	return s.rdb
}
