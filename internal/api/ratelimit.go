package api

import (
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

type RateLimitConfig struct {
	Limit  int64
	Window time.Duration
	Prefix string
}

var rateLimitLua = redis.NewScript(`
-- KEYS[1] = zset key
-- ARGV[1] = now_ms
-- ARGV[2] = window_ms
-- ARGV[3] = limit
-- ARGV[4] = member

local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

redis.call("ZREMRANGEBYSCORE", key, 0, now - window)

local count = redis.call("ZCARD", key)
if count >= limit then
  local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
  local retry_ms = window
  if oldest[2] ~= nil then
    retry_ms = window - (now - tonumber(oldest[2]))
    if retry_ms < 0 then retry_ms = 0 end
  end
  return {0, count, retry_ms}
end

redis.call("ZADD", key, now, member)
redis.call("PEXPIRE", key, window + 1000)

count = count + 1
return {1, count, 0}
`)

func RateLimitPostTasks(rdb *redis.Client, cfg RateLimitConfig) gin.HandlerFunc {
	if cfg.Limit <= 0 || cfg.Window <= 0 || rdb == nil {
		return func(c *gin.Context) { c.Next() }
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "rl:tasks:"
	}

	return func(c *gin.Context) {
		ip := c.ClientIP()
		key := cfg.Prefix + ip

		nowMs := time.Now().UnixMilli()
		windowMs := cfg.Window.Milliseconds()
		member := fmt.Sprintf("%d-%d", nowMs, rand.Int63())

		res, err := rateLimitLua.Run(
			c.Request.Context(),
			rdb,
			[]string{key},
			nowMs, windowMs, cfg.Limit, member,
		).Result()

		if err != nil {
			// 限流系统故障：放行，避免把 /tasks 打死（降级策略）
			c.Next()
			return
		}

		arr, ok := res.([]interface{})
		if !ok || len(arr) < 3 {
			c.Next()
			return
		}

		allowed := toInt64(arr[0]) == 1
		count := toInt64(arr[1])
		retryMs := toInt64(arr[2])

		c.Header("X-RateLimit-Limit", strconv.FormatInt(cfg.Limit, 10))
		remain := cfg.Limit - count
		if remain < 0 {
			remain = 0
		}
		c.Header("X-RateLimit-Remaining", strconv.FormatInt(remain, 10))

		if !allowed {
			retrySec := int64(1)
			if retryMs > 0 {
				retrySec = (retryMs + 999) / 1000
			}
			c.Header("Retry-After", strconv.FormatInt(retrySec, 10))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":               "rate limit exceeded",
				"limit":               cfg.Limit,
				"window_seconds":      int64(cfg.Window.Seconds()),
				"retry_after_seconds": retrySec,
			})
			return
		}

		c.Next()
	}
}

func toInt64(v interface{}) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)
		return n
	case []byte:
		n, _ := strconv.ParseInt(string(t), 10, 64)
		return n
	default:
		return 0
	}
}
