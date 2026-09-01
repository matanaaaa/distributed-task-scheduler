package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// Healthz 检查两个真正的依赖：Redis（队列）与 MySQL（元数据）。
// 任一不可用整个平台就没法工作，所以都返回 503 而不是假装健康。
func (h *Handler) Healthz(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	redisOK := "ok"
	metaOK := "ok"
	var firstErr string

	if err := h.S.RDB.Ping(ctx).Err(); err != nil {
		redisOK = "failed"
		firstErr = "redis: " + err.Error()
	}
	if err := h.S.Meta.Ping(ctx); err != nil {
		metaOK = "failed"
		if firstErr == "" {
			firstErr = "meta_db: " + err.Error()
		}
	}

	if redisOK != "ok" || metaOK != "ok" {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "degraded",
			"redis":   redisOK,
			"meta_db": metaOK,
			"error":   firstErr,
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "redis": "ok", "meta_db": "ok"})
}

// Metrics 输出 Prometheus 文本格式。
// 只读 Redis，不查 MySQL：抓取频率高，不该给元数据库加负载。
func (h *Handler) Metrics(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	qHigh, _ := h.S.RDB.LLen(ctx, "queue:high").Result()
	qNormal, _ := h.S.RDB.LLen(ctx, "queue:normal").Result()
	qInflight, _ := h.S.RDB.LLen(ctx, "queue:inflight").Result()
	qDLQ, _ := h.S.RDB.LLen(ctx, "queue:dlq").Result()
	zProcSize, _ := h.S.RDB.ZCard(ctx, "z:processing").Result()

	m, _ := h.S.RDB.HGetAll(ctx, "metrics:tasks").Result()
	get := func(k string) int64 {
		n, _ := strconv.ParseInt(m[k], 10, 64)
		return n
	}

	c.Header("Content-Type", "text/plain; charset=utf-8")
	c.String(http.StatusOK, ""+
		"# HELP dts_queue_length 各队列当前长度\n"+
		"# TYPE dts_queue_length gauge\n"+
		fmt.Sprintf("dts_queue_length{queue=\"high\"} %d\n", qHigh)+
		fmt.Sprintf("dts_queue_length{queue=\"normal\"} %d\n", qNormal)+
		fmt.Sprintf("dts_queue_length{queue=\"inflight\"} %d\n", qInflight)+
		fmt.Sprintf("dts_queue_length{queue=\"dlq\"} %d\n", qDLQ)+
		"\n"+
		"# HELP dts_processing_lease_size 持有租约的分片数\n"+
		"# TYPE dts_processing_lease_size gauge\n"+
		fmt.Sprintf("dts_processing_lease_size %d\n", zProcSize)+
		"\n"+
		"# HELP dts_shards_processing 正在执行的分片数\n"+
		"# TYPE dts_shards_processing gauge\n"+
		fmt.Sprintf("dts_shards_processing %d\n", get("processing"))+
		"\n"+
		"# HELP dts_shards_total 分片终态累计数\n"+
		"# TYPE dts_shards_total counter\n"+
		fmt.Sprintf("dts_shards_total{status=\"success\"} %d\n", get("success_total"))+
		fmt.Sprintf("dts_shards_total{status=\"failed\"} %d\n", get("failed_total"))+
		fmt.Sprintf("dts_shards_total{status=\"dead\"} %d\n", get("dead_total"))+
		"\n"+
		"# HELP dts_timeout_requeue_total 租约超时被抢回队列的次数\n"+
		"# TYPE dts_timeout_requeue_total counter\n"+
		fmt.Sprintf("dts_timeout_requeue_total %d\n", get("timeout_requeue_total"))+
		"\n"+
		"# HELP dts_rows_total 同步行数累计\n"+
		"# TYPE dts_rows_total counter\n"+
		fmt.Sprintf("dts_rows_total{stage=\"read\"} %d\n", get("rows_read_total"))+
		fmt.Sprintf("dts_rows_total{stage=\"written\"} %d\n", get("rows_written_total"))+
		fmt.Sprintf("dts_rows_total{stage=\"failed\"} %d\n", get("rows_failed_total")),
	)
}
