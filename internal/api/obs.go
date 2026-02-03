package api

import (
    "context"
    "fmt"
    "net/http"
    "os"
    "path/filepath"
    "time"

    "github.com/gin-gonic/gin"
)

func (h *Handler) Healthz(c *gin.Context) {
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()

    // 1) redis ping
    if err := h.S.Store.Ping(ctx); err != nil {
        c.JSON(http.StatusServiceUnavailable, gin.H{
            "status": "degraded",
            "redis":  "failed",
            "disk":   "unknown",
            "error":  err.Error(),
        })
        return
    }

    // 2) disk writable: mkdir + temp file
    if err := ensureDirs(h.S.DataDir); err != nil {
        c.JSON(http.StatusServiceUnavailable, gin.H{
            "status": "degraded",
            "redis":  "ok",
            "disk":   "failed",
            "error":  err.Error(),
        })
        return
    }

    tmpDir := filepath.Join(h.S.DataDir, "tmp")
    if err := os.MkdirAll(tmpDir, 0o755); err != nil {
        c.JSON(http.StatusServiceUnavailable, gin.H{
            "status": "degraded",
            "redis":  "ok",
            "disk":   "failed",
            "error":  err.Error(),
        })
        return
    }

    f, err := os.CreateTemp(tmpDir, "healthz-*.tmp")
    if err != nil {
        c.JSON(http.StatusServiceUnavailable, gin.H{
            "status": "degraded",
            "redis":  "ok",
            "disk":   "failed",
            "error":  err.Error(),
        })
        return
    }
    name := f.Name()
    _ = f.Close()
    _ = os.Remove(name)

    c.JSON(http.StatusOK, gin.H{"status": "ok", "redis": "ok", "disk": "ok"})
}

func (h *Handler) Metrics(c *gin.Context) {
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()

    // queue lengths
    qHigh, _ := h.S.RDB.LLen(ctx, "queue:high").Result()
    qNormal, _ := h.S.RDB.LLen(ctx, "queue:normal").Result()
    qDLQ, _ := h.S.RDB.LLen(ctx, "queue:dlq").Result()

    // counters
    m, _ := h.S.RDB.HGetAll(ctx, "metrics:tasks").Result()

    // helper: string->int64
    get := func(k string) int64 {
        v := m[k]
        if v == "" {
            return 0
        }
        // 最小：不严格处理 parse error
        var n int64
        fmt.Sscanf(v, "%d", &n)
        return n
    }

    processing := get("processing")
    success := get("success_total")
    failed := get("failed_total")
    dead := get("dead_total")

    c.Header("Content-Type", "text/plain; charset=utf-8")

    // Prometheus-like format
    c.String(http.StatusOK, ""+
        "# TYPE dts_queue_length gauge\n"+
        fmt.Sprintf("dts_queue_length{queue=\"high\"} %d\n", qHigh)+
        fmt.Sprintf("dts_queue_length{queue=\"normal\"} %d\n", qNormal)+
        fmt.Sprintf("dts_queue_length{queue=\"dlq\"} %d\n", qDLQ)+
        "\n"+
        "# TYPE dts_tasks_processing gauge\n"+
        fmt.Sprintf("dts_tasks_processing %d\n", processing)+
        "\n"+
        "# TYPE dts_tasks_total counter\n"+
        fmt.Sprintf("dts_tasks_total{status=\"success\"} %d\n", success)+
        fmt.Sprintf("dts_tasks_total{status=\"failed\"} %d\n", failed)+
        fmt.Sprintf("dts_tasks_total{status=\"dead\"} %d\n", dead),
    )
}
