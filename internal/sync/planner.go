package sync

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/matanaaaa/distributed-task-scheduler/internal/connector"
	"github.com/matanaaaa/distributed-task-scheduler/internal/meta"
	"github.com/matanaaaa/distributed-task-scheduler/internal/model"
)

// Enqueuer 把分片任务投进队列。
// 只依赖这一个方法，便于替换队列实现。
type Enqueuer interface {
	Enqueue(ctx context.Context, priority string, taskID string) error
}

// Planner 负责把一个作业展开成一次可执行的 Run
type Planner struct {
	meta *meta.Store
	q    Enqueuer
}

func NewPlanner(m *meta.Store, q Enqueuer) *Planner {
	return &Planner{meta: m, q: q}
}

// Trigger 触发一次同步。
//
// 步骤顺序是刻意的：
//  1. 先取源端当前水位定住本次窗口上界，之后所有分片共用它
//  2. 建 Run
//  3. 在窗口内切分片
//  4. 分片先落 MySQL，再入 Redis 队列
//
// 第 4 步顺序不能颠倒：如果先入队，worker 可能瞬间领到一个
// 元数据还不存在的任务 ID，只能报错重试，白跑一轮。
func (p *Planner) Trigger(ctx context.Context, job *model.Job, trigger model.TriggerType) (*model.JobRun, error) {
	if !job.Enabled {
		return nil, fmt.Errorf("job %s is disabled", job.Name)
	}

	src, err := connector.NewSource(job.SourceType, job.SourceConfig)
	if err != nil {
		return nil, err
	}
	defer src.Close()

	// 1) 定住窗口
	rng := connector.ReadRange{}
	if job.SyncMode == model.SyncModeIncremental {
		rng.From = job.Watermark
		to, err := src.CurrentWatermark(ctx, job)
		if err != nil {
			return nil, fmt.Errorf("read current watermark: %w", err)
		}
		rng.To = to
	}

	// 2) 建 Run
	now := time.Now().UTC()
	run := &model.JobRun{
		ID:            uuid.NewString(),
		JobID:         job.ID,
		TriggerType:   trigger,
		Status:        model.StatusRunning,
		SyncMode:      job.SyncMode,
		WatermarkFrom: rng.From,
		WatermarkTo:   rng.To,
		StartedAt:     now,
	}
	if err := p.meta.CreateRun(ctx, run); err != nil {
		return nil, err
	}

	// 3) 切分片
	shards, err := src.Split(ctx, job, rng)
	if err != nil {
		_ = p.meta.FailRun(ctx, run.ID, fmt.Sprintf("split failed: %v", err))
		return nil, fmt.Errorf("split: %w", err)
	}

	// 窗口内没有数据：直接收敛为成功，并把水位推到上界
	if len(shards) == 0 {
		if err := p.meta.SetRunShardTotal(ctx, run.ID, 0); err != nil {
			return nil, err
		}
		converged, err := p.meta.ConvergeRun(ctx, run.ID)
		if err != nil {
			return nil, err
		}
		if converged.Status == model.StatusSuccess && !rng.To.IsZero() {
			if err := p.meta.AdvanceWatermark(ctx, job.ID, rng.To); err != nil {
				return nil, err
			}
		}
		log.Printf("[planner] job=%s run=%s no data in window, converged to %s",
			job.Name, run.ID, converged.Status)
		return converged, nil
	}

	// 4a) 分片落库
	priority := job.Priority
	if priority != "high" {
		priority = "normal"
	}

	tasks := make([]model.SyncTask, 0, len(shards))
	for _, sh := range shards {
		tasks = append(tasks, model.SyncTask{
			ID:         uuid.NewString(),
			RunID:      run.ID,
			JobID:      job.ID,
			ShardIndex: sh.Index,
			ShardLo:    sh.Lo,
			ShardHi:    sh.Hi,
			Status:     model.StatusQueued,
			Priority:   priority,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
	}
	if err := p.meta.CreateTasks(ctx, tasks); err != nil {
		_ = p.meta.FailRun(ctx, run.ID, fmt.Sprintf("create tasks failed: %v", err))
		return nil, err
	}
	if err := p.meta.SetRunShardTotal(ctx, run.ID, len(tasks)); err != nil {
		return nil, err
	}

	// 4b) 入队
	enqueued := 0
	for i := range tasks {
		if err := p.q.Enqueue(ctx, priority, tasks[i].ID); err != nil {
			// 入队失败的分片留在 queued 状态。Run 不会收敛，
			// 这是有意的：宁可挂着等人处理，也不要假装成功。
			log.Printf("[planner] enqueue failed: run=%s task=%s err=%v", run.ID, tasks[i].ID, err)
			continue
		}
		enqueued++
	}
	if enqueued == 0 {
		_ = p.meta.FailRun(ctx, run.ID, "no task could be enqueued")
		return nil, fmt.Errorf("enqueue: all %d shards failed to enqueue", len(tasks))
	}

	log.Printf("[planner] job=%s run=%s mode=%s shards=%d enqueued=%d window=(%s,%s]->(%s,%s]",
		job.Name, run.ID, job.SyncMode, len(tasks), enqueued,
		rng.From.Value, fmt.Sprint(rng.From.ID), rng.To.Value, fmt.Sprint(rng.To.ID))

	return run, nil
}
