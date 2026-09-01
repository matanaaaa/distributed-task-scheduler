package model

import "fmt"

// taskTransitions SyncTask 的合法状态转换。
//
//	queued   --claim-->      running
//	running  --ok-->         success        (终态)
//	running  --err-->        failed
//	running  --lease timeout-> queued       (watchdog 重新入队)
//	failed   --retry-->      retrying
//	failed   --exhausted-->  dead           (终态，已进 DLQ)
//	retrying --backoff-->    queued
var taskTransitions = map[Status][]Status{
	StatusQueued:   {StatusRunning, StatusDead},
	StatusRunning:  {StatusSuccess, StatusFailed, StatusQueued},
	StatusFailed:   {StatusRetrying, StatusDead},
	StatusRetrying: {StatusQueued},
	StatusSuccess:  {},
	StatusDead:     {},
}

// runTransitions JobRun 的合法状态转换。
// partial 表示部分分片成功、部分最终失败，是同步场景里必须显式表达的结果。
var runTransitions = map[Status][]Status{
	StatusRunning: {StatusSuccess, StatusFailed, StatusPartial},
	StatusSuccess: {},
	StatusFailed:  {},
	StatusPartial: {},
}

// DeriveRunStatus 由分片统计推导 JobRun 应处的状态。
//
// 这段判定原来写在 SQL 的 CASE 表达式里，搬到 Go 层有两个好处：
// 一是可以直接单测，二是状态合法性的真相源只剩状态机这一处。
//
//	total == 0            空窗口，没有数据要同步，视为成功
//	done+failed < total   还有分片没跑完
//	failed == 0           全部成功
//	done   == 0           全部失败
//	其余                   部分成功，必须显式表达为 partial
func DeriveRunStatus(total, done, failed int) Status {
	if total <= 0 {
		return StatusSuccess
	}
	if done+failed < total {
		return StatusRunning
	}
	if failed == 0 {
		return StatusSuccess
	}
	if done == 0 {
		return StatusFailed
	}
	return StatusPartial
}

func canTransition(table map[Status][]Status, from, to Status) bool {
	allowed, ok := table[from]
	if !ok {
		return false
	}
	for _, s := range allowed {
		if s == to {
			return true
		}
	}
	return false
}

// TaskCanTransition 判断分片任务状态转换是否合法
func TaskCanTransition(from, to Status) bool {
	return canTransition(taskTransitions, from, to)
}

// RunCanTransition 判断 JobRun 状态转换是否合法
func RunCanTransition(from, to Status) bool {
	return canTransition(runTransitions, from, to)
}

// IsTaskTerminal 分片任务是否已到终态
func IsTaskTerminal(s Status) bool {
	return s == StatusSuccess || s == StatusDead
}

// IsRunTerminal JobRun 是否已到终态
func IsRunTerminal(s Status) bool {
	return s == StatusSuccess || s == StatusFailed || s == StatusPartial
}

// ErrIllegalTransition 描述一次被拒绝的状态转换
type ErrIllegalTransition struct {
	Kind string
	From Status
	To   Status
}

func (e *ErrIllegalTransition) Error() string {
	return fmt.Sprintf("illegal %s state transition: %s -> %s", e.Kind, e.From, e.To)
}

// CheckTaskTransition 校验分片任务状态转换，非法则返回错误
func CheckTaskTransition(from, to Status) error {
	if !TaskCanTransition(from, to) {
		return &ErrIllegalTransition{Kind: "task", From: from, To: to}
	}
	return nil
}

// CheckRunTransition 校验 JobRun 状态转换，非法则返回错误
func CheckRunTransition(from, to Status) error {
	if !RunCanTransition(from, to) {
		return &ErrIllegalTransition{Kind: "run", From: from, To: to}
	}
	return nil
}
