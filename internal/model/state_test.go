package model

import "testing"

func TestTaskTransitions(t *testing.T) {
	legal := [][2]Status{
		{StatusQueued, StatusRunning},
		{StatusRunning, StatusSuccess},
		{StatusRunning, StatusFailed},
		// watchdog 发现租约超时，把任务抢回队列
		{StatusRunning, StatusQueued},
		{StatusFailed, StatusRetrying},
		{StatusFailed, StatusDead},
		{StatusRetrying, StatusQueued},
	}
	for _, c := range legal {
		if !TaskCanTransition(c[0], c[1]) {
			t.Errorf("expected %s -> %s to be legal", c[0], c[1])
		}
	}

	illegal := [][2]Status{
		// 终态不可再流转
		{StatusSuccess, StatusRunning},
		{StatusDead, StatusQueued},
		// 不能跳过 running 直接成功
		{StatusQueued, StatusSuccess},
		// 不能从失败直接跳成功，必须重新入队重跑
		{StatusFailed, StatusSuccess},
		{StatusRetrying, StatusRunning},
		// run 级状态不属于任务状态机
		{StatusRunning, StatusPartial},
	}
	for _, c := range illegal {
		if TaskCanTransition(c[0], c[1]) {
			t.Errorf("expected %s -> %s to be illegal", c[0], c[1])
		}
	}
}

func TestRunTransitions(t *testing.T) {
	for _, to := range []Status{StatusSuccess, StatusFailed, StatusPartial} {
		if !RunCanTransition(StatusRunning, to) {
			t.Errorf("expected run running -> %s to be legal", to)
		}
	}
	if RunCanTransition(StatusSuccess, StatusRunning) {
		t.Error("run success is terminal, must not go back to running")
	}
	if RunCanTransition(StatusRunning, StatusRetrying) {
		t.Error("retrying is a task-level state, not a run state")
	}
}

func TestTerminalHelpers(t *testing.T) {
	if !IsTaskTerminal(StatusSuccess) || !IsTaskTerminal(StatusDead) {
		t.Error("success and dead must be task terminal states")
	}
	if IsTaskTerminal(StatusFailed) {
		t.Error("failed is not terminal: it can still be retried")
	}
	if !IsRunTerminal(StatusPartial) {
		t.Error("partial must be a run terminal state")
	}
	if IsRunTerminal(StatusRunning) {
		t.Error("running is not a run terminal state")
	}
}

func TestCheckTransitionReturnsTypedError(t *testing.T) {
	err := CheckTaskTransition(StatusSuccess, StatusRunning)
	if err == nil {
		t.Fatal("expected error for illegal transition")
	}
	var ill *ErrIllegalTransition
	if v, ok := err.(*ErrIllegalTransition); ok {
		ill = v
	} else {
		t.Fatalf("expected *ErrIllegalTransition, got %T", err)
	}
	if ill.From != StatusSuccess || ill.To != StatusRunning {
		t.Errorf("error carries wrong states: %+v", ill)
	}
	if CheckTaskTransition(StatusQueued, StatusRunning) != nil {
		t.Error("legal transition must not return error")
	}
}

func TestWatermarkIsZero(t *testing.T) {
	if !(Watermark{}).IsZero() {
		t.Error("empty watermark must be zero")
	}
	if (Watermark{ID: 1}).IsZero() {
		t.Error("watermark with ID must not be zero")
	}
	if (Watermark{Value: "2026-01-01"}).IsZero() {
		t.Error("watermark with Value must not be zero")
	}
}

func TestDeriveRunStatus(t *testing.T) {
	cases := []struct {
		name   string
		total  int
		done   int
		failed int
		want   Status
	}{
		{"空窗口视为成功", 0, 0, 0, StatusSuccess},
		{"还有分片在跑", 4, 1, 0, StatusRunning},
		{"有失败但未跑完", 4, 1, 1, StatusRunning},
		{"全部成功", 4, 4, 0, StatusSuccess},
		{"全部失败", 4, 0, 4, StatusFailed},
		{"部分成功必须显式表达", 4, 3, 1, StatusPartial},
		{"单分片成功", 1, 1, 0, StatusSuccess},
		{"单分片失败", 1, 0, 1, StatusFailed},
		{"负数当空处理", -1, 0, 0, StatusSuccess},
	}
	for _, c := range cases {
		if got := DeriveRunStatus(c.total, c.done, c.failed); got != c.want {
			t.Errorf("%s: DeriveRunStatus(%d,%d,%d) = %s, want %s",
				c.name, c.total, c.done, c.failed, got, c.want)
		}
	}
}

func TestDeriveRunStatusResultIsAlwaysReachable(t *testing.T) {
	// 推导出来的状态必须是 running 能合法转换过去的，
	// 否则 ConvergeRun 会在状态机校验处被自己拒掉
	for total := 1; total <= 5; total++ {
		for done := 0; done <= total; done++ {
			for failed := 0; failed+done <= total; failed++ {
				got := DeriveRunStatus(total, done, failed)
				if got == StatusRunning {
					continue
				}
				if !RunCanTransition(StatusRunning, got) {
					t.Fatalf("DeriveRunStatus(%d,%d,%d) = %s is not reachable from running",
						total, done, failed, got)
				}
			}
		}
	}
}
