package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func (w *Worker) runDocker(ctx context.Context, taskID string, attempt int, workDir, stdoutPath, stderrPath string) (exitCode int, timeout bool, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, w.execTimeout)
	defer cancel()

	// 1) stdout/stderr file
	if err := os.MkdirAll(filepath.Dir(stdoutPath), 0755); err != nil {
		return -1, false, fmt.Errorf("mkdir stdout dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(stderrPath), 0755); err != nil {
		return -1, false, fmt.Errorf("mkdir stderr dir: %w", err)
	}

	stdoutFile, err := os.Create(stdoutPath)
	if err != nil {
		return -1, false, fmt.Errorf("create stdout.log: %w", err)
	}
	defer stdoutFile.Close()

	stderrFile, err := os.Create(stderrPath)
	if err != nil {
		return -1, false, fmt.Errorf("create stderr.log: %w", err)
	}
	defer stderrFile.Close()

	// 2) workdir path
	absWork, err := filepath.Abs(workDir)
	if err != nil {
		return -1, false, fmt.Errorf("abs workdir: %w", err)
	}
	hostPath := strings.ReplaceAll(absWork, `\`, `/`)

	// 3) container name: task_<taskID>_attempt_<attempt>
	// docker name 只能用 [a-zA-Z0-9][a-zA-Z0-9_.-]，UUID 里的 '-' 没问题
	containerName := fmt.Sprintf("task_%s_attempt_%d", taskID, attempt)

	// 4) docker args
	args := []string{
		"run", "--rm",
		"--name", containerName,

		// (2) 默认禁网：减少攻击面/外部依赖
		"--network", "none",

		// (3) 资源限制：防卡死/防炸机器
		"--cpus", "1",
		"--memory", "512m",

		"-v", fmt.Sprintf("%s:/work", hostPath),
		"-w", "/work",
		w.execImage,
		"bash", "run.sh",
	}

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile

	runErr := cmd.Run()

	// 5) 超时：明确标记 + 兜底强杀容器
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		_ = exec.Command("docker", "rm", "-f", containerName).Run()
		return -1, true, fmt.Errorf("timeout: %w", runErr)
	}

	// 6) 正常错误：拿 exit code
	if runErr != nil {
		var ee *exec.ExitError
		if errors.As(runErr, &ee) {
			return ee.ExitCode(), false, runErr
		}
		return -1, false, runErr
	}

	return 0, false, nil
}

