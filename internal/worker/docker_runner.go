package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func (w *Worker) runDocker(ctx context.Context, workDir, stdoutPath, stderrPath string) (exitCode int, timeout bool, err error) {
	stdoutFile, _ := os.Create(stdoutPath)
	stderrFile, _ := os.Create(stderrPath)
	defer stdoutFile.Close()
	defer stderrFile.Close()

	absWork, _ := filepath.Abs(workDir)
	hostPath := strings.ReplaceAll(absWork, `\`, `/`) // Windows friendly

	args := []string{
		"run", "--rm",
		"-v", fmt.Sprintf("%s:/work", hostPath),
		"-w", "/work",
		w.execImage, // 默认 ubuntu:22.04
		"bash", "run.sh",
	}

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile

	runErr := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return -1, true, fmt.Errorf("timeout")
	}
	if runErr != nil {
		if ee, ok := runErr.(*exec.ExitError); ok {
			return ee.ExitCode(), false, runErr
		}
		return -1, false, runErr
	}
	return 0, false, nil
}
