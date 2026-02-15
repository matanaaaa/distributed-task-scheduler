package worker

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"errors"
	"os/exec"
)

type execMeta struct {
	TaskID      string `json:"task_id"`
	Attempt     int    `json:"attempt"`
	ExitCode    int    `json:"exit_code"`
	Timeout     bool   `json:"timeout"`
	DurationMs  int64  `json:"duration_ms"`
	Error       string `json:"error,omitempty"`
	ExecMode  string `json:"exec_mode"`
	ExecImage   string `json:"exec_image,omitempty"`
}

func (w *Worker) buildResultZipFromTaskPackage(ctx context.Context, msg QueueMsg, attempt int) (string, error) {
	taskID := msg.TaskID

	// 1) workdir
	workDir := filepath.Join(w.dataDir, "work", taskID)
	_ = os.RemoveAll(workDir)
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return "", err
	}

	// result zip path
	resultZipPath := filepath.Join(w.dataDir, "tmp", fmt.Sprintf("%s_result.zip", taskID))
	if err := os.MkdirAll(filepath.Dir(resultZipPath), 0755); err != nil {
		return "", err
	}

	// 提前读取use_docker
	useDockerStr, _ := w.RDB.HGet(ctx, "task:"+taskID, "use_docker").Result()
	useDocker := strings.ToLower(strings.TrimSpace(useDockerStr)) == "true"

	// meta字段
	execMode := "local"
	execImage := "local"
	if useDocker {
		execMode = "docker"
		execImage = w.execImage
	}

	// 失败收尾：写 metadata + touch logs + zipResult
	finalizeFailure := func(cause error) (string, error) {
		_ = os.MkdirAll(filepath.Join(workDir, "output"), 0755)
		_ = ensureFile(filepath.Join(workDir, "stdout.log"))
		_ = ensureFile(filepath.Join(workDir, "stderr.log"))

		meta := execMeta{
			TaskID:     taskID,
			Attempt:    attempt,
			ExitCode:   -1,
			Timeout:    false,
			DurationMs: 0,
			ExecMode:   execMode,
			ExecImage:  execImage,
			Error:      cause.Error(),
		}
		_ = writeJSON(filepath.Join(workDir, "metadata.json"), meta)

		if zipErr := zipResult(resultZipPath, workDir, []string{
			"output",
			"stdout.log",
			"stderr.log",
			"metadata.json",
		}); zipErr != nil {
			return "", fmt.Errorf("%w (and zipResult failed: %v)", cause, zipErr)
		}
		return resultZipPath, cause
	}

	// 2) download task zip -> workdir/task.zip
	taskZipPath := filepath.Join(workDir, "task.zip")
	if err := w.downloadTaskZip(ctx, taskID, taskZipPath); err != nil {
		return finalizeFailure(fmt.Errorf("download task zip: %w", err))
	}

	if err := unzipToDir(taskZipPath, workDir, w.unzipMaxBytes, w.unzipEntryMaxBytes); err != nil {
		return finalizeFailure(fmt.Errorf("unzip task zip: %w", err))
	}

	runSh := filepath.Join(workDir, "run.sh")
	if _, err := os.Stat(runSh); err != nil {
		return finalizeFailure(fmt.Errorf("task contract violated: missing run.sh"))
	}

	// 5) ensure output dir exists
	outputDir := filepath.Join(workDir, "output")
	_ = os.MkdirAll(outputDir, 0755)

	// 6) run
	stdoutPath := filepath.Join(workDir, "stdout.log")
	stderrPath := filepath.Join(workDir, "stderr.log")

	var exitCode int
	var timeout bool
	var runErr error

	start := time.Now()
	if useDocker {
		exitCode, timeout, runErr = w.runDocker(ctx, taskID, attempt, workDir, stdoutPath, stderrPath)
	} else {
		exitCode, timeout, runErr = w.runLocal(ctx, workDir, stdoutPath, stderrPath)
	}
	dur := time.Since(start)

	meta := execMeta{
		TaskID:     taskID,
		Attempt:    attempt,
		ExitCode:   exitCode,
		Timeout:    timeout,
		DurationMs: dur.Milliseconds(),
		ExecMode:   execMode,
		ExecImage:  execImage,
	}
	if runErr != nil {
		meta.Error = runErr.Error()
	}
	_ = writeJSON(filepath.Join(workDir, "metadata.json"), meta)

	// 7) pack result.zip
	if err := zipResult(resultZipPath, workDir, []string{
		"output",
		"stdout.log",
		"stderr.log",
		"metadata.json",
	}); err != nil {
		return "", err
	}

	// 8) if run failed, return zip + error
	if runErr != nil || exitCode != 0 || timeout {
		return resultZipPath, fmt.Errorf("task exec failed: exit_code=%d timeout=%v err=%v", exitCode, timeout, runErr)
	}
	return resultZipPath, nil
}



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

func (w *Worker) runLocal(ctx context.Context, workDir, stdoutPath, stderrPath string) (exitCode int, timeout bool, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, w.execTimeout)
	defer cancel()

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

	cmd := exec.CommandContext(ctx, "bash", "run.sh")
	cmd.Dir = workDir
	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile

	runErr := cmd.Run()

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return -1, true, fmt.Errorf("timeout: %w", runErr)
	}
	if runErr != nil {
		var ee *exec.ExitError
		if errors.As(runErr, &ee) {
			return ee.ExitCode(), false, runErr
		}
		return -1, false, runErr
	}
	return 0, false, nil
}


// -------- helpers --------

func ensureFile(path string) error {
	// touch empty file if not exists
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	return f.Close()
}

func (w *Worker) downloadTaskZip(ctx context.Context, taskID, dstPath string) error {
	url := fmt.Sprintf("%s/tasks/%s/download", w.apiBaseURL, taskID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := w.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("download api failed: code=%d body=%s", resp.StatusCode, string(body))
	}

	f, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	return err
}

func unzipToDir(zipPath, dstDir string , maxTotalBytes, maxEntryBytes int64) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	absDst, err := filepath.Abs(dstDir)
	if err != nil {
		return fmt.Errorf("abs dst dir: %w", err)
	}
	dstPrefix := absDst + string(os.PathSeparator)

	var totalWritten int64

	for _, f := range r.File {
		name := f.Name
		if name == "" {
			return fmt.Errorf("zip-slip detected: empty entry name")
		}
		// Reject absolute paths
		if strings.HasPrefix(name, "/") || strings.HasPrefix(name, "\\") {
			return fmt.Errorf("zip-slip detected: absolute path entry: %q", name)
		}
		// Reject Windows drive letter paths (C:\...)
		if len(name) >= 2 && name[1] == ':' {
			return fmt.Errorf("zip-slip detected: drive letter entry: %q", name)
		}

		cleanName := filepath.Clean(name)
		fullPath := filepath.Join(absDst, cleanName)

		absFull, err := filepath.Abs(fullPath)
		if err != nil {
			return fmt.Errorf("abs full path: %w", err)
		}
		// Must stay within dstDir
		if absFull != absDst && !strings.HasPrefix(absFull, dstPrefix) {
			return fmt.Errorf("zip-slip detected: entry %q escapes dst", name)
		}

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(absFull, 0755); err != nil {
				return err
			}
			continue
		}

		// Declared size guard (if available)
		if f.UncompressedSize64 > uint64(maxEntryBytes) {
			return fmt.Errorf("zip entry too large: %q (%d bytes > %d)", name, f.UncompressedSize64, maxEntryBytes)
		}

		if err := os.MkdirAll(filepath.Dir(absFull), 0755); err != nil {
			return err
		}

		in, err := f.Open()
		if err != nil {
			return err
		}
		out, err := os.OpenFile(absFull, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			_ = in.Close()
			return err
		}

		// Hard limit while extracting (protect against zip bombs)
		limited := io.LimitReader(in, maxEntryBytes+1)
		written, copyErr := io.Copy(out, limited)

		_ = out.Close()
		_ = in.Close()

		if copyErr != nil {
			return copyErr
		}
		if written > maxEntryBytes {
			return fmt.Errorf("zip entry too large while extracting: %q (> %d bytes)", name, maxEntryBytes)
		}

		totalWritten += written
		if totalWritten > maxTotalBytes {
			return fmt.Errorf("zip too large after unzip: total=%d limit=%d", totalWritten, maxTotalBytes)
		}
	}

	return nil
}


func writeJSON(path string, v any) error {
	b, _ := json.MarshalIndent(v, "", "  ")
	return os.WriteFile(path, b, 0644)
}

func zipResult(zipPath, workDir string, items []string) error {
	zf, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer zf.Close()

	zw := zip.NewWriter(zf)
	defer zw.Close()

	for _, it := range items {
		full := filepath.Join(workDir, it)
		if _, err := os.Stat(full); err != nil {
			// 不存在就跳过
			continue
		}
		if err := addPathToZip(zw, workDir, full); err != nil {
			return err
		}
	}
	return nil
}

func addPathToZip(zw *zip.Writer, baseDir, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return filepath.Walk(path, func(p string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if fi.IsDir() {
				return nil
			}
			return addFileToZip(zw, baseDir, p)
		})
	}
	return addFileToZip(zw, baseDir, path)
}

func addFileToZip(zw *zip.Writer, baseDir, filePath string) error {
	rel, err := filepath.Rel(baseDir, filePath)
	if err != nil {
		return err
	}
	rel = filepath.ToSlash(rel)

	fw, err := zw.Create(rel)
	if err != nil {
		return err
	}
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(fw, f)
	return err
}

func execCtxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
