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
)

type execMeta struct {
	TaskID      string `json:"task_id"`
	Attempt     int    `json:"attempt"`
	ExitCode    int    `json:"exit_code"`
	Timeout     bool   `json:"timeout"`
	DurationMs  int64  `json:"duration_ms"`
	Error       string `json:"error,omitempty"`
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

	// result zip path 先确定（失败也要落这个）
	resultZipPath := filepath.Join(w.dataDir, "tmp", fmt.Sprintf("%s_result.zip", taskID))
	if err := os.MkdirAll(filepath.Dir(resultZipPath), 0755); err != nil {
		return "", err
	}

	// 统一失败收尾：写 metadata + touch logs + zipResult
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
			ExecImage:  w.execImage,
			Error:      cause.Error(),
		}
		_ = writeJSON(filepath.Join(workDir, "metadata.json"), meta)

		// 即使 zip 失败，也要把原 cause 往上抛；zipErr 只做补充信息
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

	if err := unzipToDir(taskZipPath, workDir); err != nil {
		return finalizeFailure(fmt.Errorf("unzip task zip: %w", err))
	}

	runSh := filepath.Join(workDir, "run.sh")
	if _, err := os.Stat(runSh); err != nil {
		return finalizeFailure(fmt.Errorf("task contract violated: missing run.sh"))
	}

	// 5) ensure output dir exists
	outputDir := filepath.Join(workDir, "output")
	_ = os.MkdirAll(outputDir, 0755)

	// 6) run in docker
	stdoutPath := filepath.Join(workDir, "stdout.log")
	stderrPath := filepath.Join(workDir, "stderr.log")

	start := time.Now()
	exitCode, timeout, runErr := w.runDocker(ctx, taskID, attempt, workDir, stdoutPath, stderrPath)
	dur := time.Since(start)

	meta := execMeta{
		TaskID:     taskID,
		Attempt:    attempt,
		ExitCode:   exitCode,
		Timeout:    timeout,
		DurationMs: dur.Milliseconds(),
		ExecImage:  w.execImage, 
	}
	if runErr != nil {
		meta.Error = runErr.Error()
	}

	metaPath := filepath.Join(workDir, "metadata.json")
	_ = writeJSON(metaPath, meta)

	// 7) pack result.zip (output/ + logs + metadata)
	
	if err := zipResult(resultZipPath, workDir, []string{
		"output",
		"stdout.log",
		"stderr.log",
		"metadata.json",
	}); err != nil {
		return "", err
	}

	// 8) if run failed, still return zip + error (让上层走 retry/DLQ)
	if runErr != nil || exitCode != 0 || timeout {
		return resultZipPath, fmt.Errorf("task exec failed: exit_code=%d timeout=%v err=%v", exitCode, timeout, runErr)
	}
	return resultZipPath, nil
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

func unzipToDir(zipPath, dstDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		// 防止zip-slip
		cleanName := filepath.Clean(f.Name)
		if strings.Contains(cleanName, "..") {
			return fmt.Errorf("invalid zip entry: %s", f.Name)
		}
		full := filepath.Join(dstDir, cleanName)

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(full, 0755); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			return err
		}

		in, err := f.Open()
		if err != nil {
			return err
		}
		out, err := os.Create(full)
		if err != nil {
			in.Close()
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			in.Close()
			return err
		}
		out.Close()
		in.Close()
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
