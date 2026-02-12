package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"log"
)

type statusPayload struct {
	Phase    string `json:"phase"`
	Progress int    `json:"progress"`
	Msg      string `json:"msg"`
	Status   string `json:"status"`
}

func (w *Worker) httpClient() *http.Client {
	return &http.Client{Timeout: w.httpTimeout}
}

func (w *Worker) reportStatus(taskID string, p statusPayload) error {
	url := fmt.Sprintf("%s/tasks/%s/status", w.apiBaseURL, taskID)
	b, _ := json.Marshal(p)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status api failed: code=%d body=%s", resp.StatusCode, string(body))
	}
	return nil
}

func (w *Worker) uploadResult(taskID, zipPath string, attempt int) error {
	url := fmt.Sprintf("%s/tasks/%s/result", w.apiBaseURL, taskID)

	f, err := os.Open(zipPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	if err := writer.WriteField("attempt", strconv.Itoa(attempt)); err != nil {
		return fmt.Errorf("write attempt field: %w", err)
	}

	part, err := writer.CreateFormFile("result_file", filepath.Base(zipPath))
	if err != nil {
		return err
	}
	if _, err := io.Copy(part, f); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, url, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := w.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	log.Printf("[worker] uploadResult resp: task_id=%s attempt=%d code=%d",
		taskID, attempt, resp.StatusCode,
	)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload api failed: code=%d body=%s", resp.StatusCode, string(body))
	}
	return nil
}
