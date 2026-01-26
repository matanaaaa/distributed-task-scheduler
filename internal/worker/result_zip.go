package worker

import (
	"archive/zip"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (w *Worker) buildFakeResultZip(taskID string) (string, error) {
	resultsDir := filepath.Join(w.dataDir, "results")
	if err := os.MkdirAll(resultsDir, 0755); err != nil {
		return "", err
	}

	zipName := fmt.Sprintf("%s_result.zip", taskID)
	zipPath := filepath.Join(resultsDir, zipName)

	zf, err := os.Create(zipPath)
	if err != nil {
		return "", err
	}
	defer zf.Close()

	zw := zip.NewWriter(zf)
	defer zw.Close()

	// zip 里塞一个 result.txt
	wr, err := zw.Create("result.txt")
	if err != nil {
		return "", err
	}

	content := fmt.Sprintf(
		"result ok\ntask_id=%s\nts=%s\n",
		taskID,
		time.Now().UTC().Format(time.RFC3339),
	)
	if _, err := wr.Write([]byte(content)); err != nil {
		return "", err
	}

	return zipPath, nil
}

func removeIfExists(p string) error {
	if p == "" {
		return nil
	}
	_ = os.Remove(p)
	return nil
}
