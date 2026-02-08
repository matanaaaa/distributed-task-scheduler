package worker

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"runtime"
)

// WarmUpDockerImage pre-pulls docker image to avoid polluting per-task stderr logs.
// If docker not installed/running, just return error.
func WarmUpDockerImage(ctx context.Context, image string) error {
	if image == "" {
		return fmt.Errorf("empty image")
	}

	// check: docker exists
	if _, err := exec.LookPath("docker"); err != nil {
		return fmt.Errorf("docker not found in PATH: %w", err)
	}

	// Use `docker pull <image>`
	cmd := exec.CommandContext(ctx, "docker", "pull", image)

	// Output goes to worker process console
	out, err := cmd.CombinedOutput()
	log.Printf("[worker] docker pull image=%s os=%s\n%s", image, runtime.GOOS, string(out))

	if err != nil {
		return fmt.Errorf("docker pull failed: %w", err)
	}
	return nil
}
