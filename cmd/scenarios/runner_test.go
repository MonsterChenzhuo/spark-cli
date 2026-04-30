package scenarios

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunnerDryRunEmitsLogSourceWithoutParsing(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "application_1_1")
	if err := os.WriteFile(logPath, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	rc := Run(context.Background(), Options{
		Scenario: "app-summary",
		AppID:    "application_1_1",
		LogDirs:  []string{"file://" + dir},
		Format:   "json",
		DryRun:   true,
		Stdout:   &stdout,
		Stderr:   &stderr,
	})
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"log_path"`) {
		t.Errorf("stdout missing log_path:\n%s", stdout.String())
	}
}
