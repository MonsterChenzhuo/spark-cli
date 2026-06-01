package cmd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGuidedDiagnoseFlagWiresThroughRootCommand(t *testing.T) {
	configDir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", configDir)
	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "application_1_14")
	if err := os.WriteFile(logPath, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := "clusters:\n  prod:\n    log_dirs:\n      - file://" + logDir + "\n"
	if err := os.WriteFile(filepath.Join(configDir, "config.yaml"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{
		"diagnose", "application_1_14", "--guided", "--dry-run",
	}, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	if !strings.Contains(stdout.String(), logPath) {
		t.Fatalf("stdout did not resolve through guided flag:\n%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), `selected only configured cluster "prod"`) {
		t.Fatalf("stderr missing guided selection note:\n%s", stderr.String())
	}
}
