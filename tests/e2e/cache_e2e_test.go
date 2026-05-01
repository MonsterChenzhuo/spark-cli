package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opay-bigdata/spark-cli/cmd"
)

func runCLI(t *testing.T, args []string) (map[string]any, string) {
	t.Helper()
	cmd.ResetForTest()
	var stdout, stderr bytes.Buffer
	rc := cmd.RunWith(context.Background(), args, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	var m map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &m); err != nil {
		t.Fatalf("not json: %v\n%s", err, stdout.String())
	}
	return m, stderr.String()
}

func writeTinyLog(t *testing.T, dir string) string {
	t.Helper()
	src, err := os.ReadFile(filepath.Join("..", "testdata", "tiny_app.json"))
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "application_1_1")
	if err := os.WriteFile(p, src, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestE2ECacheHitSkipsParse(t *testing.T) {
	logsDir := t.TempDir()
	cacheDir := t.TempDir()
	writeTinyLog(t, logsDir)

	args := []string{"app-summary", "application_1_1",
		"--log-dirs", "file://" + logsDir,
		"--cache-dir", cacheDir,
		"--format", "json",
	}

	first, _ := runCLI(t, args)
	if pe, ok := first["parsed_events"].(float64); !ok || pe == 0 {
		t.Fatalf("first run should parse events, got parsed_events=%v", first["parsed_events"])
	}

	entries, err := os.ReadDir(cacheDir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("cache dir empty after first run: err=%v entries=%v", err, entries)
	}

	second, _ := runCLI(t, args)
	if pe, ok := second["parsed_events"].(float64); !ok || pe != 0 {
		t.Errorf("cache hit should set parsed_events=0, got %v", second["parsed_events"])
	}
}

func TestE2ECacheInvalidatesOnMtimeChange(t *testing.T) {
	logsDir := t.TempDir()
	cacheDir := t.TempDir()
	logPath := writeTinyLog(t, logsDir)

	args := []string{"app-summary", "application_1_1",
		"--log-dirs", "file://" + logsDir,
		"--cache-dir", cacheDir,
		"--format", "json",
	}
	_, _ = runCLI(t, args)

	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(logPath, future, future); err != nil {
		t.Fatal(err)
	}

	second, _ := runCLI(t, args)
	if pe, ok := second["parsed_events"].(float64); !ok || pe == 0 {
		t.Errorf("mtime change should force reparse, got parsed_events=%v", second["parsed_events"])
	}
}

func TestE2ENoCacheFlagSkipsWrite(t *testing.T) {
	logsDir := t.TempDir()
	cacheDir := t.TempDir()
	writeTinyLog(t, logsDir)

	args := []string{"app-summary", "application_1_1",
		"--log-dirs", "file://" + logsDir,
		"--cache-dir", cacheDir,
		"--no-cache",
		"--format", "json",
	}
	_, _ = runCLI(t, args)
	entries, _ := os.ReadDir(cacheDir)
	if len(entries) != 0 {
		t.Errorf("--no-cache should not write any file, got %d entries", len(entries))
	}
}
