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

// `spark-cli cache list --format json --cache-dir <tmp>` 应当输出结构化清单,
// 经过 cache 写入后能看到 application gob.zst entry。round-12 加守门,
// 确保 cache subcommand 与 root --cache-dir flag 串起来。
func TestE2ECacheListReadsCustomCacheDir(t *testing.T) {
	logsDir := t.TempDir()
	cacheDir := t.TempDir()
	writeTinyLog(t, logsDir)

	// 先跑一次 app-summary 写入缓存
	args := []string{"app-summary", "application_1_1",
		"--log-dirs", "file://" + logsDir,
		"--cache-dir", cacheDir,
		"--format", "json",
	}
	if _, _ = runCLI(t, args); t.Failed() {
		return
	}

	// cache list --format json 拿到非空 entries
	cmd.ResetForTest()
	var stdout, stderr bytes.Buffer
	rc := cmd.RunWith(context.Background(),
		[]string{"cache", "list", "--cache-dir", cacheDir, "--format", "json"},
		&stdout, &stderr)
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	var listOut map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &listOut); err != nil {
		t.Fatalf("not json: %v\n%s", err, stdout.String())
	}
	if total, _ := listOut["total"].(float64); total < 1 {
		t.Errorf("expected >= 1 cache entry, got %v\n%s", total, stdout.String())
	}
	if dir, _ := listOut["dir"].(string); dir != cacheDir {
		t.Errorf("dir=%q want %q", dir, cacheDir)
	}
}

// `cache clear --dry-run --cache-dir <tmp>` 应当报告"would remove"但不真删。
func TestE2ECacheClearDryRunDoesNotDelete(t *testing.T) {
	logsDir := t.TempDir()
	cacheDir := t.TempDir()
	writeTinyLog(t, logsDir)

	args := []string{"app-summary", "application_1_1",
		"--log-dirs", "file://" + logsDir,
		"--cache-dir", cacheDir,
		"--format", "json",
	}
	_, _ = runCLI(t, args)

	beforeEntries, _ := os.ReadDir(cacheDir)
	if len(beforeEntries) == 0 {
		t.Fatal("cache should have entries after first run")
	}

	cmd.ResetForTest()
	var stdout, stderr bytes.Buffer
	rc := cmd.RunWith(context.Background(),
		[]string{"cache", "clear", "--cache-dir", cacheDir, "--dry-run"},
		&stdout, &stderr)
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	if !bytes.Contains(stdout.Bytes(), []byte("would remove")) {
		t.Errorf("expected 'would remove' marker:\n%s", stdout.String())
	}
	afterEntries, _ := os.ReadDir(cacheDir)
	if len(afterEntries) != len(beforeEntries) {
		t.Errorf("dry-run should not delete: before=%d after=%d", len(beforeEntries), len(afterEntries))
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
