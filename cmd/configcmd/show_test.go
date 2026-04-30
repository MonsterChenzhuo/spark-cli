package configcmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/config"
)

func TestRenderShowsSources(t *testing.T) {
	cfg := &config.Config{
		LogDirs: []string{"file:///tmp/spark-events"},
		HDFS:    config.HDFSConfig{User: "alice"},
	}
	cfg.Timeout = 30_000_000_000 // 30s in ns
	var buf bytes.Buffer
	render(&buf, cfg, sources{
		LogDirs:  "file",
		HDFSUser: "default",
		Timeout:  "default",
	})
	out := buf.String()
	if !strings.Contains(out, "log_dirs") || !strings.Contains(out, "file:///tmp/spark-events") {
		t.Errorf("missing log_dirs in %q", out)
	}
	if !strings.Contains(out, "(file)") {
		t.Errorf("missing source label in %q", out)
	}
}

func TestDetectSourcesFromConfigDir(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	t.Setenv("SPARK_CLI_LOG_DIRS", "")
	t.Setenv("SPARK_CLI_HDFS_USER", "")
	t.Setenv("SPARK_CLI_TIMEOUT", "")
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("log_dirs:\n  - file:///x\nhdfs:\n  user: u\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{LogDirs: []string{"file:///x"}, HDFS: config.HDFSConfig{User: "u"}}
	src := detectSources(cfg)
	if src.LogDirs != "file" || src.HDFSUser != "file" || src.Timeout != "file" {
		t.Errorf("expected file labels, got %+v", src)
	}
}

func TestDetectSourcesEnvOverridesFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	t.Setenv("SPARK_CLI_LOG_DIRS", "file:///e")
	t.Setenv("SPARK_CLI_HDFS_USER", "")
	t.Setenv("SPARK_CLI_TIMEOUT", "")
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("log_dirs:\n  - file:///x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{LogDirs: []string{"file:///x"}}
	src := detectSources(cfg)
	if src.LogDirs != "env" {
		t.Errorf("expected env, got %q", src.LogDirs)
	}
}
