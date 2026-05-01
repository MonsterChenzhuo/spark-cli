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
		HDFS:    config.HDFSConfig{User: "alice", ConfDir: "/etc/hadoop/conf"},
	}
	cfg.Timeout = 30_000_000_000 // 30s in ns
	var buf bytes.Buffer
	render(&buf, cfg, sources{
		LogDirs:       "file",
		HDFSUser:      "default",
		HadoopConfDir: "file",
		Timeout:       "default",
	})
	out := buf.String()
	if !strings.Contains(out, "log_dirs") || !strings.Contains(out, "file:///tmp/spark-events") {
		t.Errorf("missing log_dirs in %q", out)
	}
	if !strings.Contains(out, "(file)") {
		t.Errorf("missing source label in %q", out)
	}
	if !strings.Contains(out, "/etc/hadoop/conf") || !strings.Contains(out, "hdfs.conf_dir") {
		t.Errorf("missing hdfs.conf_dir in %q", out)
	}
}

func TestDetectSourcesFromConfigDir(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	t.Setenv("SPARK_CLI_LOG_DIRS", "")
	t.Setenv("SPARK_CLI_HDFS_USER", "")
	t.Setenv("SPARK_CLI_HADOOP_CONF_DIR", "")
	t.Setenv("SPARK_CLI_TIMEOUT", "")
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("log_dirs:\n  - file:///x\nhdfs:\n  user: u\n  conf_dir: /etc/hadoop/conf\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{LogDirs: []string{"file:///x"}, HDFS: config.HDFSConfig{User: "u", ConfDir: "/etc/hadoop/conf"}}
	src := detectSources(cfg)
	if src.LogDirs != "file" || src.HDFSUser != "file" || src.HadoopConfDir != "file" || src.Timeout != "file" {
		t.Errorf("expected file labels, got %+v", src)
	}
}

func TestDetectSourcesHadoopConfDirEnv(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	t.Setenv("SPARK_CLI_LOG_DIRS", "")
	t.Setenv("SPARK_CLI_HDFS_USER", "")
	t.Setenv("SPARK_CLI_HADOOP_CONF_DIR", "/opt/hadoop/conf")
	t.Setenv("SPARK_CLI_TIMEOUT", "")
	cfg := &config.Config{}
	src := detectSources(cfg)
	if src.HadoopConfDir != "env" {
		t.Errorf("expected env label, got %q", src.HadoopConfDir)
	}
}

func TestRenderIncludesCacheDir(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", "/tmp/xdg")
	cfg := &config.Config{}
	var buf bytes.Buffer
	render(&buf, cfg, sources{
		LogDirs:       "default",
		HDFSUser:      "default",
		HadoopConfDir: "default",
		CacheDir:      "default",
		Timeout:       "default",
	})
	if !strings.Contains(buf.String(), "cache.dir (default): /tmp/xdg/spark-cli") {
		t.Errorf("output missing cache.dir line:\n%s", buf.String())
	}
}

func TestDetectSourcesCacheDirEnv(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	t.Setenv("SPARK_CLI_CACHE_DIR", "/tmp/cd")
	cfg := &config.Config{Cache: config.CacheConfig{Dir: "/tmp/cd"}}
	src := detectSources(cfg)
	if src.CacheDir != "env" {
		t.Errorf("expected env label for cache.dir, got %q", src.CacheDir)
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
