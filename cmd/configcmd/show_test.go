package configcmd

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

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

func TestRenderIncludesSHSTimeout(t *testing.T) {
	cfg := &config.Config{}
	cfg.SHS.Timeout = 60_000_000_000 // 60s
	var buf bytes.Buffer
	render(&buf, cfg, sources{
		LogDirs:       "default",
		HDFSUser:      "default",
		HadoopConfDir: "default",
		CacheDir:      "default",
		SHSTimeout:    "default",
		Timeout:       "default",
	})
	if !strings.Contains(buf.String(), "shs.timeout (default): 1m0s") {
		t.Errorf("output missing shs.timeout line:\n%s", buf.String())
	}
}

func TestDetectSourcesSHSTimeoutEnv(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	t.Setenv("SPARK_CLI_SHS_TIMEOUT", "120s")
	cfg := &config.Config{}
	src := detectSources(cfg)
	if src.SHSTimeout != "env" {
		t.Errorf("expected env label for shs.timeout, got %q", src.SHSTimeout)
	}
}

// config show 必须打印 sql.detail 字段(round-2 加的 --sql-detail 配置)。
// 默认值 truncate;cfg.SQL.Detail 为空时 render 显示 "truncate"(默认)。
func TestRenderShowsSQLDetailDefault(t *testing.T) {
	cfg := &config.Config{}
	var buf bytes.Buffer
	render(&buf, cfg, sources{
		LogDirs: "default", HDFSUser: "default", HadoopConfDir: "default",
		CacheDir: "default", SHSTimeout: "default", SQLDetail: "default", Timeout: "default",
	})
	if !strings.Contains(buf.String(), "sql.detail (default): truncate") {
		t.Errorf("output missing sql.detail default line:\n%s", buf.String())
	}
}

func TestDetectSourcesSQLDetailEnv(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	t.Setenv("SPARK_CLI_SQL_DETAIL", "full")
	cfg := &config.Config{}
	src := detectSources(cfg)
	if src.SQLDetail != "env" {
		t.Errorf("expected env label for sql.detail, got %q", src.SQLDetail)
	}
}

// `config show --format json` 应当输出结构化 effective config,每字段含
// source 与 value,让 agent / 程序消费方一次拿到完整状态。round-9 加。
func TestRenderJSONEmitsAllFieldsWithSources(t *testing.T) {
	cfg := &config.Config{
		LogDirs: []string{"shs://h:18081"},
		HDFS:    config.HDFSConfig{User: "alice"},
		Cache:   config.CacheConfig{Dir: "/cache"},
		SHS:     config.SHSConfig{Timeout: 5 * time.Minute},
		SQL:     config.SQLConfig{Detail: "full"},
		Timeout: 30 * time.Second,
	}
	src := sources{
		LogDirs: "file", HDFSUser: "env", HadoopConfDir: "default",
		CacheDir: "flag", SHSTimeout: "file", SQLDetail: "env", Timeout: "default",
	}
	var buf bytes.Buffer
	if err := renderJSON(&buf, cfg, src); err != nil {
		t.Fatalf("renderJSON: %v", err)
	}
	var got map[string]map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("not json: %v\n%s", err, buf.String())
	}
	for _, k := range []string{"log_dirs", "hdfs.user", "hdfs.conf_dir", "cache.dir", "shs.timeout", "sql.detail", "timeout"} {
		f, ok := got[k]
		if !ok {
			t.Errorf("missing field %q", k)
			continue
		}
		if _, ok := f["source"]; !ok {
			t.Errorf("field %q missing source", k)
		}
		if _, ok := f["value"]; !ok {
			t.Errorf("field %q missing value", k)
		}
	}
	if got["log_dirs"]["source"] != "file" {
		t.Errorf("log_dirs source=%v want file", got["log_dirs"]["source"])
	}
	if got["sql.detail"]["value"] != "full" {
		t.Errorf("sql.detail value=%v want full", got["sql.detail"]["value"])
	}
}

// `config show --cache-dir /tmp/x` 应当让 cache.dir 字段 source 标 "flag",
// value 反映 flag 值。round-13 修 — 之前 detectSources 只看 yaml/env,
// 完全忽略 root persistent flag。
func TestApplyRootFlagOverrides(t *testing.T) {
	root := &cobra.Command{Use: "spark-cli"}
	root.PersistentFlags().String("cache-dir", "", "")
	root.PersistentFlags().String("sql-detail", "", "")
	root.PersistentFlags().String("shs-timeout", "", "")
	if err := root.PersistentFlags().Set("cache-dir", "/tmp/x"); err != nil {
		t.Fatal(err)
	}
	if err := root.PersistentFlags().Set("sql-detail", "full"); err != nil {
		t.Fatal(err)
	}
	if err := root.PersistentFlags().Set("shs-timeout", "10m"); err != nil {
		t.Fatal(err)
	}

	child := &cobra.Command{Use: "show"}
	root.AddCommand(child)

	cfg := &config.Config{Cache: config.CacheConfig{Dir: "/from/yaml"}}
	src := sources{CacheDir: "file", SQLDetail: "default", SHSTimeout: "default"}
	applyRootFlagOverrides(child, cfg, &src)

	if cfg.Cache.Dir != "/tmp/x" {
		t.Errorf("cfg.Cache.Dir=%q want /tmp/x", cfg.Cache.Dir)
	}
	if src.CacheDir != "flag" {
		t.Errorf("src.CacheDir=%q want flag", src.CacheDir)
	}
	if cfg.SQL.Detail != "full" {
		t.Errorf("cfg.SQL.Detail=%q want full", cfg.SQL.Detail)
	}
	if src.SQLDetail != "flag" {
		t.Errorf("src.SQLDetail=%q want flag", src.SQLDetail)
	}
	if cfg.SHS.Timeout != 10*time.Minute {
		t.Errorf("cfg.SHS.Timeout=%v want 10m", cfg.SHS.Timeout)
	}
	if src.SHSTimeout != "flag" {
		t.Errorf("src.SHSTimeout=%q want flag", src.SHSTimeout)
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
