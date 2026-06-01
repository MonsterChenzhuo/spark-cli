package configcmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/opay-bigdata/spark-cli/internal/config"
	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
)

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

func TestDetectSourcesTLSInsecureEnv(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	t.Setenv("SPARK_CLI_TLS_INSECURE_SKIP_VERIFY", "true")
	cfg := &config.Config{}
	src := detectSources(cfg)
	if src.TLSInsecureSkipVerify != "env" {
		t.Errorf("expected env label for tls.insecure_skip_verify, got %q", src.TLSInsecureSkipVerify)
	}
}

// `config show --format json` 应当输出结构化 effective config,每字段含
// source 与 value,让 agent / 程序消费方一次拿到完整状态。round-9 加。
func TestRenderJSONEmitsAllFieldsWithSources(t *testing.T) {
	cfg := &config.Config{
		LogDirs:         []string{"shs://h:18081"},
		HDFS:            config.HDFSConfig{User: "alice"},
		Cache:           config.CacheConfig{Dir: "/cache"},
		SHS:             config.SHSConfig{Timeout: 5 * time.Minute},
		TLS:             config.TLSConfig{InsecureSkipVerify: true},
		SQL:             config.SQLConfig{Detail: "full"},
		Timeout:         30 * time.Second,
		ActiveCluster:   "prod",
		SelectedCluster: "prod",
		Clusters: map[string]config.ClusterConfig{
			"prod": {
				LogDirs: []string{"shs://h:18081"},
				YARN:    config.YARNConfig{BaseURLs: []string{"http://gw/yarn"}},
				TLS:     config.TLSConfig{InsecureSkipVerify: true},
			},
		},
	}
	src := sources{
		LogDirs: "file", HDFSUser: "env", HadoopConfDir: "default",
		CacheDir: "flag", SHSTimeout: "file", TLSInsecureSkipVerify: "file", SQLDetail: "env", Timeout: "default",
	}
	var buf bytes.Buffer
	if err := renderJSON(&buf, cfg, src); err != nil {
		t.Fatalf("renderJSON: %v", err)
	}
	var got map[string]map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("not json: %v\n%s", err, buf.String())
	}
	for _, k := range []string{"log_dirs", "hdfs.user", "hdfs.conf_dir", "cache.dir", "shs.timeout", "tls.insecure_skip_verify", "sql.detail", "timeout"} {
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
	if got["tls.insecure_skip_verify"]["value"] != true {
		t.Errorf("tls.insecure_skip_verify value=%v want true", got["tls.insecure_skip_verify"]["value"])
	}
	if got["active_cluster"]["value"] != "prod" {
		t.Errorf("active_cluster value=%v want prod", got["active_cluster"]["value"])
	}
	if got["selected_cluster"]["value"] != "prod" {
		t.Errorf("selected_cluster value=%v want prod", got["selected_cluster"]["value"])
	}
	if _, ok := got["clusters"]; !ok {
		t.Fatalf("missing clusters field")
	}
}

func TestShowDefaultsToJSON(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("log_dirs:\n  - file:///x\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := New()
	cmd.SetArgs([]string{"show"})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stdout)
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("config show: %v\n%s", err, stdout.String())
	}
	var got map[string]map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("default output should be json: %v\n%s", err, stdout.String())
	}
	if got["log_dirs"]["value"] == nil {
		t.Fatalf("log_dirs missing value: %+v", got["log_dirs"])
	}
}

func TestShowRejectsTextFormat(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)

	cmd := New()
	cmd.SetArgs([]string{"show", "--format", "text"})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stdout)
	err := cmd.ExecuteContext(context.Background())
	if err == nil {
		t.Fatalf("config show --format text should fail, stdout=%s", stdout.String())
	}
	if !strings.Contains(err.Error(), `unknown --format "text"`) {
		t.Fatalf("unexpected error: %v", err)
	}
	var ce *cerrors.Error
	if !errors.As(err, &ce) || ce.Code != cerrors.CodeFlagInvalid {
		t.Fatalf("error should be FLAG_INVALID, got %#v", err)
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
	root.PersistentFlags().Bool("tls-insecure-skip-verify", false, "")
	root.PersistentFlags().String("cluster", "", "")
	if err := root.PersistentFlags().Set("cache-dir", "/tmp/x"); err != nil {
		t.Fatal(err)
	}
	if err := root.PersistentFlags().Set("sql-detail", "full"); err != nil {
		t.Fatal(err)
	}
	if err := root.PersistentFlags().Set("shs-timeout", "10m"); err != nil {
		t.Fatal(err)
	}
	if err := root.PersistentFlags().Set("tls-insecure-skip-verify", "true"); err != nil {
		t.Fatal(err)
	}
	if err := root.PersistentFlags().Set("cluster", "prod"); err != nil {
		t.Fatal(err)
	}

	child := &cobra.Command{Use: "show"}
	root.AddCommand(child)

	cfg := &config.Config{
		Cache: config.CacheConfig{Dir: "/from/yaml"},
		Clusters: map[string]config.ClusterConfig{
			"prod": {
				LogDirs: []string{"shs://prod:18081"},
				YARN:    config.YARNConfig{BaseURLs: []string{"http://prod/yarn"}},
				TLS:     config.TLSConfig{InsecureSkipVerify: true},
			},
		},
	}
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
	if !cfg.TLS.InsecureSkipVerify {
		t.Errorf("cfg.TLS.InsecureSkipVerify=%v want true", cfg.TLS.InsecureSkipVerify)
	}
	if src.TLSInsecureSkipVerify != "flag" {
		t.Errorf("src.TLSInsecureSkipVerify=%q want flag", src.TLSInsecureSkipVerify)
	}
	if cfg.SelectedCluster != "prod" {
		t.Errorf("cfg.SelectedCluster=%q want prod", cfg.SelectedCluster)
	}
	if cfg.LogDirs[0] != "shs://prod:18081" || cfg.YARN.BaseURLs[0] != "http://prod/yarn" {
		t.Errorf("cluster did not override sources: log_dirs=%v yarn=%v", cfg.LogDirs, cfg.YARN.BaseURLs)
	}
	if src.Cluster != "flag" || src.LogDirs != "flag" || src.YARNBaseURLs != "flag" {
		t.Errorf("cluster sources not marked flag: %+v", src)
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
