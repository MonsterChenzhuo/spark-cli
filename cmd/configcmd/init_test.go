package configcmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// `spark-cli config init` 写出的 yaml 应当包含可选段(shs / cache / sql)的注释,
// 让用户初始化后立即知道有哪些 flag 可调。round-6 加这条 — 之前 yaml 只写
// log_dirs / hdfs / timeout 三段,用户根本不知道 sql.detail 等字段存在。
func TestInitWritesYAMLWithOptionalSectionComments(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)

	cmd := New()
	cmd.SetArgs([]string{"init"})
	// 模拟全部 default 输入:6 个 newline
	cmd.SetIn(strings.NewReader("\n\n\n\n\n\n"))
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stdout)

	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}

	yaml, err := os.ReadFile(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatalf("read yaml: %v", err)
	}
	body := string(yaml)
	for _, want := range []string{
		"log_dirs:",
		"# shs:",
		"# tls:",
		"# cache:",
		"# sql:",
		"insecure_skip_verify: false",
		"detail: truncate",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("yaml missing %q\n%s", want, body)
		}
	}
}

func TestInitIsNonInteractiveAndEmitsJSON(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)

	cmd := New()
	cmd.SetArgs([]string{
		"init",
		"--local-log-dir", "/logs",
		"--hdfs-namenode", "nn:8020",
		"--hdfs-history-dir", "/spark-history",
		"--hdfs-user", "alice",
		"--hadoop-conf-dir", "/etc/hadoop/conf",
		"--timeout", "45s",
	})
	cmd.SetIn(strings.NewReader(""))
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stdout)

	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("init: %v\n%s", err, stdout.String())
	}
	var got struct {
		Command string `json:"command"`
		Path    string `json:"path"`
		Written bool   `json:"written"`
		Config  struct {
			LogDirs []string `json:"log_dirs"`
			HDFS    struct {
				User    string `json:"user"`
				ConfDir string `json:"conf_dir"`
			} `json:"hdfs"`
			Timeout string `json:"timeout"`
		} `json:"config"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout should be json: %v\n%s", err, stdout.String())
	}
	if got.Command != "config init" || !got.Written {
		t.Fatalf("unexpected response: %+v", got)
	}
	if got.Config.LogDirs[0] != "file:///logs" || got.Config.LogDirs[1] != "hdfs://nn:8020/spark-history" {
		t.Fatalf("log_dirs=%v", got.Config.LogDirs)
	}
	if got.Config.HDFS.User != "alice" || got.Config.HDFS.ConfDir != "/etc/hadoop/conf" || got.Config.Timeout != "45s" {
		t.Fatalf("config fields wrong: %+v", got.Config)
	}
}
