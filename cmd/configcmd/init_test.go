package configcmd

import (
	"bytes"
	"context"
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
		"# cache:",
		"# sql:",
		"detail: truncate",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("yaml missing %q\n%s", want, body)
		}
	}
}
