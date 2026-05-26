package configcmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/config"
)

func TestClusterAddWritesConfig(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)

	cmd := New()
	cmd.SetArgs([]string{
		"cluster", "add", "prod",
		"--log-dirs", "shs://shs-prod:18081",
		"--yarn-base-urls", "http://gw/prod/yarn",
		"--shs-timeout", "7m",
		"--activate",
	})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stdout)

	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("cluster add: %v\n%s", err, stdout.String())
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ActiveCluster != "prod" || cfg.SelectedCluster != "prod" {
		t.Fatalf("cluster not active: active=%q selected=%q", cfg.ActiveCluster, cfg.SelectedCluster)
	}
	if len(cfg.LogDirs) != 1 || cfg.LogDirs[0] != "shs://shs-prod:18081" {
		t.Fatalf("LogDirs=%v", cfg.LogDirs)
	}
	if len(cfg.YARN.BaseURLs) != 1 || cfg.YARN.BaseURLs[0] != "http://gw/prod/yarn" {
		t.Fatalf("YARN.BaseURLs=%v", cfg.YARN.BaseURLs)
	}
	body, err := os.ReadFile(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "clusters:") || !strings.Contains(string(body), "active_cluster: prod") {
		t.Fatalf("config yaml missing cluster fields:\n%s", string(body))
	}
}

func TestClusterListJSON(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	body := `
active_cluster: prod
clusters:
  prod:
    log_dirs:
      - shs://shs-prod:18081
    yarn:
      base_urls:
        - http://gw/prod/yarn
`
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := New()
	cmd.SetArgs([]string{"cluster", "list", "--format", "json"})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stdout)

	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("cluster list: %v\n%s", err, stdout.String())
	}
	var got struct {
		ActiveCluster string `json:"active_cluster"`
		Clusters      []struct {
			Name         string   `json:"name"`
			Active       bool     `json:"active"`
			LogDirs      []string `json:"log_dirs"`
			YARNBaseURLs []string `json:"yarn_base_urls"`
			SHSTimeout   string   `json:"shs_timeout"`
		} `json:"clusters"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode json: %v\n%s", err, stdout.String())
	}
	if got.ActiveCluster != "prod" || len(got.Clusters) != 1 {
		t.Fatalf("unexpected clusters: %+v", got)
	}
	if got.Clusters[0].Name != "prod" || !got.Clusters[0].Active {
		t.Fatalf("unexpected cluster row: %+v", got.Clusters[0])
	}
	if got.Clusters[0].YARNBaseURLs[0] != "http://gw/prod/yarn" {
		t.Fatalf("YARNBaseURLs=%v", got.Clusters[0].YARNBaseURLs)
	}
}
