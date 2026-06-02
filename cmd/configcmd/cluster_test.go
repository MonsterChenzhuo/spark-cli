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

	"github.com/opay-bigdata/spark-cli/internal/config"
	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
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
		"--tls-insecure-skip-verify",
		"--activate",
	})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stdout)

	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("cluster add: %v\n%s", err, stdout.String())
	}
	var resp struct {
		Command   string `json:"command"`
		Cluster   string `json:"cluster"`
		Path      string `json:"path"`
		Activated bool   `json:"activated"`
		Written   bool   `json:"written"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &resp); err != nil {
		t.Fatalf("cluster add stdout should be json: %v\n%s", err, stdout.String())
	}
	if resp.Command != "config cluster add" || resp.Cluster != "prod" || !resp.Activated || !resp.Written {
		t.Fatalf("unexpected response: %+v", resp)
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
	if !cfg.TLS.InsecureSkipVerify {
		t.Fatalf("TLS.InsecureSkipVerify=%v want true", cfg.TLS.InsecureSkipVerify)
	}
	body, err := os.ReadFile(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "clusters:") ||
		!strings.Contains(string(body), "active_cluster: prod") ||
		!strings.Contains(string(body), "insecure_skip_verify: true") {
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
    tls:
      insecure_skip_verify: true
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
			TLSInsecure  bool     `json:"tls_insecure_skip_verify"`
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
	if !got.Clusters[0].TLSInsecure {
		t.Fatalf("TLSInsecure=%v want true", got.Clusters[0].TLSInsecure)
	}
}

func TestClusterListDefaultsToJSON(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	body := `
active_cluster: prod
clusters:
  prod:
    log_dirs:
      - shs://shs-prod:18081
`
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := New()
	cmd.SetArgs([]string{"cluster", "list"})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stdout)
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("cluster list: %v\n%s", err, stdout.String())
	}
	var got struct {
		ActiveCluster string `json:"active_cluster"`
		Clusters      []struct {
			Name string `json:"name"`
		} `json:"clusters"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("default output should be json: %v\n%s", err, stdout.String())
	}
	if got.ActiveCluster != "prod" || got.Clusters[0].Name != "prod" {
		t.Fatalf("unexpected output: %+v", got)
	}
}

func TestClusterListRejectsTextFormat(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)

	cmd := New()
	cmd.SetArgs([]string{"cluster", "list", "--format", "text"})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stdout)
	err := cmd.ExecuteContext(context.Background())
	if err == nil {
		t.Fatalf("cluster list --format text should fail, stdout=%s", stdout.String())
	}
	if !strings.Contains(err.Error(), `unknown --format "text"`) {
		t.Fatalf("unexpected error: %v", err)
	}
	var ce *cerrors.Error
	if !errors.As(err, &ce) || ce.Code != cerrors.CodeFlagInvalid {
		t.Fatalf("error should be FLAG_INVALID, got %#v", err)
	}
}
