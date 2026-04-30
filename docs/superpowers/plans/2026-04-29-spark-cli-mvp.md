# spark-cli MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现一个 Go 单二进制 `spark-cli`，按 applicationId 从本地或 HDFS 定位 Spark EventLog，流式解析后输出 5 个诊断场景的结构化 JSON，专为 Claude Code 等 AI agent 设计。

**Architecture:** cobra 命令树 → 通用 runner（加载 config → 定位 LogSource → 流式解析 → 调场景纯函数 → 渲染）。算法层（`internal/scenario/`）与 cobra 解耦；文件系统抽象（`internal/fs.FS`）让本地和 HDFS 在场景层完全等价。

**Tech Stack:** Go 1.22 · cobra · colinmarc/hdfs · klauspost/compress/zstd · pierrec/lz4/v4 · golang/snappy · influxdata/tdigest · goreleaser · golangci-lint v2.1.6

**Spec:** `docs/superpowers/specs/2026-04-29-spark-cli-design.md`

---

## File Structure

| 文件 | 职责 |
|---|---|
| `main.go` | 入口，调用 `cmd.Execute()` |
| `cmd/root.go` | cobra root + 全局 flag + Execute |
| `cmd/version.go` | `spark-cli version` |
| `cmd/configcmd/init.go` | `config init` 交互式生成 yaml |
| `cmd/configcmd/show.go` | `config show` 打印生效配置 |
| `cmd/scenarios/register.go` | 注册 5 个场景 cobra 命令 |
| `cmd/scenarios/runner.go` | 通用 pipeline：config → locate → parse → render |
| `cmd/scenarios/{app_summary,slow_stages,data_skew,gc_pressure,diagnose}.go` | 单个场景的 cobra wiring |
| `internal/config/config.go` | Config struct + Load + Validate + ApplyEnv + ApplyFlags |
| `internal/errors/errors.go` | 结构化错误 + exit code 映射 |
| `internal/fs/fs.go` | FS interface (Open / Stat / List) |
| `internal/fs/local.go` | 本地实现 |
| `internal/fs/hdfs.go` | colinmarc/hdfs 封装 |
| `internal/eventlog/locate.go` | appId → LogSource，URI 路由、归一化、V1/V2 探测 |
| `internal/eventlog/reader.go` | 解压 dispatch（none/zstd/lz4/snappy）+ V2 多 part 拼接 |
| `internal/eventlog/events.go` | 事件结构体（最小字段映射） |
| `internal/eventlog/decoder.go` | bufio 流式 JSONL 解析 + 路由到 aggregator |
| `internal/stats/tdigest.go` | t-digest 包装 + percentile helper |
| `internal/model/application.go` | Application 顶层模型 |
| `internal/model/{job,stage,task,executor}.go` | 子模型 |
| `internal/model/aggregator.go` | 边读边累加 |
| `internal/scenario/result.go` | Envelope / Row 类型 |
| `internal/scenario/{app_summary,slow_stages,data_skew,gc_pressure,diagnose}.go` | 场景纯函数 |
| `internal/rules/rule.go` | Rule interface + Finding |
| `internal/rules/{skew,gc,spill,failed_tasks,tiny_tasks}_rule.go` | 5 条规则 |
| `internal/output/{json,table,markdown}.go` | 三种渲染 |
| `tests/testdata/*.json` / `*.zstd` | 合成 EventLog 样本 |
| `tests/golden/*.json` | 期望 stdout |
| `tests/e2e/e2e_test.go` | 全链路 |
| `.claude/skills/spark/SKILL.md` | Claude Code skill |
| `scripts/install.sh` | 一行安装 |
| `Makefile` / `.goreleaser.yml` / `.golangci.yml` | 工程化 |
| `.github/workflows/{ci,release}.yml` | CI/CD |
| `README.md` / `README.zh.md` / `CHANGELOG.md` / `LICENSE` | 文档 |

---

## Conventions for all tasks

- **Module path**: `github.com/opay-bigdata/spark-cli`
- **Working dir**: `/Users/opay-20240095/IdeaProjects/createcli/spark-cli`
- **Commit style**: `feat:` / `test:` / `chore:` / `docs:` / `ci:` / `fix:`
- **Branch**: 直接在默认分支 `main` 上提交（git init 后第一个 commit 就是 init）
- **Test framework**: 标准库 `testing`；不引入 testify（保持极简依赖）
- **Run**：每个 Task 末尾的 commit 步骤之前必须保证 `go test ./...` 全绿（首个有测试的 task 起算）
- **golangci-lint** 配置见 Task 2，所有代码须 lint 通过

---

## Task 1: Init Go module + git + base files

**Files:**
- Create: `go.mod`, `main.go`, `.gitignore`, `LICENSE`

- [ ] **Step 1: Init git + go module**

```bash
cd /Users/opay-20240095/IdeaProjects/createcli/spark-cli
git init -b main
go mod init github.com/opay-bigdata/spark-cli
```

- [ ] **Step 2: Write `.gitignore`**

```gitignore
spark-cli
dist/
coverage.out
*.test
.idea/
.vscode/
```

- [ ] **Step 3: Write `LICENSE` (MIT)**

Use the standard MIT text, year 2026, holder "OPay Bigdata".

- [ ] **Step 4: Write `main.go`**

```go
// Copyright (c) 2026 OPay Bigdata.
// SPDX-License-Identifier: MIT
package main

import (
	"os"

	"github.com/opay-bigdata/spark-cli/cmd"
)

func main() {
	os.Exit(cmd.Execute())
}
```

- [ ] **Step 5: Create empty `cmd/root.go` placeholder so `go build` compiles**

```go
// Package cmd wires the cobra command tree.
package cmd

// Execute is the package entry point invoked by main.go.
func Execute() int { return 0 }
```

- [ ] **Step 6: Verify build**

Run: `go build ./...`
Expected: no error, no binary side-effect (we use `go install` later).

- [ ] **Step 7: Commit**

```bash
git add .
git commit -m "chore: init go module and skeleton"
```

---

## Task 2: Add Makefile + golangci-lint config + cobra dependency

**Files:**
- Create: `Makefile`, `.golangci.yml`

- [ ] **Step 1: Add cobra dep**

```bash
go get github.com/spf13/cobra@v1.8.1
go mod tidy
```

- [ ] **Step 2: Write `Makefile`**

```makefile
GO ?= go
BINARY := spark-cli
PKG := github.com/opay-bigdata/spark-cli
LDFLAGS := -s -w -X $(PKG)/cmd.version=$(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: build install unit-test e2e test lint fmt tidy clean release-snapshot

build:
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BINARY) .

install:
	$(GO) install -ldflags "$(LDFLAGS)" .

unit-test:
	$(GO) test -race -count=1 ./...

e2e: build
	$(GO) test -race -count=1 -tags=e2e ./tests/e2e/...

test: unit-test e2e

lint:
	$(GO) vet ./...
	@gofmt -l . | tee /dev/stderr | (! read)
	@$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.1.6 run

fmt:
	gofmt -w .

tidy:
	$(GO) mod tidy

clean:
	rm -f $(BINARY)
	rm -rf dist/

release-snapshot:
	$(GO) run github.com/goreleaser/goreleaser/v2@latest release --snapshot --clean
```

- [ ] **Step 3: Write `.golangci.yml`**

```yaml
version: "2"
run:
  timeout: 3m
  go: "1.22"
linters:
  default: none
  enable:
    - errcheck
    - govet
    - ineffassign
    - staticcheck
    - unused
    - misspell
    - gocyclo
  settings:
    gocyclo:
      min-complexity: 18
issues:
  max-issues-per-linter: 0
  max-same-issues: 0
```

- [ ] **Step 4: Verify**

Run: `make build`
Expected: `./spark-cli` binary produced, exit 0.

Run: `make lint`
Expected: exit 0 (no findings yet).

- [ ] **Step 5: Commit**

```bash
git add Makefile .golangci.yml go.mod go.sum
git commit -m "chore: add Makefile, golangci config, cobra dep"
```

---

## Task 3: Implement cobra root + version command (TDD)

**Files:**
- Modify: `cmd/root.go`
- Create: `cmd/version.go`, `cmd/version_test.go`

- [ ] **Step 1: Write failing test `cmd/version_test.go`**

```go
package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersionCommandPrintsVersion(t *testing.T) {
	root := newRootCmd()
	root.AddCommand(newVersionCmd())
	root.SetArgs([]string{"version"})
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}
	got := buf.String()
	if !strings.Contains(got, "spark-cli") {
		t.Fatalf("missing program name; got %q", got)
	}
	if !strings.Contains(got, version) {
		t.Fatalf("missing version %q in %q", version, got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/...`
Expected: FAIL — `newRootCmd` / `newVersionCmd` / `version` undefined.

- [ ] **Step 3: Implement `cmd/root.go`**

```go
// Package cmd wires the cobra command tree.
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var version = "dev"

type globalFlags struct {
	LogDirs    string
	HDFSUser   string
	Timeout    string
	Format     string
	Top        int
	DryRun     bool
	NoProgress bool
}

var globals globalFlags

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "spark-cli",
		Short:         "Diagnose Spark applications by parsing EventLog — designed for Claude Code.",
		Long:          `spark-cli locates a Spark EventLog by applicationId (local or HDFS), parses it once, and emits structured JSON for AI agents.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&globals.LogDirs, "log-dirs", "", "Comma-separated list of EventLog dirs (file:// or hdfs:// URIs)")
	root.PersistentFlags().StringVar(&globals.HDFSUser, "hdfs-user", "", "HDFS simple-auth user (defaults to $USER)")
	root.PersistentFlags().StringVar(&globals.Timeout, "timeout", "", "Overall parse timeout, e.g. 30s")
	root.PersistentFlags().StringVar(&globals.Format, "format", "json", "Output format: json | table | markdown")
	root.PersistentFlags().IntVar(&globals.Top, "top", 10, "Top N for ranked scenarios")
	root.PersistentFlags().BoolVar(&globals.DryRun, "dry-run", false, "Locate file only; do not parse events")
	root.PersistentFlags().BoolVar(&globals.NoProgress, "no-progress", false, "Disable stderr progress")
	return root
}

// Execute is the package entry point invoked by main.go.
func Execute() int {
	root := newRootCmd()
	root.AddCommand(newVersionCmd())
	if err := root.Execute(); err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		return 1
	}
	return 0
}
```

- [ ] **Step 4: Implement `cmd/version.go`**

```go
package cmd

import (
	"github.com/spf13/cobra"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print spark-cli version",
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Printf("spark-cli %s\n", version)
			return nil
		},
	}
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./cmd/...`
Expected: PASS.

- [ ] **Step 6: Smoke run**

Run: `go run . version`
Expected: prints `spark-cli dev`.

- [ ] **Step 7: Commit**

```bash
git add cmd/ go.sum
git commit -m "feat(cmd): cobra root with version subcommand"
```

---

## Task 4: Structured errors package

**Files:**
- Create: `internal/errors/errors.go`, `internal/errors/errors_test.go`

- [ ] **Step 1: Write failing test `internal/errors/errors_test.go`**

```go
package errors

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestNewIsTypedError(t *testing.T) {
	e := New(CodeAppNotFound, "no app", "check log_dirs")
	if e.Code != CodeAppNotFound || e.Message != "no app" || e.Hint != "check log_dirs" {
		t.Fatalf("unexpected: %+v", e)
	}
	if e.Error() == "" {
		t.Fatal("Error() empty")
	}
}

func TestExitCodeMapping(t *testing.T) {
	cases := []struct {
		code Code
		want int
	}{
		{CodeAppNotFound, 2},
		{CodeFlagInvalid, 2},
		{CodeConfigMissing, 2},
		{CodeHDFSUnreachable, 3},
		{CodeLogUnreadable, 3},
		{CodeLogParseFailed, 1},
		{CodeInternal, 1},
	}
	for _, c := range cases {
		if got := ExitCode(New(c.code, "", "")); got != c.want {
			t.Errorf("%s: got %d want %d", c.code, got, c.want)
		}
	}
	if ExitCode(nil) != 0 {
		t.Fatal("nil should be 0")
	}
}

func TestWriteJSONEnvelope(t *testing.T) {
	var buf bytes.Buffer
	WriteJSON(&buf, New(CodeAppNotFound, "no app", "hint"))
	var out struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
			Hint    string `json:"hint"`
		} `json:"error"`
	}
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Error.Code != "APP_NOT_FOUND" || out.Error.Message != "no app" || out.Error.Hint != "hint" {
		t.Fatalf("unexpected: %+v", out)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/errors/...`
Expected: FAIL — package not implemented.

- [ ] **Step 3: Implement `internal/errors/errors.go`**

```go
// Package errors defines structured CLI errors with exit-code mapping.
package errors

import (
	"encoding/json"
	"fmt"
	"io"
)

type Code string

const (
	CodeConfigMissing   Code = "CONFIG_MISSING"
	CodeAppNotFound     Code = "APP_NOT_FOUND"
	CodeAppAmbiguous    Code = "APP_AMBIGUOUS"
	CodeLogUnreadable   Code = "LOG_UNREADABLE"
	CodeLogParseFailed  Code = "LOG_PARSE_FAILED"
	CodeLogIncomplete   Code = "LOG_INCOMPLETE"
	CodeHDFSUnreachable Code = "HDFS_UNREACHABLE"
	CodeFlagInvalid     Code = "FLAG_INVALID"
	CodeInternal        Code = "INTERNAL"
)

const (
	ExitOK       = 0
	ExitInternal = 1
	ExitUserErr  = 2
	ExitIOErr    = 3
)

type Error struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
	Hint    string `json:"hint,omitempty"`
}

func New(code Code, msg, hint string) *Error {
	return &Error{Code: code, Message: msg, Hint: hint}
}

func (e *Error) Error() string {
	if e.Hint != "" {
		return fmt.Sprintf("%s: %s (hint: %s)", e.Code, e.Message, e.Hint)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func ExitCode(err error) int {
	if err == nil {
		return ExitOK
	}
	e, ok := err.(*Error)
	if !ok {
		return ExitInternal
	}
	switch e.Code {
	case CodeConfigMissing, CodeAppNotFound, CodeAppAmbiguous, CodeFlagInvalid:
		return ExitUserErr
	case CodeHDFSUnreachable, CodeLogUnreadable, CodeLogIncomplete:
		return ExitIOErr
	default:
		return ExitInternal
	}
}

type envelope struct {
	Error *Error `json:"error"`
}

func WriteJSON(w io.Writer, err error) {
	if err == nil {
		return
	}
	e, ok := err.(*Error)
	if !ok {
		e = &Error{Code: CodeInternal, Message: err.Error()}
	}
	b, _ := json.Marshal(envelope{Error: e})
	_, _ = w.Write(b)
	_, _ = w.Write([]byte("\n"))
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/errors/...`
Expected: PASS.

- [ ] **Step 5: Wire into root Execute (use WriteJSON + ExitCode)**

Modify `cmd/root.go` `Execute`:

```go
func Execute() int {
	root := newRootCmd()
	root.AddCommand(newVersionCmd())
	if err := root.Execute(); err != nil {
		cerrors.WriteJSON(os.Stderr, err)
		return cerrors.ExitCode(err)
	}
	return cerrors.ExitOK
}
```

Add import alias `cerrors "github.com/opay-bigdata/spark-cli/internal/errors"`.

- [ ] **Step 6: Smoke**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/errors cmd/root.go go.sum
git commit -m "feat(errors): structured error envelope with exit-code mapping"
```

---

## Task 5: Config package — Load / Validate / ApplyEnv / ApplyFlags

**Files:**
- Create: `internal/config/config.go`, `internal/config/config_test.go`

- [ ] **Step 1: Write failing test `internal/config/config_test.go`**

```go
package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaultsWhenNoFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Timeout != 30*time.Second {
		t.Errorf("default timeout = %v, want 30s", cfg.Timeout)
	}
	if len(cfg.LogDirs) != 0 {
		t.Errorf("default log_dirs not empty: %v", cfg.LogDirs)
	}
}

func TestLoadParsesYAML(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	body := `
log_dirs:
  - file:///tmp/spark-events
  - hdfs://nn:8020/spark-history
hdfs:
  user: alice
timeout: 45s
`
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.LogDirs) != 2 || cfg.LogDirs[0] != "file:///tmp/spark-events" {
		t.Errorf("log_dirs = %v", cfg.LogDirs)
	}
	if cfg.HDFS.User != "alice" {
		t.Errorf("hdfs.user = %q", cfg.HDFS.User)
	}
	if cfg.Timeout != 45*time.Second {
		t.Errorf("timeout = %v", cfg.Timeout)
	}
}

func TestApplyEnvOverrides(t *testing.T) {
	cfg := &Config{Timeout: 30 * time.Second}
	t.Setenv("SPARK_CLI_LOG_DIRS", "file:///a,file:///b")
	t.Setenv("SPARK_CLI_HDFS_USER", "bob")
	t.Setenv("SPARK_CLI_TIMEOUT", "10s")
	ApplyEnv(cfg)
	if len(cfg.LogDirs) != 2 || cfg.LogDirs[1] != "file:///b" {
		t.Errorf("log_dirs = %v", cfg.LogDirs)
	}
	if cfg.HDFS.User != "bob" {
		t.Errorf("user = %q", cfg.HDFS.User)
	}
	if cfg.Timeout != 10*time.Second {
		t.Errorf("timeout = %v", cfg.Timeout)
	}
}

func TestApplyFlagsTakesPrecedence(t *testing.T) {
	cfg := &Config{LogDirs: []string{"file:///a"}}
	ApplyFlags(cfg, FlagOverrides{LogDirs: "file:///b,file:///c", HDFSUser: "carol", Timeout: 5 * time.Second})
	if cfg.LogDirs[0] != "file:///b" || cfg.HDFS.User != "carol" || cfg.Timeout != 5*time.Second {
		t.Errorf("override failed: %+v", cfg)
	}
}

func TestValidateRequiresLogDirs(t *testing.T) {
	cfg := &Config{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("want validation error")
	}
}
```

- [ ] **Step 2: Add yaml dep**

Run: `go get gopkg.in/yaml.v3 && go mod tidy`

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/config/...`
Expected: FAIL — package not implemented.

- [ ] **Step 4: Implement `internal/config/config.go`**

```go
// Package config loads and validates spark-cli configuration.
package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type HDFSConfig struct {
	User string `yaml:"user"`
}

type Config struct {
	LogDirs []string      `yaml:"log_dirs"`
	HDFS    HDFSConfig    `yaml:"hdfs"`
	Timeout time.Duration `yaml:"timeout"`
}

const defaultTimeout = 30 * time.Second

func configDir() string {
	if d := os.Getenv("SPARK_CLI_CONFIG_DIR"); d != "" {
		return d
	}
	if h, err := os.UserHomeDir(); err == nil {
		return filepath.Join(h, ".config", "spark-cli")
	}
	return "."
}

func Load() (*Config, error) {
	cfg := &Config{Timeout: defaultTimeout}
	path := filepath.Join(configDir(), "config.yaml")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}
	raw := struct {
		LogDirs []string   `yaml:"log_dirs"`
		HDFS    HDFSConfig `yaml:"hdfs"`
		Timeout string     `yaml:"timeout"`
	}{}
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	cfg.LogDirs = raw.LogDirs
	cfg.HDFS = raw.HDFS
	if raw.Timeout != "" {
		d, err := time.ParseDuration(raw.Timeout)
		if err != nil {
			return nil, err
		}
		cfg.Timeout = d
	}
	return cfg, nil
}

func ApplyEnv(cfg *Config) {
	if v := os.Getenv("SPARK_CLI_LOG_DIRS"); v != "" {
		cfg.LogDirs = splitCSV(v)
	}
	if v := os.Getenv("SPARK_CLI_HDFS_USER"); v != "" {
		cfg.HDFS.User = v
	}
	if v := os.Getenv("SPARK_CLI_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Timeout = d
		}
	}
}

type FlagOverrides struct {
	LogDirs  string
	HDFSUser string
	Timeout  time.Duration
}

func ApplyFlags(cfg *Config, f FlagOverrides) {
	if f.LogDirs != "" {
		cfg.LogDirs = splitCSV(f.LogDirs)
	}
	if f.HDFSUser != "" {
		cfg.HDFS.User = f.HDFSUser
	}
	if f.Timeout > 0 {
		cfg.Timeout = f.Timeout
	}
}

func (c *Config) Validate() error {
	if len(c.LogDirs) == 0 {
		return errors.New("log_dirs is empty; run `spark-cli config init` or set --log-dirs")
	}
	if c.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	return nil
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/config/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/config go.mod go.sum
git commit -m "feat(config): yaml/env/flag config with validation"
```

---

## Task 6: `config init` and `config show` commands

**Files:**
- Create: `cmd/configcmd/configcmd.go`, `cmd/configcmd/init.go`, `cmd/configcmd/show.go`, `cmd/configcmd/show_test.go`

- [ ] **Step 1: Write failing test `cmd/configcmd/show_test.go`**

```go
package configcmd

import (
	"bytes"
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
		LogDirs: "file",
		HDFSUser: "default",
		Timeout: "default",
	})
	out := buf.String()
	if !strings.Contains(out, "log_dirs") || !strings.Contains(out, "file:///tmp/spark-events") {
		t.Errorf("missing log_dirs in %q", out)
	}
	if !strings.Contains(out, "(file)") {
		t.Errorf("missing source label in %q", out)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/configcmd/...`
Expected: FAIL — package not implemented.

- [ ] **Step 3: Implement `cmd/configcmd/configcmd.go`**

```go
// Package configcmd provides `spark-cli config init` and `spark-cli config show`.
package configcmd

import "github.com/spf13/cobra"

func New() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage spark-cli configuration",
	}
	cmd.AddCommand(newInitCmd())
	cmd.AddCommand(newShowCmd())
	return cmd
}
```

- [ ] **Step 4: Implement `cmd/configcmd/show.go`**

```go
package configcmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/opay-bigdata/spark-cli/internal/config"
)

type sources struct {
	LogDirs  string // "flag" | "env" | "file" | "default"
	HDFSUser string
	Timeout  string
}

func newShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show",
		Short: "Print the effective configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}
			src := detectSources(cfg)
			config.ApplyEnv(cfg)
			render(cmd.OutOrStdout(), cfg, src)
			return nil
		},
	}
}

func detectSources(cfg *config.Config) sources {
	src := sources{LogDirs: "default", HDFSUser: "default", Timeout: "default"}
	home, _ := os.UserHomeDir()
	path := filepath.Join(home, ".config", "spark-cli", "config.yaml")
	if _, err := os.Stat(path); err == nil {
		if len(cfg.LogDirs) > 0 {
			src.LogDirs = "file"
		}
		if cfg.HDFS.User != "" {
			src.HDFSUser = "file"
		}
		src.Timeout = "file"
	}
	if os.Getenv("SPARK_CLI_LOG_DIRS") != "" {
		src.LogDirs = "env"
	}
	if os.Getenv("SPARK_CLI_HDFS_USER") != "" {
		src.HDFSUser = "env"
	}
	if os.Getenv("SPARK_CLI_TIMEOUT") != "" {
		src.Timeout = "env"
	}
	return src
}

func render(w io.Writer, cfg *config.Config, src sources) {
	fmt.Fprintf(w, "log_dirs (%s):\n", src.LogDirs)
	if len(cfg.LogDirs) == 0 {
		fmt.Fprintln(w, "  (none — run `spark-cli config init`)")
	}
	for _, d := range cfg.LogDirs {
		fmt.Fprintf(w, "  - %s\n", d)
	}
	fmt.Fprintf(w, "hdfs.user (%s): %s\n", src.HDFSUser, cfg.HDFS.User)
	fmt.Fprintf(w, "timeout (%s): %s\n", src.Timeout, cfg.Timeout)
}
```

- [ ] **Step 5: Implement `cmd/configcmd/init.go`**

```go
package configcmd

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

func newInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Interactively create ~/.config/spark-cli/config.yaml",
		RunE: func(cmd *cobra.Command, args []string) error {
			home, err := os.UserHomeDir()
			if err != nil {
				return err
			}
			dir := filepath.Join(home, ".config", "spark-cli")
			if v := os.Getenv("SPARK_CLI_CONFIG_DIR"); v != "" {
				dir = v
			}
			if err := os.MkdirAll(dir, 0755); err != nil {
				return err
			}
			r := bufio.NewReader(cmd.InOrStdin())
			out := cmd.OutOrStdout()

			fmt.Fprint(out, "Local EventLog dir [/tmp/spark-events]: ")
			local := readLine(r, "/tmp/spark-events")
			fmt.Fprint(out, "HDFS NameNode (host:port, blank to skip): ")
			nn := readLine(r, "")
			fmt.Fprint(out, "HDFS history dir [/spark-history]: ")
			hpath := readLine(r, "/spark-history")
			fmt.Fprint(out, "HDFS user [$USER]: ")
			huser := readLine(r, os.Getenv("USER"))
			fmt.Fprint(out, "Parse timeout [30s]: ")
			to := readLine(r, "30s")

			var b strings.Builder
			b.WriteString("log_dirs:\n")
			if local != "" {
				fmt.Fprintf(&b, "  - file://%s\n", local)
			}
			if nn != "" {
				fmt.Fprintf(&b, "  - hdfs://%s%s\n", nn, hpath)
			}
			b.WriteString("hdfs:\n")
			fmt.Fprintf(&b, "  user: %s\n", huser)
			fmt.Fprintf(&b, "timeout: %s\n", to)

			path := filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(path, []byte(b.String()), 0644); err != nil {
				return err
			}
			fmt.Fprintf(out, "Wrote %s\n", path)
			return nil
		},
	}
}

func readLine(r *bufio.Reader, def string) string {
	line, _ := r.ReadString('\n')
	line = strings.TrimSpace(line)
	if line == "" {
		return def
	}
	return line
}
```

- [ ] **Step 6: Wire into root**

Modify `cmd/root.go` `Execute` to register configcmd:

```go
import "github.com/opay-bigdata/spark-cli/cmd/configcmd"

// inside Execute:
root.AddCommand(configcmd.New())
```

- [ ] **Step 7: Run tests + smoke**

Run: `go test ./...`
Expected: PASS.

Run: `go run . config show`
Expected: prints "log_dirs (default):" with empty list (or your existing config).

- [ ] **Step 8: Commit**

```bash
git add cmd/configcmd cmd/root.go
git commit -m "feat(config): config init and show subcommands"
```

---

## Task 7: FS interface + local impl

**Files:**
- Create: `internal/fs/fs.go`, `internal/fs/local.go`, `internal/fs/local_test.go`

- [ ] **Step 1: Write failing test `internal/fs/local_test.go`**

```go
package fs

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalOpenStatList(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "application_1_a"), []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "application_2_b.zstd"), []byte("xx"), 0644); err != nil {
		t.Fatal(err)
	}
	l := NewLocal()

	r, err := l.Open("file://" + filepath.Join(dir, "application_1_a"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	body, _ := io.ReadAll(r)
	r.Close()
	if string(body) != "hello" {
		t.Fatalf("body = %q", body)
	}

	st, err := l.Stat("file://" + filepath.Join(dir, "application_1_a"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Size != 5 {
		t.Fatalf("size = %d", st.Size)
	}

	matches, err := l.List("file://"+dir, "application_")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Fatalf("matches = %v", matches)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/fs/...`
Expected: FAIL — package not implemented.

- [ ] **Step 3: Implement `internal/fs/fs.go`**

```go
// Package fs abstracts local and HDFS filesystems behind a small interface.
package fs

import "io"

type FileInfo struct {
	URI   string
	Name  string
	Size  int64
	IsDir bool
}

type FS interface {
	Open(uri string) (io.ReadCloser, error)
	Stat(uri string) (FileInfo, error)
	List(dirURI, prefix string) ([]string, error) // returns full URIs
}
```

- [ ] **Step 4: Implement `internal/fs/local.go`**

```go
package fs

import (
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type Local struct{}

func NewLocal() *Local { return &Local{} }

func uriToPath(uri string) (string, error) {
	if !strings.HasPrefix(uri, "file://") {
		return uri, nil
	}
	u, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	return u.Path, nil
}

func (l *Local) Open(uri string) (io.ReadCloser, error) {
	p, err := uriToPath(uri)
	if err != nil {
		return nil, err
	}
	return os.Open(p)
}

func (l *Local) Stat(uri string) (FileInfo, error) {
	p, err := uriToPath(uri)
	if err != nil {
		return FileInfo{}, err
	}
	st, err := os.Stat(p)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{URI: uri, Name: st.Name(), Size: st.Size(), IsDir: st.IsDir()}, nil
}

func (l *Local) List(dirURI, prefix string) ([]string, error) {
	p, err := uriToPath(dirURI)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(p)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) {
			out = append(out, "file://"+filepath.Join(p, e.Name()))
		}
	}
	return out, nil
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/fs/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/fs
git commit -m "feat(fs): FS interface and local implementation"
```

---

## Task 8: HDFS impl (colinmarc/hdfs)

**Files:**
- Create: `internal/fs/hdfs.go`, `internal/fs/hdfs_test.go`

- [ ] **Step 1: Add hdfs dep**

Run: `go get github.com/colinmarc/hdfs/v2@v2.4.0 && go mod tidy`

- [ ] **Step 2: Write build-tag-gated integration test `internal/fs/hdfs_test.go`**

```go
//go:build hdfs_integration

package fs

import (
	"io"
	"os"
	"testing"
)

func TestHDFSReadIfEnvSet(t *testing.T) {
	addr := os.Getenv("SPARK_CLI_TEST_HDFS_ADDR")
	if addr == "" {
		t.Skip("set SPARK_CLI_TEST_HDFS_ADDR to run")
	}
	h, err := NewHDFS(addr, "")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	rc, err := h.Open("hdfs://" + addr + "/tmp/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	if len(body) == 0 {
		t.Fatal("empty")
	}
}
```

- [ ] **Step 3: Implement `internal/fs/hdfs.go`**

```go
package fs

import (
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	hdfs "github.com/colinmarc/hdfs/v2"
)

type HDFS struct {
	client *hdfs.Client
	addr   string
}

func NewHDFS(addr, user string) (*HDFS, error) {
	if user == "" {
		user = os.Getenv("USER")
	}
	opts := hdfs.ClientOptions{Addresses: []string{addr}, User: user}
	c, err := hdfs.NewClient(opts)
	if err != nil {
		return nil, err
	}
	return &HDFS{client: c, addr: addr}, nil
}

func (h *HDFS) Close() error { return h.client.Close() }

func uriHDFSPath(uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	if u.Scheme != "hdfs" {
		return "", os.ErrInvalid
	}
	return u.Path, nil
}

func (h *HDFS) Open(uri string) (io.ReadCloser, error) {
	p, err := uriHDFSPath(uri)
	if err != nil {
		return nil, err
	}
	return h.client.Open(p)
}

func (h *HDFS) Stat(uri string) (FileInfo, error) {
	p, err := uriHDFSPath(uri)
	if err != nil {
		return FileInfo{}, err
	}
	st, err := h.client.Stat(p)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{URI: uri, Name: st.Name(), Size: st.Size(), IsDir: st.IsDir()}, nil
}

func (h *HDFS) List(dirURI, prefix string) ([]string, error) {
	p, err := uriHDFSPath(dirURI)
	if err != nil {
		return nil, err
	}
	entries, err := h.client.ReadDir(p)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) {
			out = append(out, "hdfs://"+h.addr+path.Join(p, e.Name()))
		}
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/fs/...`
Expected: PASS (hdfs test skipped without env).

- [ ] **Step 5: Commit**

```bash
git add internal/fs/hdfs.go internal/fs/hdfs_test.go go.mod go.sum
git commit -m "feat(fs): HDFS implementation via colinmarc/hdfs"
```

---

## Task 9: EventLog reader — decompression dispatch

**Files:**
- Create: `internal/eventlog/reader.go`, `internal/eventlog/reader_test.go`

- [ ] **Step 1: Add zstd/lz4/snappy deps**

```bash
go get github.com/klauspost/compress/zstd@latest
go get github.com/pierrec/lz4/v4@latest
go get github.com/golang/snappy@latest
go mod tidy
```

- [ ] **Step 2: Write failing test `internal/eventlog/reader_test.go`**

```go
package eventlog

import (
	"bytes"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestOpenPlain(t *testing.T) {
	r, err := openCompressed(io.NopCloser(bytes.NewReader([]byte("hello"))), CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(r)
	if string(body) != "hello" {
		t.Fatalf("body = %q", body)
	}
}

func TestOpenZstd(t *testing.T) {
	var buf bytes.Buffer
	w, _ := zstd.NewWriter(&buf)
	_, _ = w.Write([]byte("zstd payload"))
	_ = w.Close()
	r, err := openCompressed(io.NopCloser(&buf), CompressionZstd)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(r)
	if string(body) != "zstd payload" {
		t.Fatalf("body = %q", body)
	}
}

func TestDetectCompressionFromName(t *testing.T) {
	cases := map[string]Compression{
		"application_1_a":              CompressionNone,
		"application_1_a.zstd":         CompressionZstd,
		"application_1_a.lz4":          CompressionLZ4,
		"application_1_a.snappy":       CompressionSnappy,
		"application_1_a.zstd.inprogress": CompressionZstd,
		"application_1_a.inprogress":   CompressionNone,
	}
	for name, want := range cases {
		if got := DetectCompression(name); got != want {
			t.Errorf("%s: got %v want %v", name, got, want)
		}
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/eventlog/...`
Expected: FAIL — package not implemented.

- [ ] **Step 4: Implement `internal/eventlog/reader.go`**

```go
// Package eventlog locates and reads Spark EventLog files (V1 single file or V2 directory).
package eventlog

import (
	"io"
	"strings"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type Compression string

const (
	CompressionNone   Compression = "none"
	CompressionZstd   Compression = "zstd"
	CompressionLZ4    Compression = "lz4"
	CompressionSnappy Compression = "snappy"
)

// DetectCompression strips trailing .inprogress then matches the next extension.
func DetectCompression(name string) Compression {
	n := strings.TrimSuffix(name, ".inprogress")
	switch {
	case strings.HasSuffix(n, ".zstd"):
		return CompressionZstd
	case strings.HasSuffix(n, ".lz4"):
		return CompressionLZ4
	case strings.HasSuffix(n, ".snappy"):
		return CompressionSnappy
	default:
		return CompressionNone
	}
}

// openCompressed wraps the underlying reader with the appropriate decompressor.
// Caller must Close the returned reader.
func openCompressed(rc io.ReadCloser, c Compression) (io.ReadCloser, error) {
	switch c {
	case CompressionNone:
		return rc, nil
	case CompressionZstd:
		zr, err := zstd.NewReader(rc)
		if err != nil {
			_ = rc.Close()
			return nil, err
		}
		return &zstdCloser{zr: zr, raw: rc}, nil
	case CompressionLZ4:
		return &readClose{r: lz4.NewReader(rc), raw: rc}, nil
	case CompressionSnappy:
		return &readClose{r: snappy.NewReader(rc), raw: rc}, nil
	}
	return rc, nil
}

type zstdCloser struct {
	zr  *zstd.Decoder
	raw io.Closer
}

func (z *zstdCloser) Read(p []byte) (int, error) { return z.zr.Read(p) }
func (z *zstdCloser) Close() error               { z.zr.Close(); return z.raw.Close() }

type readClose struct {
	r   io.Reader
	raw io.Closer
}

func (r *readClose) Read(p []byte) (int, error) { return r.r.Read(p) }
func (r *readClose) Close() error                { return r.raw.Close() }
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/eventlog/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/eventlog/reader.go internal/eventlog/reader_test.go go.mod go.sum
git commit -m "feat(eventlog): compression detection and reader dispatch"
```

---

## Task 10: EventLog locator — appId → LogSource (V1)

**Files:**
- Create: `internal/eventlog/locate.go`, `internal/eventlog/locate_test.go`

- [ ] **Step 1: Write failing test `internal/eventlog/locate_test.go`**

```go
package eventlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/fs"
)

func writeFile(t *testing.T, p string) {
	t.Helper()
	if err := os.WriteFile(p, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeAppID(t *testing.T) {
	cases := map[string]string{
		"application_1735000000_0001":          "application_1735000000_0001",
		"1735000000_0001":                      "application_1735000000_0001",
		"application_1735000000_0001.zstd":     "application_1735000000_0001",
		"application_1735000000_0001.inprogress": "application_1735000000_0001",
		"application_1735000000_0001.zstd.inprogress": "application_1735000000_0001",
	}
	for in, want := range cases {
		if got := normalizeAppID(in); got != want {
			t.Errorf("%s: got %s want %s", in, got, want)
		}
	}
}

func TestResolveV1Plain(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "application_1_a"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if src.Format != "v1" || src.Compression != CompressionNone || src.Incomplete {
		t.Errorf("source = %+v", src)
	}
}

func TestResolveV1Zstd(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "application_1_a.zstd"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if src.Compression != CompressionZstd {
		t.Errorf("got %v", src.Compression)
	}
}

func TestResolveV1Inprogress(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "application_1_a.zstd.inprogress"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if !src.Incomplete || src.Compression != CompressionZstd {
		t.Errorf("got %+v", src)
	}
}

func TestResolveAmbiguous(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "application_1_a"))
	writeFile(t, filepath.Join(dir, "application_1_a.zstd"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	if _, err := loc.Resolve("application_1_a"); err == nil {
		t.Fatal("want APP_AMBIGUOUS")
	}
}

func TestResolveNotFound(t *testing.T) {
	dir := t.TempDir()
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	if _, err := loc.Resolve("application_1_a"); err == nil {
		t.Fatal("want APP_NOT_FOUND")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/eventlog/...`
Expected: FAIL — Locator/normalizeAppID undefined.

- [ ] **Step 3: Implement `internal/eventlog/locate.go`** (V1 only; V2 added next task)

```go
package eventlog

import (
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/fs"

	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
)

type LogSource struct {
	URI         string
	Format      string // "v1" | "v2"
	Compression Compression
	Incomplete  bool
	Parts       []string // V2 ordered URIs; V1 nil
	SizeBytes   int64
}

type Locator struct {
	fsByScheme map[string]fs.FS
	logDirs    []string
}

func NewLocator(fsByScheme map[string]fs.FS, logDirs []string) *Locator {
	return &Locator{fsByScheme: fsByScheme, logDirs: logDirs}
}

func normalizeAppID(s string) string {
	s = strings.TrimSuffix(s, ".inprogress")
	for _, ext := range []string{".zstd", ".lz4", ".snappy"} {
		s = strings.TrimSuffix(s, ext)
	}
	if !strings.HasPrefix(s, "application_") && !strings.HasPrefix(s, "eventlog_v2_application_") {
		s = "application_" + s
	}
	return s
}

func (l *Locator) Resolve(appIDInput string) (LogSource, error) {
	appID := normalizeAppID(appIDInput)
	for _, dir := range l.logDirs {
		fsys, err := l.fsFor(dir)
		if err != nil {
			return LogSource{}, err
		}
		if src, ok, err := l.resolveV1(fsys, dir, appID); err != nil {
			return LogSource{}, err
		} else if ok {
			return src, nil
		}
	}
	return LogSource{}, cerrors.New(cerrors.CodeAppNotFound,
		fmt.Sprintf("no EventLog matching %s in any log_dir", appID),
		"check log_dirs in config or pass --log-dirs")
}

func (l *Locator) fsFor(dirURI string) (fs.FS, error) {
	u, err := url.Parse(dirURI)
	if err != nil {
		return nil, cerrors.New(cerrors.CodeFlagInvalid, "bad log_dir: "+dirURI, "use file:// or hdfs:// URI")
	}
	fsys, ok := l.fsByScheme[u.Scheme]
	if !ok {
		return nil, cerrors.New(cerrors.CodeFlagInvalid, "unsupported scheme: "+u.Scheme, "use file:// or hdfs://")
	}
	return fsys, nil
}

func (l *Locator) resolveV1(fsys fs.FS, dirURI, appID string) (LogSource, bool, error) {
	all, err := fsys.List(dirURI, appID)
	if err != nil {
		return LogSource{}, false, cerrors.New(cerrors.CodeLogUnreadable, err.Error(), "")
	}
	var matches []string
	for _, uri := range all {
		base := path.Base(uri)
		stripped := strings.TrimSuffix(base, ".inprogress")
		stripped = strings.TrimSuffix(stripped, ".zstd")
		stripped = strings.TrimSuffix(stripped, ".lz4")
		stripped = strings.TrimSuffix(stripped, ".snappy")
		if stripped == appID {
			matches = append(matches, uri)
		}
	}
	if len(matches) == 0 {
		return LogSource{}, false, nil
	}
	if len(matches) > 1 {
		return LogSource{}, false, cerrors.New(cerrors.CodeAppAmbiguous,
			fmt.Sprintf("multiple matches for %s: %v", appID, matches),
			"give the full applicationId including timestamp")
	}
	uri := matches[0]
	st, err := fsys.Stat(uri)
	if err != nil {
		return LogSource{}, false, cerrors.New(cerrors.CodeLogUnreadable, err.Error(), "")
	}
	base := path.Base(uri)
	return LogSource{
		URI:         uri,
		Format:      "v1",
		Compression: DetectCompression(base),
		Incomplete:  strings.HasSuffix(base, ".inprogress"),
		SizeBytes:   st.Size,
	}, true, nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/eventlog/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/eventlog/locate.go internal/eventlog/locate_test.go
git commit -m "feat(eventlog): V1 single-file locator with appId normalization"
```

---

## Task 11: Locator V2 directory support

**Files:**
- Modify: `internal/eventlog/locate.go`
- Modify: `internal/eventlog/locate_test.go`

- [ ] **Step 1: Add failing tests for V2**

Append to `internal/eventlog/locate_test.go`:

```go
func TestResolveV2(t *testing.T) {
	dir := t.TempDir()
	v2dir := filepath.Join(dir, "eventlog_v2_application_1_a")
	if err := os.MkdirAll(v2dir, 0755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(v2dir, "appstatus_application_1_a"))
	writeFile(t, filepath.Join(v2dir, "events_2_application_1_a.zstd"))
	writeFile(t, filepath.Join(v2dir, "events_1_application_1_a.zstd"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if src.Format != "v2" || len(src.Parts) != 2 {
		t.Fatalf("source = %+v", src)
	}
	// part 1 must come before part 2
	if !strings.HasSuffix(src.Parts[0], "events_1_application_1_a.zstd") {
		t.Fatalf("parts not sorted: %v", src.Parts)
	}
}

func TestResolveV2MissingParts(t *testing.T) {
	dir := t.TempDir()
	v2dir := filepath.Join(dir, "eventlog_v2_application_1_a")
	if err := os.MkdirAll(v2dir, 0755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(v2dir, "appstatus_application_1_a"))
	writeFile(t, filepath.Join(v2dir, "events_1_application_1_a.zstd"))
	writeFile(t, filepath.Join(v2dir, "events_3_application_1_a.zstd")) // skip 2
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	if _, err := loc.Resolve("application_1_a"); err == nil {
		t.Fatal("want LOG_INCOMPLETE")
	}
}
```

Add `import "strings"` if missing.

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./internal/eventlog/...`
Expected: FAIL — V2 not implemented.

- [ ] **Step 3: Add V2 resolver in `internal/eventlog/locate.go`**

Modify `Resolve` to also try V2 after V1 misses:

```go
func (l *Locator) Resolve(appIDInput string) (LogSource, error) {
	appID := normalizeAppID(appIDInput)
	for _, dir := range l.logDirs {
		fsys, err := l.fsFor(dir)
		if err != nil {
			return LogSource{}, err
		}
		if src, ok, err := l.resolveV1(fsys, dir, appID); err != nil {
			return LogSource{}, err
		} else if ok {
			return src, nil
		}
		if src, ok, err := l.resolveV2(fsys, dir, appID); err != nil {
			return LogSource{}, err
		} else if ok {
			return src, nil
		}
	}
	return LogSource{}, cerrors.New(cerrors.CodeAppNotFound,
		fmt.Sprintf("no EventLog matching %s in any log_dir", appID),
		"check log_dirs in config or pass --log-dirs")
}
```

Add helper:

```go
import (
	"regexp"
	"sort"
	"strconv"
)

var v2PartRE = regexp.MustCompile(`^events_(\d+)_(.+)$`)

func (l *Locator) resolveV2(fsys fs.FS, dirURI, appID string) (LogSource, bool, error) {
	v2Name := "eventlog_v2_" + appID
	dirs, err := fsys.List(dirURI, v2Name)
	if err != nil {
		return LogSource{}, false, cerrors.New(cerrors.CodeLogUnreadable, err.Error(), "")
	}
	var v2URI string
	for _, d := range dirs {
		if path.Base(d) == v2Name {
			st, err := fsys.Stat(d)
			if err != nil {
				return LogSource{}, false, cerrors.New(cerrors.CodeLogUnreadable, err.Error(), "")
			}
			if st.IsDir {
				v2URI = d
				break
			}
		}
	}
	if v2URI == "" {
		return LogSource{}, false, nil
	}
	parts, err := fsys.List(v2URI, "events_")
	if err != nil {
		return LogSource{}, false, cerrors.New(cerrors.CodeLogUnreadable, err.Error(), "")
	}
	type indexed struct {
		idx int
		uri string
	}
	var indexed_ []indexed
	var compression Compression
	for _, p := range parts {
		base := path.Base(p)
		stripped := strings.TrimSuffix(base, ".inprogress")
		stripped = strings.TrimSuffix(stripped, ".zstd")
		stripped = strings.TrimSuffix(stripped, ".lz4")
		stripped = strings.TrimSuffix(stripped, ".snappy")
		m := v2PartRE.FindStringSubmatch(stripped)
		if m == nil {
			continue
		}
		n, _ := strconv.Atoi(m[1])
		indexed_ = append(indexed_, indexed{idx: n, uri: p})
		compression = DetectCompression(base)
	}
	if len(indexed_) == 0 {
		return LogSource{}, false, nil
	}
	sort.Slice(indexed_, func(i, j int) bool { return indexed_[i].idx < indexed_[j].idx })
	for i, it := range indexed_ {
		if it.idx != i+1 {
			return LogSource{}, false, cerrors.New(cerrors.CodeLogIncomplete,
				fmt.Sprintf("V2 EventLog %s missing part %d", appID, i+1),
				"check if upload is complete")
		}
	}
	var totalSize int64
	urls := make([]string, 0, len(indexed_))
	for _, it := range indexed_ {
		urls = append(urls, it.uri)
		if st, err := fsys.Stat(it.uri); err == nil {
			totalSize += st.Size
		}
	}
	return LogSource{
		URI:         v2URI,
		Format:      "v2",
		Compression: compression,
		Parts:       urls,
		SizeBytes:   totalSize,
	}, true, nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/eventlog/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/eventlog/locate.go internal/eventlog/locate_test.go
git commit -m "feat(eventlog): V2 directory locator with part ordering"
```

---

## Task 12: Open helper — concatenate V2 parts + decompress as single stream

**Files:**
- Modify: `internal/eventlog/reader.go`
- Create: `internal/eventlog/open_test.go`

- [ ] **Step 1: Write failing test `internal/eventlog/open_test.go`**

```go
package eventlog

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/fs"
)

func TestOpenLogSourceV1Plain(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "evt")
	_ = os.WriteFile(p, []byte("hello\nworld\n"), 0644)
	src := LogSource{URI: "file://" + p, Format: "v1", Compression: CompressionNone}
	rc, err := Open(src, fs.NewLocal())
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	if string(body) != "hello\nworld\n" {
		t.Fatalf("body = %q", body)
	}
}

func TestOpenLogSourceV2Concat(t *testing.T) {
	dir := t.TempDir()
	p1 := filepath.Join(dir, "events_1")
	p2 := filepath.Join(dir, "events_2")
	_ = os.WriteFile(p1, []byte("part1\n"), 0644)
	_ = os.WriteFile(p2, []byte("part2\n"), 0644)
	src := LogSource{
		URI:         "file://" + dir,
		Format:      "v2",
		Compression: CompressionNone,
		Parts:       []string{"file://" + p1, "file://" + p2},
	}
	rc, err := Open(src, fs.NewLocal())
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	if string(body) != "part1\npart2\n" {
		t.Fatalf("body = %q", body)
	}
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `go test ./internal/eventlog/...`
Expected: FAIL — Open undefined.

- [ ] **Step 3: Add Open function to `internal/eventlog/reader.go`**

```go
import "github.com/opay-bigdata/spark-cli/internal/fs"

// Open returns a single io.ReadCloser combining all parts of a LogSource and
// applying decompression. Caller must Close.
func Open(src LogSource, fsys fs.FS) (io.ReadCloser, error) {
	if src.Format == "v2" {
		readers := make([]io.Reader, 0, len(src.Parts))
		closers := make([]io.Closer, 0, len(src.Parts))
		for _, uri := range src.Parts {
			rc, err := fsys.Open(uri)
			if err != nil {
				for _, c := range closers {
					_ = c.Close()
				}
				return nil, err
			}
			readers = append(readers, rc)
			closers = append(closers, rc)
		}
		multi := &multiCloser{r: io.MultiReader(readers...), closers: closers}
		return openCompressed(multi, src.Compression)
	}
	rc, err := fsys.Open(src.URI)
	if err != nil {
		return nil, err
	}
	return openCompressed(rc, src.Compression)
}

type multiCloser struct {
	r       io.Reader
	closers []io.Closer
}

func (m *multiCloser) Read(p []byte) (int, error) { return m.r.Read(p) }
func (m *multiCloser) Close() error {
	var first error
	for _, c := range m.closers {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/eventlog/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/eventlog/reader.go internal/eventlog/open_test.go
git commit -m "feat(eventlog): Open helper combining parts and decompression"
```

---

## Task 13: Stats package — t-digest wrapper

**Files:**
- Create: `internal/stats/tdigest.go`, `internal/stats/tdigest_test.go`

- [ ] **Step 1: Add tdigest dep**

```bash
go get github.com/influxdata/tdigest@latest
go mod tidy
```

- [ ] **Step 2: Write failing test `internal/stats/tdigest_test.go`**

```go
package stats

import "testing"

func TestDigestEmptyReturnsZero(t *testing.T) {
	d := NewDigest()
	if d.Quantile(0.5) != 0 {
		t.Fatal("empty digest must return 0")
	}
}

func TestDigestRoughPercentile(t *testing.T) {
	d := NewDigest()
	for i := 1; i <= 1000; i++ {
		d.Add(float64(i))
	}
	median := d.Quantile(0.5)
	if median < 480 || median > 520 {
		t.Fatalf("median %f out of expected range", median)
	}
	p99 := d.Quantile(0.99)
	if p99 < 985 || p99 > 1000 {
		t.Fatalf("p99 %f out of expected range", p99)
	}
}
```

- [ ] **Step 3: Run test to verify failure**

Run: `go test ./internal/stats/...`
Expected: FAIL — package not implemented.

- [ ] **Step 4: Implement `internal/stats/tdigest.go`**

```go
// Package stats wraps t-digest for percentile estimates.
package stats

import "github.com/influxdata/tdigest"

type Digest struct {
	td    *tdigest.TDigest
	count int
}

func NewDigest() *Digest {
	return &Digest{td: tdigest.NewWithCompression(100)}
}

func (d *Digest) Add(v float64) {
	d.td.Add(v, 1)
	d.count++
}

func (d *Digest) Count() int { return d.count }

func (d *Digest) Quantile(q float64) float64 {
	if d.count == 0 {
		return 0
	}
	return d.td.Quantile(q)
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/stats/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/stats go.mod go.sum
git commit -m "feat(stats): t-digest percentile wrapper"
```

---

## Task 14: Model types — Application / Stage / Executor / Job / Task keys

**Files:**
- Create: `internal/model/model.go`, `internal/model/model_test.go`

- [ ] **Step 1: Write failing test `internal/model/model_test.go`**

```go
package model

import "testing"

func TestNewApplicationInitializesMaps(t *testing.T) {
	a := NewApplication()
	if a.Executors == nil || a.Jobs == nil || a.Stages == nil {
		t.Fatal("maps not initialized")
	}
}

func TestStageKeyEquality(t *testing.T) {
	k1 := StageKey{ID: 7, Attempt: 0}
	k2 := StageKey{ID: 7, Attempt: 0}
	if k1 != k2 {
		t.Fatal("equal keys should be ==")
	}
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `go test ./internal/model/...`
Expected: FAIL.

- [ ] **Step 3: Implement `internal/model/model.go`**

```go
// Package model holds the in-memory aggregated Spark application model.
package model

import "github.com/opay-bigdata/spark-cli/internal/stats"

type StageKey struct {
	ID      int
	Attempt int
}

type Application struct {
	ID, Name, User string
	StartMs, EndMs int64
	DurationMs     int64

	Executors map[string]*Executor
	Jobs      map[int]*Job
	Stages    map[StageKey]*Stage
}

func NewApplication() *Application {
	return &Application{
		Executors: map[string]*Executor{},
		Jobs:      map[int]*Job{},
		Stages:    map[StageKey]*Stage{},
	}
}

type Executor struct {
	ID, Host        string
	Cores           int
	AddMs, RemoveMs int64
	RemoveReason    string
	TotalRunMs      int64
	TotalGCMs       int64
	TaskCount       int
	FailedTaskCount int
}

type Job struct {
	ID         int
	StageIDs   []int
	StartMs    int64
	EndMs      int64
	Result     string // "succeeded" | "failed"
}

type Stage struct {
	ID, Attempt          int
	Name                 string
	NumTasks             int
	SubmitMs, CompleteMs int64
	Status               string

	TaskDurations  *stats.Digest
	TaskInputBytes *stats.Digest

	TotalShuffleReadBytes  int64
	TotalShuffleWriteBytes int64
	TotalSpillDisk         int64
	TotalSpillMem          int64
	TotalGCMs              int64
	TotalRunMs             int64
	FailedTasks            int
	KilledTasks            int
	MaxTaskMs              int64
	MinTaskMs              int64
	MaxInputBytes          int64
}

func NewStage(id, attempt int, name string, numTasks int, submitMs int64) *Stage {
	return &Stage{
		ID:             id,
		Attempt:        attempt,
		Name:           name,
		NumTasks:       numTasks,
		SubmitMs:       submitMs,
		Status:         "running",
		TaskDurations:  stats.NewDigest(),
		TaskInputBytes: stats.NewDigest(),
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/model/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/model
git commit -m "feat(model): application/stage/executor/job model types"
```

---

## Task 15: Event types + JSONL decoder + aggregator (single test cycle)

This task is larger because event types and aggregator are tightly coupled. Tests assert end-to-end aggregation from synthetic JSONL.

**Files:**
- Create: `internal/eventlog/events.go`
- Create: `internal/eventlog/decoder.go`
- Create: `internal/model/aggregator.go`
- Create: `internal/eventlog/decoder_test.go`
- Create: `tests/testdata/tiny_app.json` (committed; see step 1)

- [ ] **Step 1: Create synthetic test data `tests/testdata/tiny_app.json`**

```json
{"Event":"SparkListenerApplicationStart","App Name":"tiny","App ID":"application_1_a","Timestamp":1000,"User":"alice"}
{"Event":"SparkListenerExecutorAdded","Timestamp":1100,"Executor ID":"1","Executor Info":{"Host":"h1","Total Cores":2}}
{"Event":"SparkListenerJobStart","Job ID":0,"Submission Time":1200,"Stage IDs":[0]}
{"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":0,"Stage Attempt ID":0,"Stage Name":"map","Number of Tasks":2,"Submission Time":1200}}
{"Event":"SparkListenerTaskEnd","Stage ID":0,"Stage Attempt ID":0,"Task Info":{"Task ID":1,"Executor ID":"1","Host":"h1","Launch Time":1200,"Finish Time":1500,"Failed":false,"Killed":false},"Task Metrics":{"Executor Run Time":300,"Executor CPU Time":250000000,"JVM GC Time":10,"Input Metrics":{"Bytes Read":1000,"Records Read":10},"Output Metrics":{"Bytes Written":0,"Records Written":0},"Shuffle Read Metrics":{"Remote Bytes Read":0,"Local Bytes Read":0,"Fetch Wait Time":0,"Total Records Read":0},"Shuffle Write Metrics":{"Shuffle Bytes Written":500,"Shuffle Write Time":0,"Shuffle Records Written":5},"Memory Bytes Spilled":0,"Disk Bytes Spilled":0}}
{"Event":"SparkListenerTaskEnd","Stage ID":0,"Stage Attempt ID":0,"Task Info":{"Task ID":2,"Executor ID":"1","Host":"h1","Launch Time":1200,"Finish Time":1700,"Failed":false,"Killed":false},"Task Metrics":{"Executor Run Time":500,"Executor CPU Time":400000000,"JVM GC Time":50,"Input Metrics":{"Bytes Read":2000,"Records Read":20},"Output Metrics":{"Bytes Written":0,"Records Written":0},"Shuffle Read Metrics":{"Remote Bytes Read":0,"Local Bytes Read":0,"Fetch Wait Time":0,"Total Records Read":0},"Shuffle Write Metrics":{"Shuffle Bytes Written":1500,"Shuffle Write Time":0,"Shuffle Records Written":15},"Memory Bytes Spilled":0,"Disk Bytes Spilled":0}}
{"Event":"SparkListenerStageCompleted","Stage Info":{"Stage ID":0,"Stage Attempt ID":0,"Stage Name":"map","Number of Tasks":2,"Submission Time":1200,"Completion Time":1800}}
{"Event":"SparkListenerJobEnd","Job ID":0,"Completion Time":1800,"Job Result":{"Result":"JobSucceeded"}}
{"Event":"SparkListenerExecutorRemoved","Timestamp":1900,"Executor ID":"1","Removed Reason":"done"}
{"Event":"SparkListenerApplicationEnd","Timestamp":2000}
```

(Each line a single JSON object, no internal newlines.)

- [ ] **Step 2: Write failing decoder test `internal/eventlog/decoder_test.go`**

```go
package eventlog

import (
	"os"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestDecodeTinyApp(t *testing.T) {
	f, err := os.Open("../../tests/testdata/tiny_app.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	app := model.NewApplication()
	agg := model.NewAggregator(app)
	parsed, err := Decode(f, agg)
	if err != nil {
		t.Fatal(err)
	}
	if parsed != 10 {
		t.Errorf("parsed = %d", parsed)
	}
	if app.ID != "application_1_a" || app.Name != "tiny" || app.User != "alice" {
		t.Fatalf("app = %+v", app)
	}
	if app.DurationMs != 1000 {
		t.Errorf("duration = %d", app.DurationMs)
	}
	if len(app.Executors) != 1 {
		t.Errorf("executors = %d", len(app.Executors))
	}
	if len(app.Stages) != 1 {
		t.Errorf("stages = %d", len(app.Stages))
	}
	st := app.Stages[model.StageKey{ID: 0, Attempt: 0}]
	if st == nil || st.Status != "succeeded" {
		t.Fatalf("stage = %+v", st)
	}
	if st.TaskDurations.Count() != 2 {
		t.Errorf("task count = %d", st.TaskDurations.Count())
	}
	if st.MaxTaskMs != 500 {
		t.Errorf("max task = %d", st.MaxTaskMs)
	}
	if st.TotalShuffleWriteBytes != 2000 {
		t.Errorf("shuffle write = %d", st.TotalShuffleWriteBytes)
	}
	if st.TotalGCMs != 60 {
		t.Errorf("gc = %d", st.TotalGCMs)
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./internal/eventlog/...`
Expected: FAIL — Decode/Aggregator undefined.

- [ ] **Step 4: Implement `internal/eventlog/events.go`**

```go
package eventlog

// Minimal event field structs — only what aggregator consumes.
// Field names match Spark's JSON keys exactly.

type evtBase struct {
	Event string `json:"Event"`
}

type evtAppStart struct {
	AppName   string `json:"App Name"`
	AppID     string `json:"App ID"`
	Timestamp int64  `json:"Timestamp"`
	User      string `json:"User"`
}

type evtAppEnd struct {
	Timestamp int64 `json:"Timestamp"`
}

type evtExecutorAdded struct {
	Timestamp    int64  `json:"Timestamp"`
	ExecutorID   string `json:"Executor ID"`
	ExecutorInfo struct {
		Host       string `json:"Host"`
		TotalCores int    `json:"Total Cores"`
	} `json:"Executor Info"`
}

type evtExecutorRemoved struct {
	Timestamp     int64  `json:"Timestamp"`
	ExecutorID    string `json:"Executor ID"`
	RemovedReason string `json:"Removed Reason"`
}

type evtJobStart struct {
	JobID    int   `json:"Job ID"`
	StageIDs []int `json:"Stage IDs"`
	SubmitMs int64 `json:"Submission Time"`
}

type evtJobEnd struct {
	JobID     int   `json:"Job ID"`
	EndMs     int64 `json:"Completion Time"`
	JobResult struct {
		Result string `json:"Result"`
	} `json:"Job Result"`
}

type evtStageInfo struct {
	StageID        int    `json:"Stage ID"`
	StageAttemptID int    `json:"Stage Attempt ID"`
	StageName      string `json:"Stage Name"`
	NumberOfTasks  int    `json:"Number of Tasks"`
	SubmitMs       int64  `json:"Submission Time"`
	CompleteMs     int64  `json:"Completion Time"`
	FailureReason  string `json:"Failure Reason"`
}

type evtStageSubmitted struct {
	StageInfo evtStageInfo `json:"Stage Info"`
}

type evtStageCompleted struct {
	StageInfo evtStageInfo `json:"Stage Info"`
}

type evtTaskEnd struct {
	StageID        int `json:"Stage ID"`
	StageAttemptID int `json:"Stage Attempt ID"`
	TaskInfo       struct {
		TaskID     int    `json:"Task ID"`
		ExecutorID string `json:"Executor ID"`
		Host       string `json:"Host"`
		LaunchTime int64  `json:"Launch Time"`
		FinishTime int64  `json:"Finish Time"`
		Failed     bool   `json:"Failed"`
		Killed     bool   `json:"Killed"`
	} `json:"Task Info"`
	TaskMetrics *struct {
		ExecutorRunTime         int64 `json:"Executor Run Time"`
		ExecutorCPUTime         int64 `json:"Executor CPU Time"`
		ExecutorDeserializeTime int64 `json:"Executor Deserialize Time"`
		JVMGCTime               int64 `json:"JVM GC Time"`
		MemoryBytesSpilled      int64 `json:"Memory Bytes Spilled"`
		DiskBytesSpilled        int64 `json:"Disk Bytes Spilled"`
		InputMetrics            struct {
			BytesRead   int64 `json:"Bytes Read"`
			RecordsRead int64 `json:"Records Read"`
		} `json:"Input Metrics"`
		OutputMetrics struct {
			BytesWritten   int64 `json:"Bytes Written"`
			RecordsWritten int64 `json:"Records Written"`
		} `json:"Output Metrics"`
		ShuffleReadMetrics struct {
			RemoteBytesRead   int64 `json:"Remote Bytes Read"`
			LocalBytesRead    int64 `json:"Local Bytes Read"`
			FetchWaitTime     int64 `json:"Fetch Wait Time"`
			TotalRecordsRead  int64 `json:"Total Records Read"`
		} `json:"Shuffle Read Metrics"`
		ShuffleWriteMetrics struct {
			BytesWritten   int64 `json:"Shuffle Bytes Written"`
			WriteTime      int64 `json:"Shuffle Write Time"`
			RecordsWritten int64 `json:"Shuffle Records Written"`
		} `json:"Shuffle Write Metrics"`
	} `json:"Task Metrics"`
}
```

- [ ] **Step 5: Implement `internal/eventlog/decoder.go`**

```go
package eventlog

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"

	"github.com/opay-bigdata/spark-cli/internal/model"

	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
)

const maxScanLine = 16 << 20 // 16MB per JSONL line

func Decode(r io.Reader, agg *model.Aggregator) (int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), maxScanLine)
	parsed := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var base evtBase
		if err := json.Unmarshal(line, &base); err != nil {
			return parsed, cerrors.New(cerrors.CodeLogParseFailed, err.Error(), "line "+itoa(parsed+1))
		}
		if err := dispatch(base.Event, line, agg); err != nil {
			return parsed, err
		}
		parsed++
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return parsed, cerrors.New(cerrors.CodeLogParseFailed,
				"event JSONL line exceeds 16MB", "file may be corrupted")
		}
		return parsed, cerrors.New(cerrors.CodeLogUnreadable, err.Error(), "")
	}
	return parsed, nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func dispatch(event string, raw []byte, agg *model.Aggregator) error {
	switch event {
	case "SparkListenerApplicationStart":
		var e evtAppStart
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		agg.OnAppStart(e.AppID, e.AppName, e.User, e.Timestamp)
	case "SparkListenerApplicationEnd":
		var e evtAppEnd
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		agg.OnAppEnd(e.Timestamp)
	case "SparkListenerExecutorAdded":
		var e evtExecutorAdded
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		agg.OnExecutorAdded(e.ExecutorID, e.ExecutorInfo.Host, e.ExecutorInfo.TotalCores, e.Timestamp)
	case "SparkListenerExecutorRemoved":
		var e evtExecutorRemoved
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		agg.OnExecutorRemoved(e.ExecutorID, e.Timestamp, e.RemovedReason)
	case "SparkListenerJobStart":
		var e evtJobStart
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		agg.OnJobStart(e.JobID, e.StageIDs, e.SubmitMs)
	case "SparkListenerJobEnd":
		var e evtJobEnd
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		agg.OnJobEnd(e.JobID, e.EndMs, e.JobResult.Result)
	case "SparkListenerStageSubmitted":
		var e evtStageSubmitted
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		s := e.StageInfo
		agg.OnStageSubmitted(s.StageID, s.StageAttemptID, s.StageName, s.NumberOfTasks, s.SubmitMs)
	case "SparkListenerStageCompleted":
		var e evtStageCompleted
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		s := e.StageInfo
		status := "succeeded"
		if s.FailureReason != "" {
			status = "failed"
		}
		agg.OnStageCompleted(s.StageID, s.StageAttemptID, s.CompleteMs, status)
	case "SparkListenerTaskEnd":
		var e evtTaskEnd
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		var m model.TaskMetrics
		if e.TaskMetrics != nil {
			m.RunMs = e.TaskMetrics.ExecutorRunTime
			m.GCMs = e.TaskMetrics.JVMGCTime
			m.InputBytes = e.TaskMetrics.InputMetrics.BytesRead
			m.ShuffleReadBytes = e.TaskMetrics.ShuffleReadMetrics.RemoteBytesRead + e.TaskMetrics.ShuffleReadMetrics.LocalBytesRead
			m.ShuffleWriteBytes = e.TaskMetrics.ShuffleWriteMetrics.BytesWritten
			m.SpillDisk = e.TaskMetrics.DiskBytesSpilled
			m.SpillMem = e.TaskMetrics.MemoryBytesSpilled
		}
		agg.OnTaskEnd(model.TaskEnd{
			StageID:    e.StageID,
			Attempt:    e.StageAttemptID,
			ExecutorID: e.TaskInfo.ExecutorID,
			Failed:     e.TaskInfo.Failed,
			Killed:     e.TaskInfo.Killed,
			LaunchMs:   e.TaskInfo.LaunchTime,
			FinishMs:   e.TaskInfo.FinishTime,
			Metrics:    m,
		})
	}
	// Unknown events skipped silently.
	return nil
}

func parseErr(err error) error {
	return cerrors.New(cerrors.CodeLogParseFailed, err.Error(), "")
}
```

- [ ] **Step 6: Implement `internal/model/aggregator.go`**

```go
package model

type TaskMetrics struct {
	RunMs             int64
	GCMs              int64
	InputBytes        int64
	ShuffleReadBytes  int64
	ShuffleWriteBytes int64
	SpillDisk         int64
	SpillMem          int64
}

type TaskEnd struct {
	StageID, Attempt int
	ExecutorID       string
	Failed, Killed   bool
	LaunchMs         int64
	FinishMs         int64
	Metrics          TaskMetrics
}

type Aggregator struct {
	app            *Application
	concurrentNow  int
	concurrentPeak int
}

func NewAggregator(app *Application) *Aggregator { return &Aggregator{app: app} }

func (a *Aggregator) PeakConcurrentExecutors() int { return a.concurrentPeak }

func (a *Aggregator) OnAppStart(id, name, user string, ts int64) {
	a.app.ID = id
	a.app.Name = name
	a.app.User = user
	a.app.StartMs = ts
}

func (a *Aggregator) OnAppEnd(ts int64) {
	a.app.EndMs = ts
	if a.app.StartMs > 0 {
		a.app.DurationMs = ts - a.app.StartMs
	}
}

func (a *Aggregator) OnExecutorAdded(id, host string, cores int, ts int64) {
	a.app.Executors[id] = &Executor{ID: id, Host: host, Cores: cores, AddMs: ts}
	a.concurrentNow++
	if a.concurrentNow > a.concurrentPeak {
		a.concurrentPeak = a.concurrentNow
	}
}

func (a *Aggregator) OnExecutorRemoved(id string, ts int64, reason string) {
	if e, ok := a.app.Executors[id]; ok {
		e.RemoveMs = ts
		e.RemoveReason = reason
	}
	if a.concurrentNow > 0 {
		a.concurrentNow--
	}
}

func (a *Aggregator) OnJobStart(id int, stageIDs []int, submitMs int64) {
	a.app.Jobs[id] = &Job{ID: id, StageIDs: stageIDs, StartMs: submitMs}
}

func (a *Aggregator) OnJobEnd(id int, endMs int64, result string) {
	if j, ok := a.app.Jobs[id]; ok {
		j.EndMs = endMs
		if result == "JobSucceeded" {
			j.Result = "succeeded"
		} else {
			j.Result = "failed"
		}
	}
}

func (a *Aggregator) OnStageSubmitted(id, attempt int, name string, numTasks int, submitMs int64) {
	k := StageKey{ID: id, Attempt: attempt}
	if _, exists := a.app.Stages[k]; !exists {
		a.app.Stages[k] = NewStage(id, attempt, name, numTasks, submitMs)
	}
}

func (a *Aggregator) OnStageCompleted(id, attempt int, completeMs int64, status string) {
	k := StageKey{ID: id, Attempt: attempt}
	s, ok := a.app.Stages[k]
	if !ok {
		s = NewStage(id, attempt, "", 0, 0)
		a.app.Stages[k] = s
	}
	s.CompleteMs = completeMs
	s.Status = status
}

func (a *Aggregator) OnTaskEnd(t TaskEnd) {
	k := StageKey{ID: t.StageID, Attempt: t.Attempt}
	s, ok := a.app.Stages[k]
	if !ok {
		s = NewStage(t.StageID, t.Attempt, "", 0, 0)
		a.app.Stages[k] = s
	}
	dur := t.Metrics.RunMs
	if dur == 0 && t.FinishMs > t.LaunchMs {
		dur = t.FinishMs - t.LaunchMs
	}
	s.TaskDurations.Add(float64(dur))
	s.TaskInputBytes.Add(float64(t.Metrics.InputBytes))
	s.TotalRunMs += dur
	s.TotalGCMs += t.Metrics.GCMs
	s.TotalShuffleReadBytes += t.Metrics.ShuffleReadBytes
	s.TotalShuffleWriteBytes += t.Metrics.ShuffleWriteBytes
	s.TotalSpillDisk += t.Metrics.SpillDisk
	s.TotalSpillMem += t.Metrics.SpillMem
	if dur > s.MaxTaskMs {
		s.MaxTaskMs = dur
	}
	if s.MinTaskMs == 0 || dur < s.MinTaskMs {
		s.MinTaskMs = dur
	}
	if t.Metrics.InputBytes > s.MaxInputBytes {
		s.MaxInputBytes = t.Metrics.InputBytes
	}
	if t.Failed {
		s.FailedTasks++
	}
	if t.Killed {
		s.KilledTasks++
	}

	if e, ok := a.app.Executors[t.ExecutorID]; ok {
		e.TotalRunMs += dur
		e.TotalGCMs += t.Metrics.GCMs
		e.TaskCount++
		if t.Failed {
			e.FailedTaskCount++
		}
	}
}
```

- [ ] **Step 7: Run tests**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/eventlog internal/model tests/testdata/tiny_app.json
git commit -m "feat(eventlog): event types, JSONL decoder, aggregator"
```

---

## Task 16 — Scenario result envelope

**Goal**: Define the JSON envelope type used by all scenarios so output formatters can stay scenario-agnostic.

- [ ] **Step 1: Failing test** — `internal/scenario/result_test.go`

```go
package scenario

import (
	"encoding/json"
	"testing"
)

func TestEnvelopeMarshalsRequiredKeys(t *testing.T) {
	env := Envelope{
		Scenario:     "app-summary",
		AppID:        "application_1_1",
		AppName:      "etl",
		LogPath:      "file:///tmp/x",
		LogFormat:    "v1",
		Compression:  "none",
		Incomplete:   false,
		ParsedEvents: 10,
		ElapsedMs:    7,
		Columns:      []string{"app_id"},
		Data:         []any{map[string]any{"app_id": "application_1_1"}},
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, k := range []string{
		"scenario", "app_id", "app_name", "log_path", "log_format",
		"compression", "incomplete", "parsed_events", "elapsed_ms", "columns", "data",
	} {
		if _, ok := m[k]; !ok {
			t.Errorf("envelope missing key %q", k)
		}
	}
}

func TestEnvelopeOmitsSummaryWhenNil(t *testing.T) {
	env := Envelope{Scenario: "slow-stages"}
	b, _ := json.Marshal(env)
	if got := string(b); contains(got, "summary") {
		t.Errorf("expected summary omitted, got %s", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
```

Run: `go test ./internal/scenario/...` → FAIL (no Envelope type).

- [ ] **Step 2: Implement** — `internal/scenario/result.go`

```go
package scenario

// Envelope is the canonical JSON shape returned by every scenario.
// data is `any` because gc-pressure returns an object, others return arrays.
// Columns mirrors data: []string for arrays, map[string][]string for gc-pressure.
type Envelope struct {
	Scenario     string `json:"scenario"`
	AppID        string `json:"app_id"`
	AppName      string `json:"app_name"`
	LogPath      string `json:"log_path"`
	LogFormat    string `json:"log_format"`
	Compression  string `json:"compression"`
	Incomplete   bool   `json:"incomplete"`
	ParsedEvents int64  `json:"parsed_events"`
	ElapsedMs    int64  `json:"elapsed_ms"`
	Columns      any    `json:"columns"`
	Data         any    `json:"data"`
	Summary      any    `json:"summary,omitempty"`
}

type DiagnoseSummary struct {
	Critical int `json:"critical"`
	Warn     int `json:"warn"`
	OK       int `json:"ok"`
}
```

- [ ] **Step 3: Run** — `go test ./internal/scenario/...` → PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/scenario/result.go internal/scenario/result_test.go
git commit -m "feat(scenario): JSON envelope type"
```

---

## Task 17 — `app-summary`

**Goal**: Build the single-row summary from an aggregated `*model.Application`.

- [ ] **Step 1: Failing test** — `internal/scenario/app_summary_test.go`

```go
package scenario

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestAppSummaryComputesGCRatioAndTopStages(t *testing.T) {
	app := model.NewApplication()
	app.ID = "application_1_1"
	app.Name = "etl"
	app.User = "alice"
	app.StartMs = 1000
	app.EndMs = 4000
	app.JobsTotal = 2
	app.TasksTotal = 100
	app.TasksFailed = 1
	app.TotalRunMs = 10000
	app.TotalGCMs = 1000

	for i, dur := range []int64{500, 200, 800} {
		s := model.NewStage(i, 0, "stage", 10, 0)
		s.SubmitMs = 0
		s.CompleteMs = dur
		s.Status = "succeeded"
		app.Stages[model.StageKey{ID: i, Attempt: 0}] = s
	}
	app.MaxConcurrentExecutors = 5

	row := AppSummary(app)
	if row.GCRatio < 0.099 || row.GCRatio > 0.101 {
		t.Errorf("gc_ratio=%v want ~0.1", row.GCRatio)
	}
	if len(row.TopStagesByDuration) != 3 {
		t.Fatalf("top_stages_by_duration=%d want 3", len(row.TopStagesByDuration))
	}
	if row.TopStagesByDuration[0].StageID != 2 || row.TopStagesByDuration[0].DurationMs != 800 {
		t.Errorf("top stage order wrong: %+v", row.TopStagesByDuration)
	}
}
```

Run: `go test ./internal/scenario/...` → FAIL.

- [ ] **Step 2: Implement** — `internal/scenario/app_summary.go`

```go
package scenario

import (
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type TopStage struct {
	StageID    int    `json:"stage_id"`
	Name       string `json:"name"`
	DurationMs int64  `json:"duration_ms"`
}

type AppSummaryRow struct {
	AppID                  string     `json:"app_id"`
	AppName                string     `json:"app_name"`
	User                   string     `json:"user"`
	DurationMs             int64      `json:"duration_ms"`
	ExecutorsAdded         int        `json:"executors_added"`
	ExecutorsRemoved       int        `json:"executors_removed"`
	MaxConcurrentExecutors int        `json:"max_concurrent_executors"`
	JobsTotal              int        `json:"jobs_total"`
	JobsFailed             int        `json:"jobs_failed"`
	StagesTotal            int        `json:"stages_total"`
	StagesFailed           int        `json:"stages_failed"`
	StagesSkipped          int        `json:"stages_skipped"`
	TasksTotal             int64      `json:"tasks_total"`
	TasksFailed            int64      `json:"tasks_failed"`
	TotalInputGB           float64    `json:"total_input_gb"`
	TotalOutputGB          float64    `json:"total_output_gb"`
	TotalShuffleReadGB     float64    `json:"total_shuffle_read_gb"`
	TotalShuffleWriteGB    float64    `json:"total_shuffle_write_gb"`
	TotalSpillDiskGB       float64    `json:"total_spill_disk_gb"`
	TotalGCMs              int64      `json:"total_gc_ms"`
	TotalRunMs             int64      `json:"total_run_ms"`
	GCRatio                float64    `json:"gc_ratio"`
	TopStagesByDuration    []TopStage `json:"top_stages_by_duration"`
}

func AppSummaryColumns() []string {
	return []string{
		"app_id", "app_name", "duration_ms", "max_concurrent_executors",
		"jobs_total", "stages_total", "tasks_total", "tasks_failed",
		"total_input_gb", "total_shuffle_read_gb", "total_spill_disk_gb",
		"gc_ratio",
	}
}

func AppSummary(app *model.Application) AppSummaryRow {
	row := AppSummaryRow{
		AppID:                  app.ID,
		AppName:                app.Name,
		User:                   app.User,
		ExecutorsAdded:         app.ExecutorsAdded,
		ExecutorsRemoved:       app.ExecutorsRemoved,
		MaxConcurrentExecutors: app.MaxConcurrentExecutors,
		JobsTotal:              app.JobsTotal,
		JobsFailed:             app.JobsFailed,
		StagesTotal:            len(app.Stages),
		TasksTotal:             app.TasksTotal,
		TasksFailed:            app.TasksFailed,
		TotalInputGB:           bytesToGB(app.TotalInputBytes),
		TotalOutputGB:          bytesToGB(app.TotalOutputBytes),
		TotalShuffleReadGB:     bytesToGB(app.TotalShuffleReadBytes),
		TotalShuffleWriteGB:    bytesToGB(app.TotalShuffleWriteBytes),
		TotalSpillDiskGB:       bytesToGB(app.TotalSpillDisk),
		TotalGCMs:              app.TotalGCMs,
		TotalRunMs:             app.TotalRunMs,
	}
	if app.EndMs > app.StartMs {
		row.DurationMs = app.EndMs - app.StartMs
	}
	if app.TotalRunMs > 0 {
		row.GCRatio = round3(float64(app.TotalGCMs) / float64(app.TotalRunMs))
	}

	type sd struct {
		id   int
		name string
		dur  int64
	}
	var all []sd
	for _, s := range app.Stages {
		switch s.Status {
		case "failed":
			row.StagesFailed++
		case "skipped":
			row.StagesSkipped++
		}
		dur := s.CompleteMs - s.SubmitMs
		if dur < 0 {
			dur = 0
		}
		all = append(all, sd{id: s.ID, name: s.Name, dur: dur})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].dur > all[j].dur })
	n := 3
	if len(all) < n {
		n = len(all)
	}
	for i := 0; i < n; i++ {
		row.TopStagesByDuration = append(row.TopStagesByDuration, TopStage{
			StageID: all[i].id, Name: all[i].name, DurationMs: all[i].dur,
		})
	}
	return row
}

const gb = 1024 * 1024 * 1024

func bytesToGB(b int64) float64 { return round3(float64(b) / float64(gb)) }

func round3(f float64) float64 {
	x := f * 1000
	if x < 0 {
		x -= 0.5
	} else {
		x += 0.5
	}
	return float64(int64(x)) / 1000
}
```

- [ ] **Step 3: Run** — `go test ./internal/scenario/...` → PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/scenario/app_summary.go internal/scenario/app_summary_test.go
git commit -m "feat(scenario): app-summary computation"
```

---

## Task 18 — `slow-stages`

**Goal**: Sort stages by wall time, return top N rows with task percentiles.

- [ ] **Step 1: Failing test** — `internal/scenario/slow_stages_test.go`

```go
package scenario

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestSlowStagesSortsByWallTimeDesc(t *testing.T) {
	app := model.NewApplication()
	for i, wall := range []int64{100, 500, 300} {
		s := model.NewStage(i, 0, "s", 10, 0)
		s.SubmitMs = 0
		s.CompleteMs = wall
		s.Status = "succeeded"
		for d := int64(0); d < 10; d++ {
			s.TaskDurations.Add(float64(wall / 10))
		}
		app.Stages[model.StageKey{ID: i}] = s
	}
	rows := SlowStages(app, 2)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0].StageID != 1 || rows[1].StageID != 2 {
		t.Errorf("order wrong: %+v", rows)
	}
}
```

Run: FAIL.

- [ ] **Step 2: Implement** — `internal/scenario/slow_stages.go`

```go
package scenario

import (
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SlowStageRow struct {
	StageID         int     `json:"stage_id"`
	Attempt         int     `json:"attempt"`
	Name            string  `json:"name"`
	DurationMs      int64   `json:"duration_ms"`
	Tasks           int64   `json:"tasks"`
	FailedTasks     int64   `json:"failed_tasks"`
	P50TaskMs       int64   `json:"p50_task_ms"`
	P99TaskMs       int64   `json:"p99_task_ms"`
	InputGB         float64 `json:"input_gb"`
	ShuffleReadGB   float64 `json:"shuffle_read_gb"`
	ShuffleWriteGB  float64 `json:"shuffle_write_gb"`
	SpillDiskGB     float64 `json:"spill_disk_gb"`
	GCMs            int64   `json:"gc_ms"`
}

func SlowStagesColumns() []string {
	return []string{
		"stage_id", "attempt", "name", "duration_ms", "tasks", "failed_tasks",
		"p50_task_ms", "p99_task_ms", "input_gb", "shuffle_read_gb",
		"shuffle_write_gb", "spill_disk_gb", "gc_ms",
	}
}

func SlowStages(app *model.Application, top int) []SlowStageRow {
	rows := make([]SlowStageRow, 0, len(app.Stages))
	for _, s := range app.Stages {
		dur := s.CompleteMs - s.SubmitMs
		if dur < 0 {
			dur = 0
		}
		rows = append(rows, SlowStageRow{
			StageID:        s.ID,
			Attempt:        s.Attempt,
			Name:           s.Name,
			DurationMs:     dur,
			Tasks:          int64(s.TaskDurations.Count()),
			FailedTasks:    s.FailedTasks,
			P50TaskMs:      int64(s.TaskDurations.Quantile(0.5)),
			P99TaskMs:      int64(s.TaskDurations.Quantile(0.99)),
			InputGB:        bytesToGB(s.TotalInputBytes),
			ShuffleReadGB:  bytesToGB(s.TotalShuffleReadBytes),
			ShuffleWriteGB: bytesToGB(s.TotalShuffleWriteBytes),
			SpillDiskGB:    bytesToGB(s.TotalSpillDisk),
			GCMs:           s.TotalGCMs,
		})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].DurationMs > rows[j].DurationMs })
	if top > 0 && len(rows) > top {
		rows = rows[:top]
	}
	return rows
}
```

- [ ] **Step 3: Run** — `go test ./internal/scenario/...` → PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/scenario/slow_stages.go internal/scenario/slow_stages_test.go
git commit -m "feat(scenario): slow-stages computation"
```

---

## Task 19 — `data-skew`

**Goal**: Filter stages with `tasks>=50 && p99>=1000ms`, compute skew factors, sort by `max(skew_factor, input_skew_factor)` desc.

- [ ] **Step 1: Failing test** — `internal/scenario/data_skew_test.go`

```go
package scenario

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestDataSkewFiltersAndRanks(t *testing.T) {
	app := model.NewApplication()

	skewed := model.NewStage(1, 0, "skewed", 100, 0)
	for i := 0; i < 99; i++ {
		skewed.TaskDurations.Add(100)
		skewed.TaskInputBytes.Add(1024 * 1024)
	}
	skewed.TaskDurations.Add(20000)
	skewed.TaskInputBytes.Add(500 * 1024 * 1024)
	skewed.MaxInputBytes = 500 * 1024 * 1024
	skewed.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = skewed

	tooFew := model.NewStage(2, 0, "small", 10, 0)
	for i := 0; i < 10; i++ {
		tooFew.TaskDurations.Add(5000)
	}
	tooFew.Status = "succeeded"
	app.Stages[model.StageKey{ID: 2}] = tooFew

	short := model.NewStage(3, 0, "fast", 100, 0)
	for i := 0; i < 100; i++ {
		short.TaskDurations.Add(50)
	}
	short.Status = "succeeded"
	app.Stages[model.StageKey{ID: 3}] = short

	rows := DataSkew(app, 10)
	if len(rows) != 1 || rows[0].StageID != 1 {
		t.Fatalf("expected only stage 1, got %+v", rows)
	}
	if rows[0].Verdict != "severe" {
		t.Errorf("verdict=%s want severe", rows[0].Verdict)
	}
}
```

Run: FAIL.

- [ ] **Step 2: Implement** — `internal/scenario/data_skew.go`

```go
package scenario

import (
	"math"
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type DataSkewRow struct {
	StageID          int     `json:"stage_id"`
	Name             string  `json:"name"`
	Tasks            int64   `json:"tasks"`
	P50TaskMs        int64   `json:"p50_task_ms"`
	P99TaskMs        int64   `json:"p99_task_ms"`
	SkewFactor       float64 `json:"skew_factor"`
	MedianInputMB    float64 `json:"median_input_mb"`
	MaxInputMB       float64 `json:"max_input_mb"`
	InputSkewFactor  float64 `json:"input_skew_factor"`
	Verdict          string  `json:"verdict"`
}

func DataSkewColumns() []string {
	return []string{
		"stage_id", "name", "tasks", "p50_task_ms", "p99_task_ms",
		"skew_factor", "median_input_mb", "max_input_mb",
		"input_skew_factor", "verdict",
	}
}

func DataSkew(app *model.Application, top int) []DataSkewRow {
	out := make([]DataSkewRow, 0)
	for _, s := range app.Stages {
		if s.Status != "succeeded" {
			continue
		}
		tasks := int64(s.TaskDurations.Count())
		if tasks < 50 {
			continue
		}
		p99 := s.TaskDurations.Quantile(0.99)
		if p99 < 1000 {
			continue
		}
		median := s.TaskDurations.Quantile(0.5)
		if median < 1 {
			median = 1
		}
		medianBytes := s.TaskInputBytes.Quantile(0.5)
		if medianBytes < 1 {
			medianBytes = 1
		}
		skew := p99 / median
		inputSkew := float64(s.MaxInputBytes) / medianBytes
		f := math.Max(skew, inputSkew)
		v := "mild"
		switch {
		case f >= 10:
			v = "severe"
		case f >= 4:
			v = "warn"
		}
		out = append(out, DataSkewRow{
			StageID:         s.ID,
			Name:            s.Name,
			Tasks:           tasks,
			P50TaskMs:       int64(median),
			P99TaskMs:       int64(p99),
			SkewFactor:      round3(skew),
			MedianInputMB:   bytesToMB(int64(medianBytes)),
			MaxInputMB:      bytesToMB(s.MaxInputBytes),
			InputSkewFactor: round3(inputSkew),
			Verdict:         v,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		fi := math.Max(out[i].SkewFactor, out[i].InputSkewFactor)
		fj := math.Max(out[j].SkewFactor, out[j].InputSkewFactor)
		return fi > fj
	})
	if top > 0 && len(out) > top {
		out = out[:top]
	}
	return out
}

const mb = 1024 * 1024

func bytesToMB(b int64) float64 { return round3(float64(b) / float64(mb)) }
```

- [ ] **Step 3: Run** — `go test ./internal/scenario/...` → PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/scenario/data_skew.go internal/scenario/data_skew_test.go
git commit -m "feat(scenario): data-skew computation"
```

---

## Task 20 — `gc-pressure` (double-segment envelope)

**Goal**: Build `{by_stage, by_executor}` segments. Emit columns as `{by_stage:[...], by_executor:[...]}`.

- [ ] **Step 1: Failing test** — `internal/scenario/gc_pressure_test.go`

```go
package scenario

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestGCPressureFiltersAndOrders(t *testing.T) {
	app := model.NewApplication()

	hot := model.NewStage(1, 0, "hot", 100, 0)
	hot.TotalRunMs = 60000
	hot.TotalGCMs = 24000
	app.Stages[model.StageKey{ID: 1}] = hot

	cold := model.NewStage(2, 0, "cold", 100, 0)
	cold.TotalRunMs = 1000
	cold.TotalGCMs = 500
	app.Stages[model.StageKey{ID: 2}] = cold

	app.Executors["12"] = &model.Executor{ID: "12", Host: "h12", TotalRunMs: 100000, TotalGCMs: 30000, TaskCount: 100}
	app.Executors["13"] = &model.Executor{ID: "13", Host: "h13", TotalRunMs: 1000, TotalGCMs: 100, TaskCount: 1}

	res := GCPressure(app, 10)
	if len(res.ByStage) != 1 || res.ByStage[0].StageID != 1 {
		t.Errorf("by_stage=%+v", res.ByStage)
	}
	if len(res.ByExecutor) != 1 || res.ByExecutor[0].ExecutorID != "12" {
		t.Errorf("by_executor=%+v", res.ByExecutor)
	}
	if res.ByExecutor[0].Verdict != "severe" {
		t.Errorf("verdict=%s want severe", res.ByExecutor[0].Verdict)
	}
}
```

Run: FAIL.

- [ ] **Step 2: Implement** — `internal/scenario/gc_pressure.go`

```go
package scenario

import (
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type GCStageRow struct {
	StageID    int     `json:"stage_id"`
	Name       string  `json:"name"`
	GCRatio    float64 `json:"gc_ratio"`
	TotalGCMs  int64   `json:"total_gc_ms"`
	TotalRunMs int64   `json:"total_run_ms"`
	Verdict    string  `json:"verdict"`
}

type GCExecRow struct {
	ExecutorID string  `json:"executor_id"`
	Host       string  `json:"host"`
	GCRatio    float64 `json:"gc_ratio"`
	TotalGCMs  int64   `json:"total_gc_ms"`
	TotalRunMs int64   `json:"total_run_ms"`
	Tasks      int64   `json:"tasks"`
	Verdict    string  `json:"verdict"`
}

type GCPressureResult struct {
	ByStage    []GCStageRow `json:"by_stage"`
	ByExecutor []GCExecRow  `json:"by_executor"`
}

func GCPressureColumns() map[string][]string {
	return map[string][]string{
		"by_stage":    {"stage_id", "name", "gc_ratio", "total_gc_ms", "total_run_ms", "verdict"},
		"by_executor": {"executor_id", "host", "gc_ratio", "total_gc_ms", "total_run_ms", "tasks", "verdict"},
	}
}

func gcVerdict(r float64) string {
	switch {
	case r >= 0.20:
		return "severe"
	case r >= 0.10:
		return "warn"
	default:
		return "ok"
	}
}

func GCPressure(app *model.Application, top int) GCPressureResult {
	res := GCPressureResult{ByStage: []GCStageRow{}, ByExecutor: []GCExecRow{}}
	for _, s := range app.Stages {
		if s.TotalRunMs < 30000 {
			continue
		}
		r := float64(s.TotalGCMs) / float64(s.TotalRunMs)
		res.ByStage = append(res.ByStage, GCStageRow{
			StageID: s.ID, Name: s.Name,
			GCRatio: round3(r), TotalGCMs: s.TotalGCMs, TotalRunMs: s.TotalRunMs,
			Verdict: gcVerdict(r),
		})
	}
	sort.SliceStable(res.ByStage, func(i, j int) bool { return res.ByStage[i].GCRatio > res.ByStage[j].GCRatio })
	if top > 0 && len(res.ByStage) > top {
		res.ByStage = res.ByStage[:top]
	}

	for _, e := range app.Executors {
		if e.TotalRunMs < 60000 {
			continue
		}
		r := float64(e.TotalGCMs) / float64(e.TotalRunMs)
		res.ByExecutor = append(res.ByExecutor, GCExecRow{
			ExecutorID: e.ID, Host: e.Host,
			GCRatio: round3(r), TotalGCMs: e.TotalGCMs, TotalRunMs: e.TotalRunMs,
			Tasks:   e.TaskCount,
			Verdict: gcVerdict(r),
		})
	}
	sort.SliceStable(res.ByExecutor, func(i, j int) bool { return res.ByExecutor[i].GCRatio > res.ByExecutor[j].GCRatio })
	if top > 0 && len(res.ByExecutor) > top {
		res.ByExecutor = res.ByExecutor[:top]
	}
	return res
}
```

- [ ] **Step 3: Run** — `go test ./internal/scenario/...` → PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/scenario/gc_pressure.go internal/scenario/gc_pressure_test.go
git commit -m "feat(scenario): gc-pressure double-segment computation"
```

---

## Task 21 — `diagnose` rule engine + 5 rules

**Goal**: Define `Rule` interface, implement 5 rules, build `diagnose` scenario that runs all rules and emits OK rows for non-triggers.

- [ ] **Step 1: Failing test** — `internal/rules/rule_test.go`

```go
package rules

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestSkewRuleTriggers(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "x", 100, 0)
	for i := 0; i < 99; i++ {
		s.TaskDurations.Add(100)
	}
	s.TaskDurations.Add(20000)
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = s

	f := SkewRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical", f.Severity)
	}
	if f.Evidence == nil {
		t.Errorf("evidence missing")
	}
}

func TestFailedTasksRuleQuiet(t *testing.T) {
	app := model.NewApplication()
	app.TasksTotal = 10000
	app.TasksFailed = 1
	f := FailedTasksRule{}.Eval(app)
	if f.Severity != "ok" {
		t.Errorf("severity=%s want ok", f.Severity)
	}
}
```

Run: FAIL.

- [ ] **Step 2: Implement rule interface** — `internal/rules/rule.go`

```go
package rules

import "github.com/opay-bigdata/spark-cli/internal/model"

type Finding struct {
	RuleID     string         `json:"rule_id"`
	Severity   string         `json:"severity"`
	Title      string         `json:"title"`
	Evidence   map[string]any `json:"evidence"`
	Suggestion string         `json:"suggestion"`
}

type Rule interface {
	ID() string
	Title() string
	Eval(app *model.Application) Finding
}

func All() []Rule {
	return []Rule{
		SkewRule{}, GCRule{}, SpillRule{}, FailedTasksRule{}, TinyTasksRule{},
	}
}

func okFinding(id, title string) Finding {
	return Finding{RuleID: id, Severity: "ok", Title: title}
}
```

- [ ] **Step 3: Implement 5 rules**

`internal/rules/skew_rule.go`:
```go
package rules

import (
	"fmt"
	"math"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SkewRule struct{}

func (SkewRule) ID() string    { return "data_skew" }
func (SkewRule) Title() string { return "Data skew detected" }

func (SkewRule) Eval(app *model.Application) Finding {
	var bestF float64
	var bestStage *model.Stage
	var bestP50, bestP99 float64
	for _, s := range app.Stages {
		if s.Status != "succeeded" || int64(s.TaskDurations.Count()) < 50 {
			continue
		}
		p99 := s.TaskDurations.Quantile(0.99)
		if p99 < 1000 {
			continue
		}
		median := s.TaskDurations.Quantile(0.5)
		if median < 1 {
			median = 1
		}
		medianB := s.TaskInputBytes.Quantile(0.5)
		if medianB < 1 {
			medianB = 1
		}
		f := math.Max(p99/median, float64(s.MaxInputBytes)/medianB)
		if f > bestF {
			bestF = f
			bestStage = s
			bestP50 = median
			bestP99 = p99
		}
	}
	if bestStage == nil || bestF < 4 {
		return okFinding(SkewRule{}.ID(), SkewRule{}.Title())
	}
	sev := "warn"
	if bestF >= 10 {
		sev = "critical"
	}
	return Finding{
		RuleID:   SkewRule{}.ID(),
		Severity: sev,
		Title:    SkewRule{}.Title(),
		Evidence: map[string]any{
			"stage_id":    bestStage.ID,
			"skew_factor": round3(bestF),
			"p50_task_ms": int64(bestP50),
			"p99_task_ms": int64(bestP99),
		},
		Suggestion: fmt.Sprintf("stage %d 任务长尾严重，median %dms / P99 %dms。检查 join key 分布或开启 AQE skew join。",
			bestStage.ID, int64(bestP50), int64(bestP99)),
	}
}

func round3(f float64) float64 {
	x := f * 1000
	if x < 0 {
		x -= 0.5
	} else {
		x += 0.5
	}
	return float64(int64(x)) / 1000
}
```

`internal/rules/gc_rule.go`:
```go
package rules

import (
	"fmt"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type GCRule struct{}

func (GCRule) ID() string    { return "gc_pressure" }
func (GCRule) Title() string { return "GC pressure" }

func (GCRule) Eval(app *model.Application) Finding {
	var bestRatio float64
	var bestExec *model.Executor
	for _, e := range app.Executors {
		if e.TotalRunMs < 60000 {
			continue
		}
		r := float64(e.TotalGCMs) / float64(e.TotalRunMs)
		if r > bestRatio {
			bestRatio = r
			bestExec = e
		}
	}
	if bestExec == nil || bestRatio < 0.10 {
		return okFinding(GCRule{}.ID(), GCRule{}.Title())
	}
	sev := "warn"
	if bestRatio >= 0.20 {
		sev = "critical"
	}
	return Finding{
		RuleID:   GCRule{}.ID(),
		Severity: sev,
		Title:    GCRule{}.Title(),
		Evidence: map[string]any{
			"executor_id": bestExec.ID,
			"gc_ratio":    round3(bestRatio),
			"total_gc_ms": bestExec.TotalGCMs,
			"total_run_ms": bestExec.TotalRunMs,
		},
		Suggestion: fmt.Sprintf("executor %s GC 占比 %.1f%%，考虑增大 executor 内存或减小分区。",
			bestExec.ID, bestRatio*100),
	}
}
```

`internal/rules/spill_rule.go`:
```go
package rules

import (
	"fmt"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SpillRule struct{}

func (SpillRule) ID() string    { return "disk_spill" }
func (SpillRule) Title() string { return "Disk spill" }

func (SpillRule) Eval(app *model.Application) Finding {
	var maxSpill int64
	var hot *model.Stage
	for _, s := range app.Stages {
		if s.TotalSpillDisk > maxSpill {
			maxSpill = s.TotalSpillDisk
			hot = s
		}
	}
	gb := float64(maxSpill) / (1024 * 1024 * 1024)
	if hot == nil || gb < 1 {
		return okFinding(SpillRule{}.ID(), SpillRule{}.Title())
	}
	sev := "warn"
	if gb >= 10 {
		sev = "critical"
	}
	return Finding{
		RuleID:   SpillRule{}.ID(),
		Severity: sev,
		Title:    SpillRule{}.Title(),
		Evidence: map[string]any{
			"stage_id":      hot.ID,
			"spill_disk_gb": round3(gb),
		},
		Suggestion: fmt.Sprintf("stage %d 磁盘溢写 %.2f GB，考虑提高 spark.sql.shuffle.partitions 或增大 executor 内存。",
			hot.ID, gb),
	}
}
```

`internal/rules/failed_tasks_rule.go`:
```go
package rules

import (
	"fmt"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type FailedTasksRule struct{}

func (FailedTasksRule) ID() string    { return "failed_tasks" }
func (FailedTasksRule) Title() string { return "Failed tasks" }

func (FailedTasksRule) Eval(app *model.Application) Finding {
	if app.TasksTotal == 0 {
		return okFinding(FailedTasksRule{}.ID(), FailedTasksRule{}.Title())
	}
	ratio := float64(app.TasksFailed) / float64(app.TasksTotal)
	if ratio < 0.005 {
		return okFinding(FailedTasksRule{}.ID(), FailedTasksRule{}.Title())
	}
	sev := "warn"
	if ratio >= 0.05 {
		sev = "critical"
	}
	return Finding{
		RuleID:   FailedTasksRule{}.ID(),
		Severity: sev,
		Title:    FailedTasksRule{}.Title(),
		Evidence: map[string]any{
			"tasks_failed": app.TasksFailed,
			"tasks_total":  app.TasksTotal,
			"failure_rate": round3(ratio),
		},
		Suggestion: fmt.Sprintf("应用有 %.1f%% 任务失败 (%d/%d)，检查 driver 日志中的 task failure reason。",
			ratio*100, app.TasksFailed, app.TasksTotal),
	}
}
```

`internal/rules/tiny_tasks_rule.go`:
```go
package rules

import (
	"fmt"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type TinyTasksRule struct{}

func (TinyTasksRule) ID() string    { return "tiny_tasks" }
func (TinyTasksRule) Title() string { return "Tiny tasks" }

func (TinyTasksRule) Eval(app *model.Application) Finding {
	for _, s := range app.Stages {
		tasks := int64(s.TaskDurations.Count())
		if tasks < 200 {
			continue
		}
		p50 := s.TaskDurations.Quantile(0.5)
		if p50 >= 100 {
			continue
		}
		return Finding{
			RuleID:   TinyTasksRule{}.ID(),
			Severity: "warn",
			Title:    TinyTasksRule{}.Title(),
			Evidence: map[string]any{
				"stage_id":    s.ID,
				"tasks":       tasks,
				"p50_task_ms": int64(p50),
			},
			Suggestion: fmt.Sprintf("stage %d 有 %d 个任务但 P50 仅 %dms，分区过细，考虑 coalesce/repartition。",
				s.ID, tasks, int64(p50)),
		}
	}
	return okFinding(TinyTasksRule{}.ID(), TinyTasksRule{}.Title())
}
```

- [ ] **Step 4: Diagnose scenario** — `internal/scenario/diagnose.go`

```go
package scenario

import (
	"github.com/opay-bigdata/spark-cli/internal/model"
	"github.com/opay-bigdata/spark-cli/internal/rules"
)

func DiagnoseColumns() []string {
	return []string{"rule_id", "severity", "title", "evidence", "suggestion"}
}

func Diagnose(app *model.Application) ([]rules.Finding, DiagnoseSummary) {
	all := rules.All()
	out := make([]rules.Finding, 0, len(all))
	var sum DiagnoseSummary
	for _, r := range all {
		f := r.Eval(app)
		switch f.Severity {
		case "critical":
			sum.Critical++
		case "warn":
			sum.Warn++
		default:
			sum.OK++
		}
		out = append(out, f)
	}
	return out, sum
}
```

`internal/scenario/diagnose_test.go`:
```go
package scenario

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestDiagnoseEmitsOKForNonTriggers(t *testing.T) {
	app := model.NewApplication()
	findings, sum := Diagnose(app)
	if len(findings) != 5 {
		t.Fatalf("findings=%d want 5", len(findings))
	}
	if sum.OK != 5 {
		t.Errorf("ok=%d want 5", sum.OK)
	}
}
```

- [ ] **Step 5: Run** — `go test ./internal/...` → PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/rules internal/scenario/diagnose.go internal/scenario/diagnose_test.go
git commit -m "feat(diagnose): rule engine and 5 MVP rules"
```

---

## Task 22 — JSON output formatter

**Goal**: Encode `Envelope` as a single JSON object on stdout (no trailing newline policy: one trailing `\n`). This is the **default** format and the canonical contract for agents.

- [ ] **Step 1: Failing test** — `internal/output/json_test.go`

```go
package output

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/scenario"
)

func TestJSONWriterEmitsCompactSingleObject(t *testing.T) {
	env := scenario.Envelope{
		Scenario: "app-summary", AppID: "x",
		Columns: []string{"a"}, Data: []any{map[string]any{"a": 1}},
	}
	var buf bytes.Buffer
	if err := WriteJSON(&buf, env); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	if buf.Bytes()[buf.Len()-1] != '\n' {
		t.Errorf("missing trailing newline")
	}
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("not single JSON object: %v", err)
	}
	if m["scenario"] != "app-summary" {
		t.Errorf("scenario field lost")
	}
}
```

Run: FAIL.

- [ ] **Step 2: Implement** — `internal/output/json.go`

```go
package output

import (
	"encoding/json"
	"io"
)

func WriteJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}
```

- [ ] **Step 3: Run** — `go test ./internal/output/...` → PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/output/json.go internal/output/json_test.go
git commit -m "feat(output): JSON formatter"
```

---

## Task 23 — Table output formatter

**Goal**: Render envelope as a simple aligned text table for human eyes. `gc-pressure` renders two tables back-to-back (`by_stage` then `by_executor`).

- [ ] **Step 1: Failing test** — `internal/output/table_test.go`

```go
package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/scenario"
)

func TestTableRendersHeaderAndRows(t *testing.T) {
	env := scenario.Envelope{
		Scenario: "slow-stages",
		Columns:  []string{"stage_id", "duration_ms"},
		Data: []any{
			map[string]any{"stage_id": 7, "duration_ms": 1234},
			map[string]any{"stage_id": 12, "duration_ms": 99},
		},
	}
	var buf bytes.Buffer
	if err := WriteTable(&buf, env); err != nil {
		t.Fatalf("WriteTable: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"stage_id", "duration_ms", "1234", "12"} {
		if !strings.Contains(out, want) {
			t.Errorf("table missing %q\n%s", want, out)
		}
	}
}

func TestTableRendersGCDoubleSegment(t *testing.T) {
	env := scenario.Envelope{
		Scenario: "gc-pressure",
		Columns:  map[string]any{"by_stage": []any{"stage_id", "gc_ratio"}, "by_executor": []any{"executor_id", "gc_ratio"}},
		Data: map[string]any{
			"by_stage":    []any{map[string]any{"stage_id": 7, "gc_ratio": 0.4}},
			"by_executor": []any{map[string]any{"executor_id": "12", "gc_ratio": 0.3}},
		},
	}
	var buf bytes.Buffer
	if err := WriteTable(&buf, env); err != nil {
		t.Fatalf("WriteTable: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "by_stage") || !strings.Contains(out, "by_executor") {
		t.Errorf("missing segment headers:\n%s", out)
	}
}
```

Run: FAIL.

- [ ] **Step 2: Implement** — `internal/output/table.go`

```go
package output

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/scenario"
)

func WriteTable(w io.Writer, env scenario.Envelope) error {
	fmt.Fprintf(w, "# %s  app=%s  events=%d  elapsed=%dms\n",
		env.Scenario, env.AppID, env.ParsedEvents, env.ElapsedMs)
	if env.Scenario == "gc-pressure" {
		return writeGCSegments(w, env)
	}
	cols := toStringSlice(env.Columns)
	rows := toRowSlice(env.Data)
	return renderTable(w, cols, rows)
}

func writeGCSegments(w io.Writer, env scenario.Envelope) error {
	colMap := toMap(env.Columns)
	dataMap := toMap(env.Data)
	for _, seg := range []string{"by_stage", "by_executor"} {
		fmt.Fprintf(w, "\n## %s\n", seg)
		cols := toStringSlice(colMap[seg])
		rows := toRowSlice(dataMap[seg])
		if err := renderTable(w, cols, rows); err != nil {
			return err
		}
	}
	return nil
}

func renderTable(w io.Writer, cols []string, rows []map[string]any) error {
	if len(cols) == 0 {
		return nil
	}
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c)
	}
	cells := make([][]string, len(rows))
	for ri, row := range rows {
		cells[ri] = make([]string, len(cols))
		for ci, c := range cols {
			s := stringify(row[c])
			cells[ri][ci] = s
			if len(s) > widths[ci] {
				widths[ci] = len(s)
			}
		}
	}
	writeRow(w, cols, widths)
	writeSep(w, widths)
	for _, row := range cells {
		writeRow(w, row, widths)
	}
	return nil
}

func writeRow(w io.Writer, cells []string, widths []int) {
	parts := make([]string, len(cells))
	for i, s := range cells {
		parts[i] = padRight(s, widths[i])
	}
	fmt.Fprintln(w, strings.Join(parts, "  "))
}

func writeSep(w io.Writer, widths []int) {
	parts := make([]string, len(widths))
	for i, n := range widths {
		parts[i] = strings.Repeat("-", n)
	}
	fmt.Fprintln(w, strings.Join(parts, "  "))
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}

func stringify(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		return fmt.Sprintf("%.3f", x)
	case map[string]any, []any:
		b, _ := json.Marshal(x)
		return string(b)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func toStringSlice(v any) []string {
	switch x := v.(type) {
	case []string:
		return x
	case []any:
		out := make([]string, len(x))
		for i, e := range x {
			out[i] = fmt.Sprint(e)
		}
		return out
	default:
		return nil
	}
}

func toRowSlice(v any) []map[string]any {
	switch x := v.(type) {
	case []any:
		out := make([]map[string]any, 0, len(x))
		for _, e := range x {
			if m, ok := e.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	case []map[string]any:
		return x
	default:
		return nil
	}
}

func toMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}
```

- [ ] **Step 3: Run** — `go test ./internal/output/...` → PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/output/table.go internal/output/table_test.go
git commit -m "feat(output): table formatter with gc-pressure double segment"
```

---

## Task 24 — Markdown output formatter

**Goal**: Same data as table but pipe-delimited GitHub markdown — for embedding in chat messages or docs.

- [ ] **Step 1: Failing test** — `internal/output/markdown_test.go`

```go
package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/scenario"
)

func TestMarkdownProducesPipedTable(t *testing.T) {
	env := scenario.Envelope{
		Scenario: "slow-stages",
		Columns:  []string{"stage_id", "duration_ms"},
		Data: []any{
			map[string]any{"stage_id": 7, "duration_ms": 1234},
		},
	}
	var buf bytes.Buffer
	if err := WriteMarkdown(&buf, env); err != nil {
		t.Fatalf("WriteMarkdown: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"| stage_id |", "| --- |", "| 7 |"} {
		if !strings.Contains(out, want) {
			t.Errorf("markdown missing %q\n%s", want, out)
		}
	}
}
```

Run: FAIL.

- [ ] **Step 2: Implement** — `internal/output/markdown.go`

```go
package output

import (
	"fmt"
	"io"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/scenario"
)

func WriteMarkdown(w io.Writer, env scenario.Envelope) error {
	fmt.Fprintf(w, "## %s — `%s`\n\n", env.Scenario, env.AppID)
	fmt.Fprintf(w, "_log: `%s` · format: %s · compression: %s · events: %d · elapsed: %dms_\n\n",
		env.LogPath, env.LogFormat, env.Compression, env.ParsedEvents, env.ElapsedMs)
	if env.Scenario == "gc-pressure" {
		colMap := toMap(env.Columns)
		dataMap := toMap(env.Data)
		for _, seg := range []string{"by_stage", "by_executor"} {
			fmt.Fprintf(w, "### %s\n\n", seg)
			renderMD(w, toStringSlice(colMap[seg]), toRowSlice(dataMap[seg]))
			fmt.Fprintln(w)
		}
		return nil
	}
	renderMD(w, toStringSlice(env.Columns), toRowSlice(env.Data))
	return nil
}

func renderMD(w io.Writer, cols []string, rows []map[string]any) {
	if len(cols) == 0 {
		return
	}
	fmt.Fprintln(w, "| "+strings.Join(cols, " | ")+" |")
	seps := make([]string, len(cols))
	for i := range cols {
		seps[i] = "---"
	}
	fmt.Fprintln(w, "| "+strings.Join(seps, " | ")+" |")
	for _, row := range rows {
		cells := make([]string, len(cols))
		for i, c := range cols {
			cells[i] = stringify(row[c])
		}
		fmt.Fprintln(w, "| "+strings.Join(cells, " | ")+" |")
	}
}
```

- [ ] **Step 3: Run** — `go test ./internal/output/...` → PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/output/markdown.go internal/output/markdown_test.go
git commit -m "feat(output): markdown formatter"
```

---

## Task 25 — Command pipeline & cobra wiring

**Goal**: A single `runner.Run` shared by all 5 cobra commands. Pipeline:

1. Resolve config → `eventlog.LogSource`
2. If `--dry-run`, build envelope with the LogSource and skip parsing
3. Open + decompress + decode events → `*model.Application`
4. Hand off to scenario-specific function for `data` + `columns`
5. Render via formatter

- [ ] **Step 1: Failing test** — `cmd/scenarios/runner_test.go`

```go
package scenarios

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunnerDryRunEmitsLogSourceWithoutParsing(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "application_1_1")
	if err := os.WriteFile(logPath, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	rc := Run(context.Background(), Options{
		Scenario:    "app-summary",
		AppID:       "application_1_1",
		LogDirs:     []string{"file://" + dir},
		Format:      "json",
		DryRun:      true,
		Stdout:      &stdout,
		Stderr:      &stderr,
	})
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"log_path"`) {
		t.Errorf("stdout missing log_path:\n%s", stdout.String())
	}
	if strings.Contains(stdout.String(), `"parsed_events":0,"elapsed_ms":0`) {
		// dry-run should still set ParsedEvents=0 but it must be present
	}
}
```

Run: FAIL.

- [ ] **Step 2: Implement** — `cmd/scenarios/runner.go`

```go
package scenarios

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/opay-bigdata/spark-cli/internal/config"
	cliErrors "github.com/opay-bigdata/spark-cli/internal/errors"
	"github.com/opay-bigdata/spark-cli/internal/eventlog"
	"github.com/opay-bigdata/spark-cli/internal/fs"
	"github.com/opay-bigdata/spark-cli/internal/model"
	"github.com/opay-bigdata/spark-cli/internal/output"
	"github.com/opay-bigdata/spark-cli/internal/scenario"
)

type Options struct {
	Scenario string
	AppID    string
	LogDirs  []string
	HDFSUser string
	Timeout  time.Duration
	Format   string // json | table | markdown
	Top      int
	DryRun   bool
	Stdout   io.Writer
	Stderr   io.Writer
}

func Run(ctx context.Context, opts Options) int {
	cfg, err := buildConfig(opts)
	if err != nil {
		return writeErr(opts.Stderr, err)
	}
	fsys, err := fs.New(cfg)
	if err != nil {
		return writeErr(opts.Stderr, err)
	}
	defer fsys.Close()

	loc := eventlog.NewLocator(fsys, cfg.LogDirs)
	src, err := loc.Resolve(ctx, opts.AppID)
	if err != nil {
		return writeErr(opts.Stderr, err)
	}

	env := scenario.Envelope{
		Scenario:    opts.Scenario,
		AppID:       src.AppID,
		LogPath:     src.LogPath(),
		LogFormat:   src.Format,
		Compression: string(src.Compression),
		Incomplete:  src.InProgress,
	}

	if opts.DryRun {
		env.Columns = []string{"app_id", "log_path", "log_format", "compression", "incomplete", "size_mb"}
		env.Data = []any{map[string]any{
			"app_id":      src.AppID,
			"log_path":    src.LogPath(),
			"log_format":  src.Format,
			"compression": string(src.Compression),
			"incomplete":  src.InProgress,
			"size_mb":     src.SizeMB(),
		}}
		return render(opts, env)
	}

	start := time.Now()
	app, parsed, err := parseApp(ctx, fsys, src)
	if err != nil {
		return writeErr(opts.Stderr, err)
	}
	env.AppName = app.Name
	env.ParsedEvents = parsed
	env.ElapsedMs = time.Since(start).Milliseconds()

	if err := buildScenarioBody(opts, app, &env); err != nil {
		return writeErr(opts.Stderr, err)
	}
	return render(opts, env)
}

func parseApp(ctx context.Context, fsys fs.FS, src eventlog.LogSource) (*model.Application, int64, error) {
	r, err := eventlog.Open(ctx, fsys, src)
	if err != nil {
		return nil, 0, err
	}
	defer r.Close()
	app := model.NewApplication()
	app.ID = src.AppID
	agg := model.NewAggregator(app)
	count, err := eventlog.Decode(r, agg)
	if err != nil {
		return nil, count, err
	}
	return app, count, nil
}

func render(opts Options, env scenario.Envelope) int {
	var err error
	switch opts.Format {
	case "", "json":
		err = output.WriteJSON(opts.Stdout, env)
	case "table":
		err = output.WriteTable(opts.Stdout, env)
	case "markdown":
		err = output.WriteMarkdown(opts.Stdout, env)
	default:
		err = cliErrors.New("FLAG_INVALID", fmt.Sprintf("unknown --format %q", opts.Format), "use json|table|markdown")
	}
	if err != nil {
		return writeErr(opts.Stderr, err)
	}
	return 0
}

func writeErr(w io.Writer, err error) int {
	cliErr := cliErrors.As(err)
	output.WriteJSON(w, map[string]any{"error": cliErr})
	return cliErr.ExitCode()
}

func buildConfig(opts Options) (*config.Config, error) {
	cfg, err := config.Load("")
	if err != nil {
		return nil, err
	}
	cfg.ApplyEnv()
	cfg.ApplyFlags(config.FlagOverrides{
		LogDirs:  opts.LogDirs,
		HDFSUser: opts.HDFSUser,
		Timeout:  opts.Timeout,
	})
	return cfg, cfg.Validate()
}
```

- [ ] **Step 3: Scenario dispatcher** — `cmd/scenarios/dispatch.go`

```go
package scenarios

import (
	cliErrors "github.com/opay-bigdata/spark-cli/internal/errors"
	"github.com/opay-bigdata/spark-cli/internal/model"
	"github.com/opay-bigdata/spark-cli/internal/scenario"
)

func buildScenarioBody(opts Options, app *model.Application, env *scenario.Envelope) error {
	switch opts.Scenario {
	case "app-summary":
		env.Columns = scenario.AppSummaryColumns()
		env.Data = []any{scenario.AppSummary(app)}
	case "slow-stages":
		env.Columns = scenario.SlowStagesColumns()
		rows := scenario.SlowStages(app, opts.Top)
		anyRows := make([]any, len(rows))
		for i, r := range rows {
			anyRows[i] = r
		}
		env.Data = anyRows
	case "data-skew":
		env.Columns = scenario.DataSkewColumns()
		rows := scenario.DataSkew(app, opts.Top)
		anyRows := make([]any, len(rows))
		for i, r := range rows {
			anyRows[i] = r
		}
		env.Data = anyRows
	case "gc-pressure":
		env.Columns = scenario.GCPressureColumns()
		env.Data = scenario.GCPressure(app, opts.Top)
	case "diagnose":
		env.Columns = scenario.DiagnoseColumns()
		findings, sum := scenario.Diagnose(app)
		anyRows := make([]any, len(findings))
		for i, f := range findings {
			anyRows[i] = f
		}
		env.Data = anyRows
		env.Summary = sum
	default:
		return cliErrors.New("INTERNAL", "unknown scenario "+opts.Scenario, "")
	}
	return nil
}
```

- [ ] **Step 4: Cobra command files**

`cmd/scenarios/register.go`:
```go
package scenarios

import (
	"github.com/spf13/cobra"
)

func Register(root *cobra.Command) {
	root.AddCommand(newAppSummaryCmd())
	root.AddCommand(newSlowStagesCmd())
	root.AddCommand(newDataSkewCmd())
	root.AddCommand(newGCPressureCmd())
	root.AddCommand(newDiagnoseCmd())
}
```

`cmd/scenarios/app_summary.go`:
```go
package scenarios

import (
	"github.com/opay-bigdata/spark-cli/cmd"
	"github.com/spf13/cobra"
)

func newAppSummaryCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "app-summary <appId>",
		Short: "Application-level overview",
		Args:  cobra.ExactArgs(1),
		RunE: func(cc *cobra.Command, args []string) error {
			rc := Run(cc.Context(), buildOpts("app-summary", args[0], cc))
			if rc != 0 {
				cmd.SetExitCode(rc)
			}
			return nil
		},
	}
	return c
}
```

`cmd/scenarios/slow_stages.go`:
```go
package scenarios

import (
	"github.com/opay-bigdata/spark-cli/cmd"
	"github.com/spf13/cobra"
)

func newSlowStagesCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "slow-stages <appId>",
		Short: "Stages ranked by wall time",
		Args:  cobra.ExactArgs(1),
		RunE: func(cc *cobra.Command, args []string) error {
			rc := Run(cc.Context(), buildOpts("slow-stages", args[0], cc))
			if rc != 0 {
				cmd.SetExitCode(rc)
			}
			return nil
		},
	}
	return c
}
```

`cmd/scenarios/data_skew.go`, `cmd/scenarios/gc_pressure.go`, `cmd/scenarios/diagnose.go`: same shape, only the scenario string and `Use`/`Short` differ.

`cmd/scenarios/options.go`:
```go
package scenarios

import (
	"github.com/opay-bigdata/spark-cli/cmd"
	"github.com/spf13/cobra"
)

func buildOpts(scenario, appID string, cc *cobra.Command) Options {
	g := cmd.GlobalFlags()
	return Options{
		Scenario: scenario,
		AppID:    appID,
		LogDirs:  g.LogDirs,
		HDFSUser: g.HDFSUser,
		Timeout:  g.Timeout,
		Format:   g.Format,
		Top:      g.Top,
		DryRun:   g.DryRun,
		Stdout:   cc.OutOrStdout(),
		Stderr:   cc.ErrOrStderr(),
	}
}
```

Update `cmd/root.go` to (a) expose `GlobalFlags()` returning the shared struct, (b) expose `SetExitCode(int)` and `ExitCode()` (read in `main.go`), (c) call `scenarios.Register(rootCmd)` in `init()`. Update `main.go`:

```go
package main

import (
	"context"
	"os"

	"github.com/opay-bigdata/spark-cli/cmd"
)

func main() {
	if err := cmd.Execute(context.Background()); err != nil {
		os.Exit(1)
	}
	os.Exit(cmd.ExitCode())
}
```

- [ ] **Step 5: Run** — `go build ./... && go test ./...` → PASS.

- [ ] **Step 6: Smoke test** — Generate the synthetic log on disk and exercise both dry-run and full parse:

```bash
mkdir -p /tmp/spark-cli-smoke
cp tests/testdata/tiny_app.json /tmp/spark-cli-smoke/application_1_1
go run . app-summary application_1_1 --log-dirs file:///tmp/spark-cli-smoke
go run . diagnose application_1_1 --log-dirs file:///tmp/spark-cli-smoke --dry-run
```

Expected: both emit valid JSON envelopes; first has `"scenario":"app-summary"`, second has `"data":[{"app_id":"application_1_1","log_path":...}]`.

- [ ] **Step 7: Commit**

```bash
git add cmd
git commit -m "feat(cmd): scenarios pipeline and 5 cobra commands"
```

---

## Task 26 — End-to-end golden tests

**Goal**: Drive the whole pipeline via the cobra root command on the synthetic EventLog and snapshot the JSON envelope. Catches contract drift.

- [ ] **Step 1: Test scaffolding** — `tests/e2e/e2e_test.go`

```go
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/opay-bigdata/spark-cli/cmd"
)

func TestE2E_AllScenarios_TinyApp(t *testing.T) {
	dir := t.TempDir()
	src, err := os.ReadFile(filepath.Join("..", "testdata", "tiny_app.json"))
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "application_1_1")
	if err := os.WriteFile(logPath, src, 0o644); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		args     []string
		wantKeys []string
	}{
		{"app-summary", []string{"app-summary", "application_1_1"}, []string{"scenario", "app_id", "data"}},
		{"slow-stages", []string{"slow-stages", "application_1_1", "--top", "5"}, []string{"scenario", "data"}},
		{"data-skew", []string{"data-skew", "application_1_1"}, []string{"scenario", "data"}},
		{"gc-pressure", []string{"gc-pressure", "application_1_1"}, []string{"scenario", "data"}},
		{"diagnose", []string{"diagnose", "application_1_1"}, []string{"scenario", "data", "summary"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd.ResetForTest()
			var stdout, stderr bytes.Buffer
			rc := cmd.RunWith(context.Background(), append(tc.args, "--log-dirs", "file://"+dir, "--format", "json"), &stdout, &stderr)
			if rc != 0 {
				t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
			}
			var m map[string]any
			if err := json.Unmarshal(stdout.Bytes(), &m); err != nil {
				t.Fatalf("not json: %v\n%s", err, stdout.String())
			}
			for _, k := range tc.wantKeys {
				if _, ok := m[k]; !ok {
					t.Errorf("missing key %q in stdout:\n%s", k, stdout.String())
				}
			}
		})
	}
}

func TestE2E_AppNotFound(t *testing.T) {
	dir := t.TempDir()
	cmd.ResetForTest()
	var stdout, stderr bytes.Buffer
	rc := cmd.RunWith(context.Background(), []string{"app-summary", "application_does_not_exist", "--log-dirs", "file://" + dir}, &stdout, &stderr)
	if rc != 2 {
		t.Errorf("rc=%d want 2", rc)
	}
	if !bytes.Contains(stderr.Bytes(), []byte(`"APP_NOT_FOUND"`)) {
		t.Errorf("stderr missing APP_NOT_FOUND:\n%s", stderr.String())
	}
}
```

- [ ] **Step 2: Add `cmd.RunWith` and `cmd.ResetForTest`** in `cmd/root.go`:

```go
func RunWith(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	exitCode = 0
	rootCmd.SetArgs(args)
	rootCmd.SetOut(stdout)
	rootCmd.SetErr(stderr)
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		return 1
	}
	return exitCode
}

func ResetForTest() {
	exitCode = 0
	globalFlags = GlobalFlagsState{}
}
```

- [ ] **Step 3: Run** — `go test ./tests/e2e/...` → PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e cmd/root.go
git commit -m "test(e2e): cover all scenarios on tiny_app fixture"
```

---

## Task 27 — `.claude/skills/spark/SKILL.md`

**Goal**: Ship the agent skill in-tree. This is the file Claude Code loads when the user asks about Spark performance — the skill teaches the agent the diagnose-first workflow, the envelope shape, and the canonical drill-down sequence. Content is canonical from spec Appendix A.

- [ ] **Step 1: Create the skill** — `.claude/skills/spark/SKILL.md`

```markdown
---
name: spark-performance-diagnostics
description: Use when investigating Spark application performance, slow stages, GC pressure, data skew, or task failures. Run via the spark-cli binary on local or HDFS EventLog directories.
---

# Spark Performance Diagnostics

You have access to `spark-cli`, a single-binary CLI that parses Spark EventLogs and emits JSON envelopes. Always start with `diagnose` — it runs all rules at once.

## Required input

The user must provide a Spark `applicationId` (e.g. `application_1735000000_0001`). Accept short forms (`1735000000_0001`) — the CLI normalizes them.

## Workflow

1. **Always run diagnose first**:
   ```
   spark-cli diagnose <appId>
   ```
   Read `summary.critical` and `summary.warn`. Even `severity: "ok"` rows are meaningful — they confirm a check ran.

2. **Drill down based on findings**:
   - `data_skew` critical → `spark-cli data-skew <appId> --top 10`
   - `gc_pressure` critical → `spark-cli gc-pressure <appId>` (look at `by_executor`)
   - `disk_spill` triggered → `spark-cli slow-stages <appId>` and read `spill_disk_gb`
   - `failed_tasks` triggered → ask the user for driver logs; spark-cli does not parse them
   - All `ok` but user reports slowness → `spark-cli slow-stages <appId> --top 5`

3. **For overview**: `spark-cli app-summary <appId>`

## Envelope contract

Every command emits one JSON object on stdout:

```json
{
  "scenario": "...",
  "app_id": "...",
  "log_path": "...",
  "log_format": "v1|v2",
  "compression": "none|zstd|lz4|snappy",
  "incomplete": false,
  "parsed_events": 482113,
  "elapsed_ms": 1842,
  "columns": [...],
  "data": [...]
}
```

Exceptions:
- `gc-pressure` returns `data: {by_stage: [...], by_executor: [...]}` (the only object-shaped data field)
- `diagnose` adds `summary: {critical, warn, ok}`

`incomplete: true` means an `.inprogress` log was read — treat data as preliminary.

## Errors

Errors go to **stderr** as `{"error": {"code": "...", "message": "...", "hint": "..."}}`. Exit codes:
- `0` success
- `1` internal error (file a bug)
- `2` user error (bad flag, app not found, ambiguous)
- `3` IO/HDFS unreachable

`diagnose` returns `0` even when findings are critical — read `summary.critical` to decide.

## Useful flags

- `--log-dirs <uri,uri>` — comma-separated `file://` and/or `hdfs://` URIs to search
- `--format json|table|markdown` — default `json`; use `markdown` when embedding in chat
- `--top N` — for `slow-stages` / `data-skew` / `gc-pressure`
- `--dry-run` — locate the log without parsing (fast sanity check)

## Setup if missing

If `spark-cli` is not installed, fetch the appropriate release:

```
curl -fsSL https://raw.githubusercontent.com/opay-bigdata/spark-cli/main/scripts/install.sh | bash
```

Or build from source: `go install github.com/opay-bigdata/spark-cli@latest`.

If config is missing, run `spark-cli config init` to write `~/.config/spark-cli/config.yaml` with default `log-dirs` placeholders.

## Don't

- Don't parse the JSON manually — use the documented field names from `columns` to know what's in `data`.
- Don't compare `gc_ratio` across runs without checking `total_run_ms` — short stages have noisy GC ratios.
- Don't claim a problem isn't present without running `diagnose` first.
```

- [ ] **Step 2: Commit**

```bash
git add .claude/skills/spark/SKILL.md
git commit -m "docs(skill): bundle spark-performance-diagnostics skill for agents"
```

---

## Task 28 — README, CHANGELOG, LICENSE

**Goal**: Mirror `hbase-metrics-cli` style — bilingual README/CHANGELOG, MIT LICENSE, agent-first framing.

- [ ] **Step 1: `README.md`** (English, agent-first)

```markdown
# spark-cli

A single-binary CLI for diagnosing Apache Spark application performance from EventLogs. Designed for AI agents (Claude Code) and humans alike — every command emits a structured JSON envelope on stdout.

## Quick start

```bash
spark-cli diagnose application_1735000000_0001
```

If `diagnose` flags `data_skew`:

```bash
spark-cli data-skew application_1735000000_0001 --top 10
```

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/opay-bigdata/spark-cli/main/scripts/install.sh | bash
```

Or:

```bash
go install github.com/opay-bigdata/spark-cli@latest
```

## Configure

```bash
spark-cli config init       # writes ~/.config/spark-cli/config.yaml
$EDITOR ~/.config/spark-cli/config.yaml
```

```yaml
log-dirs:
  - file:///var/log/spark-history
  - hdfs://nn:8020/spark-history
hdfs:
  user: hadoop
timeout: 30s
```

Override per-invocation via `--log-dirs`, env var `SPARK_CLI_LOG_DIRS`.

## Commands

| Command | Purpose |
|---|---|
| `spark-cli diagnose <appId>` | Run all rules; agent's first stop |
| `spark-cli app-summary <appId>` | Application-level overview |
| `spark-cli slow-stages <appId>` | Stages by wall time |
| `spark-cli data-skew <appId>` | Skewed stages |
| `spark-cli gc-pressure <appId>` | GC ratio per stage / executor |

All accept `--top N`, `--format json|table|markdown`, `--dry-run`, `--log-dirs`.

## For AI agents

The repo ships `.claude/skills/spark/SKILL.md`. Claude Code auto-loads it when present. The skill teaches the diagnose-first workflow.

## Output contract

```json
{
  "scenario": "data-skew",
  "app_id": "...",
  "log_path": "hdfs://...",
  "log_format": "v1",
  "compression": "zstd",
  "incomplete": false,
  "parsed_events": 482113,
  "elapsed_ms": 1842,
  "columns": [...],
  "data": [...]
}
```

Errors → stderr as `{"error":{"code":..., "message":..., "hint":...}}`. Exit codes: `0` success · `1` internal · `2` user · `3` IO.

## Supported EventLog formats

- V1 single file (`application_<id>` with optional `.inprogress`, `.zstd`, `.lz4`, `.snappy`)
- V2 rolling directory (`eventlog_v2_<id>/events_<n>_<id>`)

Compression: `zstd` and uncompressed are first-class; `lz4` and `snappy` are experimental (Hadoop block framing — open an issue if your logs fail to parse).

## License

MIT
```

- [ ] **Step 2: `README.zh.md`** — Chinese mirror (translate above content; the Chinese README emphasizes agent-first usage and the canonical drill-down sequence).

- [ ] **Step 3: `CHANGELOG.md`** — start with `## v0.1.0 — 2026-04-29` listing the 5 scenarios and bundled skill.

- [ ] **Step 4: `CHANGELOG.zh.md`** — Chinese mirror.

- [ ] **Step 5: `LICENSE`** — MIT, copyright `2026 OPay Big Data`.

- [ ] **Step 6: Commit**

```bash
git add README.md README.zh.md CHANGELOG.md CHANGELOG.zh.md LICENSE
git commit -m "docs: README, CHANGELOG, LICENSE"
```

---

## Task 29 — CI / release / lint

**Goal**: Mirror `hbase-metrics-cli`'s GitHub Actions and goreleaser config. CI runs lint + test on push/PR; release builds multi-platform binaries on tag.

- [ ] **Step 1: `.github/workflows/ci.yml`**

```yaml
name: CI
on:
  push:
    branches: [main]
  pull_request:

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.22' }
      - uses: golangci/golangci-lint-action@v6
        with: { version: v2.1.6 }

  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        go: ['1.22']
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: ${{ matrix.go }} }
      - run: go test -race -count=1 ./...

  build:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.22' }
      - run: go build -o spark-cli .
      - run: ./spark-cli version
```

- [ ] **Step 2: `.github/workflows/release.yml`**

```yaml
name: Release
on:
  push:
    tags: ['v*']

permissions:
  contents: write

jobs:
  goreleaser:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }
      - uses: actions/setup-go@v5
        with: { go-version: '1.22' }
      - uses: goreleaser/goreleaser-action@v6
        with:
          version: latest
          args: release --clean
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

- [ ] **Step 3: `.goreleaser.yml`**

```yaml
version: 2
project_name: spark-cli
before:
  hooks:
    - go mod tidy
builds:
  - id: spark-cli
    main: .
    binary: spark-cli
    env: [CGO_ENABLED=0]
    goos: [linux, darwin]
    goarch: [amd64, arm64]
    ldflags:
      - -s -w
      - -X github.com/opay-bigdata/spark-cli/cmd.version={{.Version}}
      - -X github.com/opay-bigdata/spark-cli/cmd.commit={{.Commit}}
      - -X github.com/opay-bigdata/spark-cli/cmd.date={{.Date}}
archives:
  - format: tar.gz
    name_template: "{{ .ProjectName }}_{{ .Version }}_{{ .Os }}_{{ .Arch }}"
    files:
      - LICENSE
      - README.md
      - CHANGELOG.md
      - .claude/skills/spark/SKILL.md
checksum:
  name_template: "checksums.txt"
changelog:
  use: github
release:
  draft: false
```

- [ ] **Step 4: `.golangci.yml`**

```yaml
version: "2"
run:
  timeout: 5m
linters:
  enable:
    - errcheck
    - govet
    - ineffassign
    - staticcheck
    - unused
    - gofmt
    - goimports
    - revive
    - misspell
issues:
  exclude-dirs:
    - tests/testdata
```

- [ ] **Step 5: Run locally** — `golangci-lint run ./... && goreleaser build --snapshot --clean` should both succeed.

- [ ] **Step 6: Commit**

```bash
git add .github .goreleaser.yml .golangci.yml
git commit -m "ci: lint, test, multi-platform release"
```

---

## Task 30 — `scripts/install.sh`

**Goal**: One-line installer pulling latest goreleaser tarball, like `hbase-metrics-cli`'s. Defaults to `$HOME/.local/bin/spark-cli`.

- [ ] **Step 1: Write** — `scripts/install.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

REPO="opay-bigdata/spark-cli"
BIN_DIR="${SPARK_CLI_BIN_DIR:-$HOME/.local/bin}"

mkdir -p "$BIN_DIR"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) ARCH=amd64 ;;
  arm64|aarch64) ARCH=arm64 ;;
  *) echo "unsupported arch: $ARCH" >&2; exit 1 ;;
esac

VERSION="${SPARK_CLI_VERSION:-$(curl -fsSL https://api.github.com/repos/$REPO/releases/latest | grep -oE '"tag_name":\s*"[^"]+"' | head -1 | sed 's/.*"\([^"]*\)"$/\1/')}"
if [[ -z "$VERSION" ]]; then
  echo "could not resolve latest version; set SPARK_CLI_VERSION=vX.Y.Z" >&2
  exit 1
fi
VERSION="${VERSION#v}"

URL="https://github.com/$REPO/releases/download/v${VERSION}/spark-cli_${VERSION}_${OS}_${ARCH}.tar.gz"
echo "Downloading $URL"
TMP="$(mktemp -d)"
curl -fsSL "$URL" -o "$TMP/spark-cli.tar.gz"
tar -xzf "$TMP/spark-cli.tar.gz" -C "$TMP"
install -m 0755 "$TMP/spark-cli" "$BIN_DIR/spark-cli"

# install agent skill
SKILL_DIR="${SPARK_CLI_SKILL_DIR:-$HOME/.claude/skills/spark}"
mkdir -p "$SKILL_DIR"
install -m 0644 "$TMP/.claude/skills/spark/SKILL.md" "$SKILL_DIR/SKILL.md" || true

rm -rf "$TMP"
echo "Installed: $BIN_DIR/spark-cli"
echo "Skill:     $SKILL_DIR/SKILL.md"
echo
echo "Make sure $BIN_DIR is on your PATH."
```

- [ ] **Step 2: Make executable + smoke test**

```bash
chmod +x scripts/install.sh
SPARK_CLI_VERSION=v0.1.0 SPARK_CLI_BIN_DIR=/tmp/spark-cli-bin scripts/install.sh   # only after first release
```

(Pre-release: skip the smoke test.)

- [ ] **Step 3: Commit**

```bash
git add scripts/install.sh
git commit -m "feat: install.sh one-liner with skill bundling"
```

---

## Task 31 — `docs/examples/` walkthrough

**Goal**: A copy-pasteable agent transcript showing the canonical drill-down. Used both as user-facing docs and as a regression check that the JSON shape matches what the skill claims.

- [ ] **Step 1: `docs/examples/diagnose-walkthrough.md`**

```markdown
# Walkthrough: diagnosing a slow Spark job

Below is a transcript an AI agent would produce after the user reports "etl_daily ran 3× longer than usual yesterday."

## 1. Run diagnose

```bash
$ spark-cli diagnose application_1735000000_0001
```
```json
{
  "scenario": "diagnose",
  "app_id": "application_1735000000_0001",
  "app_name": "etl_daily",
  "log_path": "hdfs://nn:8020/spark-history/application_1735000000_0001.zstd",
  "log_format": "v1",
  "compression": "zstd",
  "incomplete": false,
  "parsed_events": 482113,
  "elapsed_ms": 1842,
  "columns": ["rule_id", "severity", "title", "evidence", "suggestion"],
  "data": [
    {"rule_id": "data_skew", "severity": "critical", "title": "Data skew detected",
     "evidence": {"stage_id": 7, "skew_factor": 23.95, "p50_task_ms": 410, "p99_task_ms": 9820},
     "suggestion": "stage 7 任务长尾严重..."},
    {"rule_id": "gc_pressure", "severity": "warn", "title": "GC pressure",
     "evidence": {"executor_id": "12", "gc_ratio": 0.14, "total_gc_ms": 92000, "total_run_ms": 657000},
     "suggestion": "executor 12 GC 占比 14.0%..."},
    {"rule_id": "disk_spill", "severity": "ok", "title": "Disk spill", "evidence": null, "suggestion": null},
    {"rule_id": "failed_tasks", "severity": "ok", "title": "Failed tasks", "evidence": null, "suggestion": null},
    {"rule_id": "tiny_tasks", "severity": "ok", "title": "Tiny tasks", "evidence": null, "suggestion": null}
  ],
  "summary": {"critical": 1, "warn": 1, "ok": 3}
}
```

Agent reads: `summary.critical=1` from `data_skew` on stage 7. Drill down.

## 2. Drill into data-skew

```bash
$ spark-cli data-skew application_1735000000_0001 --top 5
```
Agent gets the full skewed-stage list with `verdict: "severe"` for stage 7 and surfaces "stage 7 has a 24× long-tail; check join keys or enable AQE skew join."

## 3. Confirm GC isn't masking the issue

```bash
$ spark-cli gc-pressure application_1735000000_0001
```
Agent reads `by_executor` and notes only one executor (12) is hot — confirms it's localized, not cluster-wide.

## 4. Suggest next steps

The agent now has enough to write a remediation plan referencing both the skew (root cause) and GC (secondary symptom on the affected executor). The user's question is answered without ever opening the Spark UI.
```

- [ ] **Step 2: Commit**

```bash
git add docs/examples
git commit -m "docs(examples): canonical diagnose walkthrough"
```

---

## Self-review

Before exiting plan-writing, verify:

1. **Spec coverage** — every numbered section in `2026-04-29-spark-cli-design.md` (§1 commands/envelope, §2 layout, §3 parsing, §4 location/compression, §5 five scenarios, §6 CI/release/install/skill, §7 config, §8 DoD) maps to at least one task above. Map:
   - §1.1 commands → Task 25
   - §1.3 envelope → Task 16, validated by Tasks 22-24, 26
   - §1.4 stderr error → Task 4 (errors), Task 25 (writeErr), Task 26 (E2E error case)
   - §1.5 exit codes → Task 4, Task 25
   - §2 layout → Tasks 1-31 (each file in spec maps to a task)
   - §3 parsing → Tasks 13, 14, 15
   - §4 location/compression → Tasks 11, 12
   - §5.1-5.5 scenarios → Tasks 17, 18, 19, 20, 21
   - §6.1 CI → Task 29
   - §6.2 release → Task 29
   - §6.3 install → Task 30
   - §6.4 skill → Task 27
   - §7 config → Tasks 5, 6
   - §8 DoD → final smoke verification (below)

2. **Type consistency** — `Envelope.Columns` and `Envelope.Data` are both `any` to admit both array (default) and object (gc-pressure) shapes. JSON formatter accepts whatever the scenario layer produces.

3. **Placeholder scan** — search the plan for `TODO`, `XXX`, `FIXME`, `<placeholder>` — none should remain (only `<appId>` template strings inside example commands).

4. **Module path** — every `import` block uses `github.com/opay-bigdata/spark-cli/...`.

## Definition-of-Done verification (after Task 31)

Before declaring the MVP complete:

- [ ] `go test -race ./...` passes on Linux + macOS
- [ ] `golangci-lint run ./...` clean
- [ ] `goreleaser build --snapshot --clean` produces 4 binaries (linux/darwin × amd64/arm64)
- [ ] Manual smoke: `spark-cli diagnose <real-appId> --log-dirs file://...` returns valid JSON in <5s on a 100MB log
- [ ] `.claude/skills/spark/SKILL.md` is shipped in the release tarball (verified via `tar tzf` on the artifact)

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-29-spark-cli-mvp.md`.

Two execution options:

1. **Subagent-Driven Development** (recommended) — dispatch the `superpowers:subagent-driven-development` skill, which fans tasks out to focused subagents (one per file boundary) so each commit lands clean.
2. **Inline Execution** — run `superpowers:executing-plans` directly in this session and walk the tasks top-to-bottom.

Which approach?
