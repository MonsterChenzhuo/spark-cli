package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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
			args := append(append([]string{}, tc.args...), "--log-dirs", "file://"+dir, "--format", "json")
			rc := cmd.RunWith(context.Background(), args, &stdout, &stderr)
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

// markdown / table format 在所有 5 场景都不应崩溃,且 header 应当含
// 应用 wall 时长(round-4 加的 formatAppDuration)。这是 e2e smoke,unit
// test 已经守门具体细节。
func TestE2E_FormatTableAndMarkdownSmoke(t *testing.T) {
	dir := t.TempDir()
	src, err := os.ReadFile(filepath.Join("..", "testdata", "tiny_app.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "application_1_1"), src, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, sc := range []string{"app-summary", "slow-stages", "data-skew", "gc-pressure", "diagnose"} {
		for _, format := range []string{"table", "markdown"} {
			t.Run(sc+"/"+format, func(t *testing.T) {
				cmd.ResetForTest()
				var stdout, stderr bytes.Buffer
				rc := cmd.RunWith(context.Background(),
					[]string{sc, "application_1_1", "--log-dirs", "file://" + dir, "--format", format},
					&stdout, &stderr)
				if rc != 0 {
					t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
				}
				out := stdout.String()
				if out == "" {
					t.Errorf("empty output for %s/%s", sc, format)
				}
				// 应当含应用 wall 时长(fixture 跑 1s,formatAppDuration 输出 "app: 1.0s")
				if !bytes.Contains([]byte(out), []byte("app: ")) {
					t.Errorf("%s/%s missing app duration in header:\n%s", sc, format, out)
				}
			})
		}
	}
}

// 所有 5 场景的 envelope 顶层都应该输出 app_duration_ms(fixture 有
// ApplicationEnd 事件,DurationMs > 0)。round-2 加的字段必须在所有场景都生效,
// 不要某个 dispatch 路径漏掉。
func TestE2E_AllScenariosEmitAppDurationMs(t *testing.T) {
	dir := t.TempDir()
	src, err := os.ReadFile(filepath.Join("..", "testdata", "tiny_app.json"))
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "application_1_1")
	if err := os.WriteFile(logPath, src, 0o644); err != nil {
		t.Fatal(err)
	}

	for _, sc := range []string{"app-summary", "slow-stages", "data-skew", "gc-pressure", "diagnose"} {
		t.Run(sc, func(t *testing.T) {
			cmd.ResetForTest()
			var stdout, stderr bytes.Buffer
			rc := cmd.RunWith(context.Background(),
				[]string{sc, "application_1_1", "--log-dirs", "file://" + dir, "--format", "json"},
				&stdout, &stderr)
			if rc != 0 {
				t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
			}
			var m map[string]any
			if err := json.Unmarshal(stdout.Bytes(), &m); err != nil {
				t.Fatalf("not json: %v\n%s", err, stdout.String())
			}
			d, ok := m["app_duration_ms"]
			if !ok {
				t.Errorf("scenario=%s missing app_duration_ms\n%s", sc, stdout.String())
				return
			}
			if v, ok := d.(float64); !ok || v <= 0 {
				t.Errorf("scenario=%s app_duration_ms=%v want > 0", sc, d)
			}
		})
	}
}

// `spark-cli --version` 与 `spark-cli version` 输出一致(round-8 把 --version
// flag 接到 cobra Version 字段 + custom template,免得用户输 --version 报
// FLAG_INVALID)。
func TestE2E_VersionFlagAndCommandMatch(t *testing.T) {
	for _, args := range [][]string{{"--version"}, {"version"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			cmd.ResetForTest()
			var stdout, stderr bytes.Buffer
			rc := cmd.RunWith(context.Background(), args, &stdout, &stderr)
			if rc != 0 {
				t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
			}
			out := stdout.String()
			if !strings.HasPrefix(out, "spark-cli ") {
				t.Errorf("output=%q want prefix \"spark-cli \"", out)
			}
		})
	}
}

// 用户输错命令(typo)应当报 USER_ERR (rc=2) 而非 INTERNAL (rc=1)。
// 历史 bug:cobra "unknown command" 错误未被 wrap,默认走 INTERNAL,
// 让用户以为撞了内部 bug。
func TestE2E_UnknownCommandIsUserError(t *testing.T) {
	cmd.ResetForTest()
	var stdout, stderr bytes.Buffer
	rc := cmd.RunWith(context.Background(),
		[]string{"unknown-scenario", "application_x"},
		&stdout, &stderr)
	if rc != 2 {
		t.Errorf("rc=%d want 2 (USER_ERR), stderr=%s", rc, stderr.String())
	}
	if !bytes.Contains(stderr.Bytes(), []byte(`"FLAG_INVALID"`)) {
		t.Errorf("stderr missing FLAG_INVALID: %s", stderr.String())
	}
}

func TestE2E_AppNotFound(t *testing.T) {
	dir := t.TempDir()
	cmd.ResetForTest()
	var stdout, stderr bytes.Buffer
	rc := cmd.RunWith(context.Background(),
		[]string{"app-summary", "application_does_not_exist", "--log-dirs", "file://" + dir},
		&stdout, &stderr)
	if rc != 2 {
		t.Errorf("rc=%d want 2 stderr=%s", rc, stderr.String())
	}
	if !bytes.Contains(stderr.Bytes(), []byte(`"APP_NOT_FOUND"`)) {
		t.Errorf("stderr missing APP_NOT_FOUND:\n%s", stderr.String())
	}
}
