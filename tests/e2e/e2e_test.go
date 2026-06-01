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
		{"spark-conf", []string{"spark-conf", "application_1_1"}, []string{"scenario", "data", "summary"}},
		{"slow-stages", []string{"slow-stages", "application_1_1", "--top", "5"}, []string{"scenario", "data"}},
		{"data-skew", []string{"data-skew", "application_1_1"}, []string{"scenario", "data"}},
		{"gc-pressure", []string{"gc-pressure", "application_1_1"}, []string{"scenario", "data"}},
		{"native-io", []string{"native-io", "application_1_1"}, []string{"scenario", "data", "summary"}},
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

func TestE2E_SparkConfScenarioReturnsEnvironmentProperties(t *testing.T) {
	dir := t.TempDir()
	body := strings.Join([]string{
		`{"Event":"SparkListenerApplicationStart","App Name":"conf-app","App ID":"application_conf_1","Timestamp":1000,"User":"alice"}`,
		`{"Event":"SparkListenerEnvironmentUpdate","Spark Properties":{"spark.driver.memory":"4G","spark.sql.broadcastTimeout":"-1","spark.sql.autoBroadcastJoinThreshold":"10485760","spark.app.name":"conf-app"}}`,
		`{"Event":"SparkListenerApplicationEnd","Timestamp":2000}`,
	}, "\n")
	if err := os.WriteFile(filepath.Join(dir, "application_conf_1"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd.ResetForTest()
	var stdout, stderr bytes.Buffer
	rc := cmd.RunWith(context.Background(),
		[]string{"spark-conf", "application_conf_1", "--log-dirs", "file://" + dir, "--format", "json"},
		&stdout, &stderr)
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	var env map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("not json: %v\n%s", err, stdout.String())
	}
	if env["scenario"] != "spark-conf" {
		t.Fatalf("scenario=%v want spark-conf", env["scenario"])
	}
	summary := env["summary"].(map[string]any)
	if summary["total"].(float64) != 4 {
		t.Fatalf("summary.total=%v want 4", summary["total"])
	}
	rows := env["data"].([]any)
	if len(rows) != 4 {
		t.Fatalf("data rows=%d want 4", len(rows))
	}
	foundDriver := false
	for _, raw := range rows {
		row := raw.(map[string]any)
		if row["key"] == "spark.driver.memory" {
			foundDriver = true
			if row["category"] != "driver" || row["importance"] != "important" || row["tuning_hint"] == "" {
				t.Fatalf("bad driver memory row: %+v", row)
			}
		}
	}
	if !foundDriver {
		t.Fatalf("missing spark.driver.memory row: %+v", rows)
	}
}

func TestE2E_NativeIOScenario(t *testing.T) {
	dir := t.TempDir()
	body := strings.Join([]string{
		`{"Event":"SparkListenerApplicationStart","App Name":"native","App ID":"application_native_1","Timestamp":1000,"User":"alice"}`,
		`{"Event":"org.apache.spark.scheduler.SparkListenerNativeIOEvent","eventJson":"{\"event_id\":\"native-1\"}","native_io_schema_version":1,"native_io_event_id":"native-1","native_io_event_time":1500,"native_io_ai_kind":"paimon_native_io_reader","native_io_ai_summary":"native-columnar-read OPERATION_END phase=READ_BATCH duration_ms=500 rows=4096 bytes=16777216","native_io_event_type":"OPERATION_END","native_io_operation_id":"op-1","native_io_operation_name":"native-columnar-read","native_io_phase":"READ_BATCH","native_io_sql_execution_id":7,"native_io_stage_id":3,"native_io_task_attempt_id":99,"native_io_executor_id":"5","native_io_host":"worker-5","native_io_file_path":"obs://bucket/table/file.parquet","native_io_duration_ms":500,"native_io_rows":4096,"native_io_bytes":16777216,"native_io_metrics":{"read_batch_ms":500,"rows":4096,"bytes":16777216}}`,
		`{"Event":"SparkListenerApplicationEnd","Timestamp":3000}`,
	}, "\n")
	if err := os.WriteFile(filepath.Join(dir, "application_native_1"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd.ResetForTest()
	var stdout, stderr bytes.Buffer
	rc := cmd.RunWith(context.Background(),
		[]string{"native-io", "application_native_1", "--log-dirs", "file://" + dir, "--format", "json"},
		&stdout, &stderr)
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	var m map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &m); err != nil {
		t.Fatalf("not json: %v\n%s", err, stdout.String())
	}
	if m["scenario"] != "native-io" {
		t.Fatalf("scenario=%v want native-io", m["scenario"])
	}
	summary := m["summary"].(map[string]any)
	if summary["events_total"].(float64) != 1 || summary["reader_events"].(float64) != 1 {
		t.Fatalf("native summary wrong: %+v", summary)
	}
	data := m["data"].([]any)
	if len(data) != 1 {
		t.Fatalf("data rows=%d want 1", len(data))
	}
	row := data[0].(map[string]any)
	if row["operation_id"] != "op-1" || row["phase"] != "READ_BATCH" {
		t.Fatalf("native row wrong: %+v", row)
	}
	metrics := row["metrics"].(map[string]any)
	if metrics["read_batch_ms"].(float64) != 500 {
		t.Fatalf("native metrics wrong: %+v", metrics)
	}
}

// markdown / table format 在所有 EventLog 场景都不应崩溃,且 header 应当含
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
	for _, sc := range []string{"app-summary", "spark-conf", "slow-stages", "data-skew", "gc-pressure", "native-io", "diagnose"} {
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

// 所有 EventLog 场景的 envelope 顶层都应该输出 app_duration_ms(fixture 有
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

	for _, sc := range []string{"app-summary", "spark-conf", "slow-stages", "data-skew", "gc-pressure", "native-io", "diagnose"} {
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
