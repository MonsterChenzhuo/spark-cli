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

// app-summary 是 single-row 多列(含 nested 数组)场景:历史横向布局让一行
// 1200+ 字符,nested 字段 stringify 成几百字符 inline JSON,终端不可读。改成
// "field | value" 纵向输出。
func TestTableSingleRowGoesVertical(t *testing.T) {
	env := scenario.Envelope{
		Scenario: "app-summary",
		Columns:  []string{"app_id", "duration_ms", "top_stages"},
		Data: []any{
			map[string]any{"app_id": "app_x", "duration_ms": 4231,
				"top_stages": []any{map[string]any{"stage_id": 1}}},
		},
	}
	var buf bytes.Buffer
	if err := WriteTable(&buf, env); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "field") || !strings.Contains(out, "value") {
		t.Errorf("expected vertical key/value layout, got:\n%s", out)
	}
	for _, want := range []string{"app_id", "app_x", "duration_ms", "4231", "top_stages"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q:\n%s", want, out)
		}
	}
}

func TestTableMultiRowStaysHorizontal(t *testing.T) {
	env := scenario.Envelope{
		Scenario: "slow-stages",
		Columns:  []string{"stage_id", "duration_ms"},
		Data: []any{
			map[string]any{"stage_id": 1, "duration_ms": 100},
			map[string]any{"stage_id": 2, "duration_ms": 200},
		},
	}
	var buf bytes.Buffer
	if err := WriteTable(&buf, env); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	// 多 row 不该触发 vertical 模式 — 表头第一行应当含两个列名
	parts := strings.SplitN(out, "\n", 3)
	if len(parts) < 2 || !strings.Contains(parts[1], "stage_id") || !strings.Contains(parts[1], "duration_ms") {
		t.Errorf("expected horizontal header line, got:\n%s", out)
	}
}

// envelope.sql_executions 应当被 table formatter 渲染成段落,每个 id 一行。
// historic table 输出只渲染主表 —— 但人类用 table 看 stage 时也需要 SQL 文本。
func TestTableRendersSQLExecutionsSection(t *testing.T) {
	env := scenario.Envelope{
		Scenario: "slow-stages",
		Columns:  []string{"stage_id"},
		Data: []any{
			map[string]any{"stage_id": 14},
		},
		SQLExecutions: map[int64]string{
			5: "select * from t",
			2: "SHOW DATABASES",
		},
	}
	var buf bytes.Buffer
	if err := WriteTable(&buf, env); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "=== sql_executions ===") {
		t.Errorf("missing section header:\n%s", out)
	}
	if !strings.Contains(out, "id=2:") || !strings.Contains(out, "id=5:") {
		t.Errorf("missing id rows:\n%s", out)
	}
	// 应当按 id 升序
	idx2 := strings.Index(out, "id=2:")
	idx5 := strings.Index(out, "id=5:")
	if idx2 == -1 || idx5 == -1 || idx2 > idx5 {
		t.Errorf("ids should be sorted ascending:\n%s", out)
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
