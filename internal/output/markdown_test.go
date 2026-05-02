package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/scenario"
)

// 历史 bug:dispatch 把 scenario row(struct,如 rules.Finding / SlowStageRow)
// 塞进 []any,但旧 toRowSlice 只接受 map[string]any,导致 struct row 全部被丢弃,
// `--format markdown` / `--format table` 输出只剩表头无数据。
type fakeRow struct {
	StageID    int   `json:"stage_id"`
	DurationMs int64 `json:"duration_ms"`
}

func TestMarkdownRendersStructRowsViaJSONRoundTrip(t *testing.T) {
	env := scenario.Envelope{
		Scenario: "slow-stages",
		Columns:  []string{"stage_id", "duration_ms"},
		Data: []any{
			fakeRow{StageID: 7, DurationMs: 1234},
		},
	}
	var buf bytes.Buffer
	if err := WriteMarkdown(&buf, env); err != nil {
		t.Fatalf("WriteMarkdown: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "| 7 |") || !strings.Contains(out, "| 1234 |") {
		t.Errorf("struct row not rendered:\n%s", out)
	}
}

// app-summary 是 single-row 多列(含 nested 数组)场景,旧代码塞 25 列横向
// 表格 + nested JSON 单元格几百字符,人类不可读。改成 single-row → vertical
// "field | value" 表格;多 row 场景仍走横向表格。
func TestMarkdownSingleRowGoesVertical(t *testing.T) {
	env := scenario.Envelope{
		Scenario: "app-summary",
		Columns:  []string{"app_id", "duration_ms", "top_stages"},
		Data: []any{
			map[string]any{"app_id": "app_x", "duration_ms": 1234,
				"top_stages": []any{map[string]any{"stage_id": 1}}},
		},
	}
	var buf bytes.Buffer
	if err := WriteMarkdown(&buf, env); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "| field | value |") {
		t.Errorf("expected vertical table, got:\n%s", out)
	}
	if !strings.Contains(out, "| app_id | app_x |") {
		t.Errorf("missing app_id row:\n%s", out)
	}
	if !strings.Contains(out, "| duration_ms | 1234 |") {
		t.Errorf("missing duration_ms row:\n%s", out)
	}
}

func TestMarkdownMultiRowStaysHorizontal(t *testing.T) {
	env := scenario.Envelope{
		Scenario: "slow-stages",
		Columns:  []string{"stage_id", "duration_ms"},
		Data: []any{
			map[string]any{"stage_id": 1, "duration_ms": 100},
			map[string]any{"stage_id": 2, "duration_ms": 200},
		},
	}
	var buf bytes.Buffer
	if err := WriteMarkdown(&buf, env); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	// 多 row 不应触发 vertical 模式
	if strings.Contains(out, "| field | value |") {
		t.Errorf("multi-row should stay horizontal, got:\n%s", out)
	}
	if !strings.Contains(out, "| stage_id | duration_ms |") {
		t.Errorf("missing horizontal header:\n%s", out)
	}
}

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
