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

// markdown / table header 应当显示应用 wall 时长(让人类一眼知道跑了多久);
// app_duration_ms == 0 时不应显示这部分。
func TestMarkdownHeaderFormatAppDuration(t *testing.T) {
	cases := []struct {
		ms   int64
		want string
	}{
		{0, "no app duration"}, // 缺失:不应包含 "app:" 标记
		{30_000, "app: 30.0s"},
		{4_230_802, "app: 70.5min"},
	}
	for _, tc := range cases {
		env := scenario.Envelope{
			Scenario:      "diagnose",
			Columns:       []string{"x"},
			Data:          []any{},
			AppDurationMs: tc.ms,
		}
		var buf bytes.Buffer
		if err := WriteMarkdown(&buf, env); err != nil {
			t.Fatal(err)
		}
		out := buf.String()
		if tc.ms == 0 {
			if strings.Contains(out, "app:") {
				t.Errorf("ms=0 should not show app: marker, got:\n%s", out)
			}
		} else if !strings.Contains(out, tc.want) {
			t.Errorf("ms=%d should contain %q, got:\n%s", tc.ms, tc.want, out)
		}
	}
}

// 单元格内 `|` 必须转义成 `\|`,否则 SQL 文本(`select a | b ...`)、stage name
// 等含 pipe 的内容会破坏 markdown 表格语法,renderer 误以为多了列、整张表错位。
// 换行符 `\n` 同样破坏 row 边界,统一替换成空格。
func TestMarkdownEscapesPipeAndNewlineInCells(t *testing.T) {
	env := scenario.Envelope{
		Scenario: "slow-stages",
		Columns:  []string{"name", "sql"},
		Data: []any{
			map[string]any{"name": "stage|with|pipes", "sql": "select a\nfrom t | where x"},
			map[string]any{"name": "ok", "sql": "select 1"},
		},
	}
	var buf bytes.Buffer
	if err := WriteMarkdown(&buf, env); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	// pipe 应当被转义成 \|
	if !strings.Contains(out, "stage\\|with\\|pipes") {
		t.Errorf("pipe in cell not escaped:\n%s", out)
	}
	if !strings.Contains(out, "select a from t \\| where x") {
		t.Errorf("newline + pipe not handled:\n%s", out)
	}
	// 表格 row 应当仍然只有 2 列(每行 3 个 |:开头/中间/结尾)
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "|") && !strings.HasPrefix(line, "| --- |") {
			// 反斜杠转义后字面 | 数应当严格 = 3
			lit := strings.Count(line, "|") - strings.Count(line, "\\|")
			if lit != 3 {
				t.Errorf("row %q has %d unescaped pipes (want 3)", line, lit)
			}
		}
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
