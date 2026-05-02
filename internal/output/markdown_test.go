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
