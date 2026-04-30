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
