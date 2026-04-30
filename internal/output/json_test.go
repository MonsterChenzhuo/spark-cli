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
