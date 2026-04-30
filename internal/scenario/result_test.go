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
