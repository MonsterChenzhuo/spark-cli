package scenario

import (
	"encoding/json"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
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

func TestStageSQLPrefersRealDescription(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[5] = &model.SQLExecution{
		ID:          5,
		Description: "select count(*) from orders where dt = '2026-05-01'",
		Details:     "== Physical Plan ==\nHashAggregate ...\n",
	}
	app.StageToSQL[1] = 5

	id, desc := stageSQL(app, 1)
	if id != 5 || desc != "select count(*) from orders where dt = '2026-05-01'" {
		t.Errorf("want real SQL, got id=%d desc=%q", id, desc)
	}
}

func TestStageSQLFallsBackOnGetCallSiteDescription(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[7] = &model.SQLExecution{
		ID:          7,
		Description: "getCallSite at SQLExecution.scala:74",
		Details:     "Execution: collect at MyJob.scala:42\n== Parsed Logical Plan ==\n...",
	}
	app.StageToSQL[2] = 7

	id, desc := stageSQL(app, 2)
	if id != 7 {
		t.Errorf("id=%d want 7", id)
	}
	if desc != "Execution: collect at MyJob.scala:42" {
		t.Errorf("want details first line, got %q", desc)
	}
}

func TestStageSQLFallsBackOnEmptyDescription(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[3] = &model.SQLExecution{
		ID:          3,
		Description: "",
		Details:     "Execution: save at FileSink.scala:99",
	}
	app.StageToSQL[4] = 3

	id, desc := stageSQL(app, 4)
	if id != 3 || desc != "Execution: save at FileSink.scala:99" {
		t.Errorf("want details fallback, got id=%d desc=%q", id, desc)
	}
}

func TestStageSQLKeepsCallSiteWhenNoDetails(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[1] = &model.SQLExecution{
		ID:          1,
		Description: "getCallSite at SQLExecution.scala:74",
		Details:     "",
	}
	app.StageToSQL[9] = 1

	id, desc := stageSQL(app, 9)
	if id != 1 || desc != "getCallSite at SQLExecution.scala:74" {
		t.Errorf("want callsite preserved when no details, got id=%d desc=%q", id, desc)
	}
}

func TestStageSQLReturnsNegOneWhenNoLink(t *testing.T) {
	app := model.NewApplication()
	id, desc := stageSQL(app, 99)
	if id != -1 || desc != "" {
		t.Errorf("want (-1, \"\"), got id=%d desc=%q", id, desc)
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
