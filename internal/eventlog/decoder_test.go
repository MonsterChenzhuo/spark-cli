package eventlog

import (
	"os"
	"strings"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestDecodeTinyApp(t *testing.T) {
	f, err := os.Open("../../tests/testdata/tiny_app.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	app := model.NewApplication()
	agg := model.NewAggregator(app)
	parsed, err := Decode(f, agg)
	if err != nil {
		t.Fatal(err)
	}
	if parsed != 10 {
		t.Errorf("parsed = %d", parsed)
	}
	if app.ID != "application_1_a" || app.Name != "tiny" || app.User != "alice" {
		t.Fatalf("app = %+v", app)
	}
	if app.DurationMs != 1000 {
		t.Errorf("duration = %d", app.DurationMs)
	}
	if len(app.Executors) != 1 {
		t.Errorf("executors = %d", len(app.Executors))
	}
	if len(app.Stages) != 1 {
		t.Errorf("stages = %d", len(app.Stages))
	}
	st := app.Stages[model.StageKey{ID: 0, Attempt: 0}]
	if st == nil || st.Status != "succeeded" {
		t.Fatalf("stage = %+v", st)
	}
	if st.TaskDurations.Count() != 2 {
		t.Errorf("task count = %d", st.TaskDurations.Count())
	}
	if st.MaxTaskMs != 500 {
		t.Errorf("max task = %d", st.MaxTaskMs)
	}
	if st.TotalShuffleWriteBytes != 2000 {
		t.Errorf("shuffle write = %d", st.TotalShuffleWriteBytes)
	}
	if st.TotalGCMs != 60 {
		t.Errorf("gc = %d", st.TotalGCMs)
	}
}

func TestDecodeAllowsTruncatedTailForIncompleteLogs(t *testing.T) {
	body := strings.Join([]string{
		`{"Event":"SparkListenerApplicationStart","App Name":"tail","App ID":"application_tail","Timestamp":1000,"User":"alice"}`,
		`{"Event":"SparkListenerExecutorAdded","Timestamp":1100,"Executor ID":"1","Executor Info":{"Host":"worker","Total Cores":1}}`,
		`{"Event":"SparkListenerTaskEnd"`,
	}, "\n")

	app := model.NewApplication()
	agg := model.NewAggregator(app)
	parsed, err := DecodeWithOptions(strings.NewReader(body), agg, DecodeOptions{AllowTruncatedTail: true})
	if err != nil {
		t.Fatal(err)
	}
	if parsed != 2 {
		t.Fatalf("parsed = %d, want 2", parsed)
	}
	if app.ID != "application_tail" {
		t.Fatalf("app id = %q", app.ID)
	}
	if len(app.Executors) != 1 {
		t.Fatalf("executors = %d, want 1", len(app.Executors))
	}
}

func TestDecodeStillRejectsTruncatedTailForCompleteLogs(t *testing.T) {
	body := strings.Join([]string{
		`{"Event":"SparkListenerApplicationStart","App Name":"tail","App ID":"application_tail","Timestamp":1000,"User":"alice"}`,
		`{"Event":"SparkListenerTaskEnd"`,
	}, "\n")

	app := model.NewApplication()
	agg := model.NewAggregator(app)
	parsed, err := DecodeWithOptions(strings.NewReader(body), agg, DecodeOptions{})
	if err == nil {
		t.Fatal("expected parse error")
	}
	if parsed != 1 {
		t.Fatalf("parsed = %d, want 1", parsed)
	}
}

func TestDecodeSkipsOversizedSQLExecutionStart(t *testing.T) {
	hugeDescription := strings.Repeat("x", maxScanLine)
	body := strings.Join([]string{
		`{"Event":"SparkListenerApplicationStart","App Name":"wide","App ID":"application_wide","Timestamp":1000,"User":"alice"}`,
		`{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart","executionId":7,"description":"` + hugeDescription + `","details":"","time":1100}`,
		`{"Event":"SparkListenerApplicationEnd","Timestamp":2000}`,
	}, "\n")

	app := model.NewApplication()
	agg := model.NewAggregator(app)
	parsed, err := Decode(strings.NewReader(body), agg)
	if err != nil {
		t.Fatal(err)
	}
	if parsed != 2 {
		t.Fatalf("parsed = %d, want 2", parsed)
	}
	if app.DurationMs != 1000 {
		t.Fatalf("duration = %d, want 1000", app.DurationMs)
	}
	if len(app.SQLExecutions) != 0 {
		t.Fatalf("SQLExecutions = %d, want skipped oversized SQL description", len(app.SQLExecutions))
	}
}

func TestDecodeSkipsOversizedSQLAdaptiveExecutionUpdate(t *testing.T) {
	hugePlan := strings.Repeat("x", maxScanLine)
	body := strings.Join([]string{
		`{"Event":"SparkListenerApplicationStart","App Name":"wide","App ID":"application_wide","Timestamp":1000,"User":"alice"}`,
		`{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate","executionId":7,"physicalPlanDescription":"` + hugePlan + `"}`,
		`{"Event":"SparkListenerApplicationEnd","Timestamp":2000}`,
	}, "\n")

	app := model.NewApplication()
	agg := model.NewAggregator(app)
	parsed, err := Decode(strings.NewReader(body), agg)
	if err != nil {
		t.Fatal(err)
	}
	if parsed != 2 {
		t.Fatalf("parsed = %d, want 2", parsed)
	}
	if app.DurationMs != 1000 {
		t.Fatalf("duration = %d, want 1000", app.DurationMs)
	}
}

func TestDecodeRejectsOversizedNonSQLEvent(t *testing.T) {
	hugeName := strings.Repeat("x", maxScanLine)
	body := `{"Event":"SparkListenerApplicationStart","App Name":"` + hugeName + `","App ID":"application_wide","Timestamp":1000,"User":"alice"}`

	app := model.NewApplication()
	agg := model.NewAggregator(app)
	parsed, err := Decode(strings.NewReader(body), agg)
	if err == nil {
		t.Fatal("expected oversized non-SQL event to fail")
	}
	if parsed != 0 {
		t.Fatalf("parsed = %d, want 0", parsed)
	}
}
