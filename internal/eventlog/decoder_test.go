package eventlog

import (
	"os"
	"strconv"
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

func TestDecodeNativeIOEventTopLevelFields(t *testing.T) {
	body := strings.Join([]string{
		`{"Event":"SparkListenerApplicationStart","App Name":"native","App ID":"application_native","Timestamp":1000,"User":"alice"}`,
		`{"Event":"org.apache.spark.scheduler.SparkListenerNativeIOEvent","eventJson":"{\"event_id\":\"native-1\"}","native_io_schema_version":1,"native_io_event_id":"native-1","native_io_event_time":1500,"native_io_ai_kind":"paimon_native_io_reader","native_io_ai_summary":"native-columnar-read OPERATION_END phase=READ_BATCH duration_ms=500 rows=4096 bytes=16384","native_io_event_type":"OPERATION_END","native_io_operation_id":"op-1","native_io_operation_name":"native-columnar-read","native_io_phase":"READ_BATCH","native_io_sql_execution_id":7,"native_io_stage_id":3,"native_io_stage_attempt_id":0,"native_io_task_attempt_id":99,"native_io_task_index":4,"native_io_attempt_number":1,"native_io_executor_id":"5","native_io_host":"worker-5","native_io_file_path":"obs://bucket/table/file.parquet","native_io_duration_ms":500,"native_io_rows":4096,"native_io_bytes":16384,"native_io_native_memory_bytes":1048576,"native_io_peak_buffered_bytes":2097152,"native_io_metrics":{"read_batch_ms":500,"read_batch_count":1,"rows":4096,"bytes":16384}}`,
		`{"Event":"SparkListenerApplicationEnd","Timestamp":3000}`,
	}, "\n")

	app := model.NewApplication()
	agg := model.NewAggregator(app)
	parsed, err := Decode(strings.NewReader(body), agg)
	if err != nil {
		t.Fatal(err)
	}
	if parsed != 3 {
		t.Fatalf("parsed = %d, want 3", parsed)
	}
	if len(app.NativeIOEvents) != 1 {
		t.Fatalf("native events = %d, want 1", len(app.NativeIOEvents))
	}
	ev := app.NativeIOEvents[0]
	if ev.EventID != "native-1" || ev.OperationID != "op-1" || ev.Phase != "READ_BATCH" {
		t.Fatalf("native event parsed wrong: %+v", ev)
	}
	if ev.SQLExecutionID != 7 || ev.StageID != 3 || ev.TaskAttemptID != 99 {
		t.Fatalf("spark context fields parsed wrong: %+v", ev)
	}
	if ev.DurationMs != 500 || ev.Rows != 4096 || ev.Bytes != 16384 {
		t.Fatalf("counters parsed wrong: %+v", ev)
	}
	if got := ev.Metrics["read_batch_ms"]; got != 500 {
		t.Fatalf("metrics read_batch_ms=%v, want 500", got)
	}
}

func TestDecodeNativeIOEventLegacyEventJson(t *testing.T) {
	nativeJSON := `{"version":1,"event_id":"legacy-1","event_time":1500,"event_type":"OPERATION_END","operation_id":"op-legacy","operation_name":"native-columnar-read","phase":"READ_BATCH","sql_execution_id":8,"stage_id":4,"stage_attempt_id":0,"task_attempt_id":100,"executor_id":"6","host":"worker-6","file_path":"obs://bucket/table/legacy.parquet","duration_ms":42,"rows":10,"bytes":1024,"metrics_json":"{\"read_batch_ms\":42,\"rows\":10}"}`
	body := strings.Join([]string{
		`{"Event":"SparkListenerApplicationStart","App Name":"native","App ID":"application_native","Timestamp":1000,"User":"alice"}`,
		`{"Event":"org.apache.spark.scheduler.SparkListenerNativeIOEvent","eventJson":` + strconv.Quote(nativeJSON) + `}`,
		`{"Event":"SparkListenerApplicationEnd","Timestamp":3000}`,
	}, "\n")

	app := model.NewApplication()
	agg := model.NewAggregator(app)
	_, err := Decode(strings.NewReader(body), agg)
	if err != nil {
		t.Fatal(err)
	}
	if len(app.NativeIOEvents) != 1 {
		t.Fatalf("native events = %d, want 1", len(app.NativeIOEvents))
	}
	ev := app.NativeIOEvents[0]
	if ev.EventID != "legacy-1" || ev.OperationID != "op-legacy" || ev.StageID != 4 {
		t.Fatalf("legacy eventJson parsed wrong: %+v", ev)
	}
	if got := ev.Metrics["read_batch_ms"]; got != 42 {
		t.Fatalf("legacy metrics read_batch_ms=%v, want 42", got)
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
