package scenario

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

// 契约: AppSummaryColumns() 必须与 AppSummaryRow JSON 字段完全对应,
// 否则下游按 columns 解析 data 会丢字段。
func TestAppSummaryColumnsMatchRowFields(t *testing.T) {
	row := AppSummary(model.NewApplication())
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := make([]string, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	sort.Strings(got)
	want := append([]string{}, AppSummaryColumns()...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columns mismatch\n got=%v\nwant=%v", got, want)
	}
}

func TestAppSummaryComputesGCRatioAndTopStages(t *testing.T) {
	app := model.NewApplication()
	app.ID = "application_1_1"
	app.Name = "etl"
	app.User = "alice"
	app.StartMs = 1000
	app.EndMs = 4000
	app.JobsTotal = 2
	app.TasksTotal = 100
	app.TasksFailed = 1
	app.TotalRunMs = 10000
	app.TotalGCMs = 1000

	for i, dur := range []int64{500, 200, 800} {
		s := model.NewStage(i, 0, "stage", 10, 0)
		s.SubmitMs = 0
		s.CompleteMs = dur
		s.Status = "succeeded"
		app.Stages[model.StageKey{ID: i, Attempt: 0}] = s
	}
	app.MaxConcurrentExecutors = 5

	row := AppSummary(app)
	if row.GCRatio < 0.099 || row.GCRatio > 0.101 {
		t.Errorf("gc_ratio=%v want ~0.1", row.GCRatio)
	}
	if len(row.TopStagesByDuration) != 3 {
		t.Fatalf("top_stages_by_duration=%d want 3", len(row.TopStagesByDuration))
	}
	if row.TopStagesByDuration[0].StageID != 2 || row.TopStagesByDuration[0].DurationMs != 800 {
		t.Errorf("top stage order wrong: %+v", row.TopStagesByDuration)
	}
}
