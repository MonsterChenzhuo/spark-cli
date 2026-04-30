package scenario

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

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
