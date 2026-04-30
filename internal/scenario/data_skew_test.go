package scenario

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestDataSkewFiltersAndRanks(t *testing.T) {
	app := model.NewApplication()

	skewed := model.NewStage(1, 0, "skewed", 100, 0)
	for i := 0; i < 99; i++ {
		skewed.TaskDurations.Add(100)
		skewed.TaskInputBytes.Add(1024 * 1024)
	}
	skewed.TaskDurations.Add(20000)
	skewed.TaskInputBytes.Add(500 * 1024 * 1024)
	skewed.MaxInputBytes = 500 * 1024 * 1024
	skewed.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = skewed

	tooFew := model.NewStage(2, 0, "small", 10, 0)
	for i := 0; i < 10; i++ {
		tooFew.TaskDurations.Add(5000)
	}
	tooFew.Status = "succeeded"
	app.Stages[model.StageKey{ID: 2}] = tooFew

	short := model.NewStage(3, 0, "fast", 100, 0)
	for i := 0; i < 100; i++ {
		short.TaskDurations.Add(50)
	}
	short.Status = "succeeded"
	app.Stages[model.StageKey{ID: 3}] = short

	rows := DataSkew(app, 10)
	if len(rows) != 1 || rows[0].StageID != 1 {
		t.Fatalf("expected only stage 1, got %+v", rows)
	}
	if rows[0].Verdict != "severe" {
		t.Errorf("verdict=%s want severe", rows[0].Verdict)
	}
}
