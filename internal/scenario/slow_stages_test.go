package scenario

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestSlowStagesSortsByWallTimeDesc(t *testing.T) {
	app := model.NewApplication()
	for i, wall := range []int64{100, 500, 300} {
		s := model.NewStage(i, 0, "s", 10, 0)
		s.SubmitMs = 0
		s.CompleteMs = wall
		s.Status = "succeeded"
		for d := int64(0); d < 10; d++ {
			s.TaskDurations.Add(float64(wall / 10))
		}
		app.Stages[model.StageKey{ID: i}] = s
	}
	rows := SlowStages(app, 2)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0].StageID != 1 || rows[1].StageID != 2 {
		t.Errorf("order wrong: %+v", rows)
	}
}
