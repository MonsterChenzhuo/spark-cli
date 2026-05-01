package scenario

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestSlowStagesAttachesSQLExecution(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[7] = &model.SQLExecution{ID: 7, Description: "select count(*) from t"}
	app.StageToSQL[1] = 7

	s := model.NewStage(1, 0, "stage-with-sql", 5, 0)
	s.SubmitMs = 0
	s.CompleteMs = 100
	s.Status = "succeeded"
	for i := 0; i < 5; i++ {
		s.TaskDurations.Add(20)
	}
	app.Stages[model.StageKey{ID: 1}] = s

	bare := model.NewStage(2, 0, "stage-no-sql", 1, 0)
	bare.SubmitMs = 0
	bare.CompleteMs = 50
	bare.Status = "succeeded"
	bare.TaskDurations.Add(10)
	app.Stages[model.StageKey{ID: 2}] = bare

	rows := SlowStages(app, 0)
	got := map[int]SlowStageRow{rows[0].StageID: rows[0], rows[1].StageID: rows[1]}
	if got[1].SQLExecutionID != 7 || got[1].SQLDescription != "select count(*) from t" {
		t.Errorf("stage 1 SQL link wrong: id=%d desc=%q", got[1].SQLExecutionID, got[1].SQLDescription)
	}
	if got[2].SQLExecutionID != -1 || got[2].SQLDescription != "" {
		t.Errorf("stage 2 should not link to SQL: id=%d desc=%q", got[2].SQLExecutionID, got[2].SQLDescription)
	}
}

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
