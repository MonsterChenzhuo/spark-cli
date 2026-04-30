package scenario

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestGCPressureRanksAndClassifies(t *testing.T) {
	app := model.NewApplication()
	app.Executors["hi"] = &model.Executor{ID: "hi", Host: "h1", TotalRunMs: 10000, TotalGCMs: 4000, TaskCount: 100}
	app.Executors["mid"] = &model.Executor{ID: "mid", Host: "h2", TotalRunMs: 10000, TotalGCMs: 1500, TaskCount: 80}
	app.Executors["lo"] = &model.Executor{ID: "lo", Host: "h3", TotalRunMs: 10000, TotalGCMs: 200, TaskCount: 50}
	app.Executors["zero"] = &model.Executor{ID: "zero", Host: "h4", TotalRunMs: 0, TotalGCMs: 0, TaskCount: 0}

	rows := GCPressure(app, 10)
	if len(rows) != 3 {
		t.Fatalf("want 3 rows (zero-run filtered), got %d: %+v", len(rows), rows)
	}
	if rows[0].ExecutorID != "hi" || rows[0].Verdict != "severe" {
		t.Errorf("expected hi/severe first, got %+v", rows[0])
	}
	if rows[1].ExecutorID != "mid" || rows[1].Verdict != "warn" {
		t.Errorf("expected mid/warn second, got %+v", rows[1])
	}
	if rows[2].Verdict != "ok" {
		t.Errorf("expected ok last, got %+v", rows[2])
	}
}
