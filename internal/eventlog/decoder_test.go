package eventlog

import (
	"os"
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
