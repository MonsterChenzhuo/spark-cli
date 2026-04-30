package model

import "testing"

// Regression for the MinTaskMs sentinel bug: when the first task has duration 0,
// subsequent positive-duration tasks must NOT overwrite it. Min should stay 0.
func TestOnTaskEndMinPreservedAcrossZeroDuration(t *testing.T) {
	app := NewApplication()
	a := NewAggregator(app)
	a.OnStageSubmitted(0, 0, "s", 2, 100)
	a.OnTaskEnd(TaskEnd{StageID: 0, Attempt: 0, Metrics: TaskMetrics{RunMs: 0}})
	a.OnTaskEnd(TaskEnd{StageID: 0, Attempt: 0, Metrics: TaskMetrics{RunMs: 50}})
	st := app.Stages[StageKey{ID: 0, Attempt: 0}]
	if st.MinTaskMs != 0 {
		t.Errorf("MinTaskMs = %d, want 0", st.MinTaskMs)
	}
	if st.MaxTaskMs != 50 {
		t.Errorf("MaxTaskMs = %d, want 50", st.MaxTaskMs)
	}
}

// Regression: a task whose RunMs is zero and whose Launch/Finish indicate a
// negative wall-clock interval (clock skew) must be clamped to 0, not pollute
// totals or the t-digest with negative values.
func TestOnTaskEndNegativeDurationClamped(t *testing.T) {
	app := NewApplication()
	a := NewAggregator(app)
	a.OnStageSubmitted(0, 0, "s", 1, 100)
	a.OnTaskEnd(TaskEnd{
		StageID: 0, Attempt: 0,
		LaunchMs: 200, FinishMs: 100,
		Metrics: TaskMetrics{RunMs: 0},
	})
	st := app.Stages[StageKey{ID: 0, Attempt: 0}]
	if st.TotalRunMs != 0 {
		t.Errorf("TotalRunMs = %d, want 0", st.TotalRunMs)
	}
	if st.MinTaskMs != 0 || st.MaxTaskMs != 0 {
		t.Errorf("Min/Max = %d/%d, want 0/0", st.MinTaskMs, st.MaxTaskMs)
	}
}
