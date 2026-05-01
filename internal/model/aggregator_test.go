package model

import (
	"testing"
)

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

func TestOnEnvironmentUpdateAccumulates(t *testing.T) {
	app := NewApplication()
	a := NewAggregator(app)
	a.OnEnvironmentUpdate(map[string]string{
		"spark.sql.shuffle.partitions": "200",
		"spark.executor.memory":        "4g",
	})
	a.OnEnvironmentUpdate(map[string]string{
		"spark.sql.adaptive.enabled": "true",
	})
	if app.SparkConf["spark.sql.shuffle.partitions"] != "200" {
		t.Errorf("shuffle partitions lost: %q", app.SparkConf["spark.sql.shuffle.partitions"])
	}
	if app.SparkConf["spark.sql.adaptive.enabled"] != "true" {
		t.Errorf("adaptive flag lost: %q", app.SparkConf["spark.sql.adaptive.enabled"])
	}
	if app.SparkConf["spark.executor.memory"] != "4g" {
		t.Errorf("executor.memory lost across updates: %q", app.SparkConf["spark.executor.memory"])
	}
}

func TestOnSQLExecutionStartEndPopulatesMap(t *testing.T) {
	app := NewApplication()
	a := NewAggregator(app)
	a.OnSQLExecutionStart(7, "select * from t", "Project [...]\nFilter [...]\n", 1000)
	a.OnSQLExecutionEnd(7, 1500)
	got, ok := app.SQLExecutions[7]
	if !ok {
		t.Fatalf("SQL execution 7 not recorded")
	}
	if got.Description != "select * from t" {
		t.Errorf("description = %q", got.Description)
	}
	if got.StartMs != 1000 || got.EndMs != 1500 {
		t.Errorf("times = %d/%d", got.StartMs, got.EndMs)
	}
}

func TestOnJobStartLinksStagesToSQL(t *testing.T) {
	app := NewApplication()
	a := NewAggregator(app)
	a.OnJobStart(42, []int{10, 11, 12}, 1000, map[string]string{
		"spark.sql.execution.id": "9",
	})
	if app.Jobs[42].SQLExecutionID != 9 {
		t.Errorf("Job.SQLExecutionID = %d, want 9", app.Jobs[42].SQLExecutionID)
	}
	for _, sid := range []int{10, 11, 12} {
		if app.StageToSQL[sid] != 9 {
			t.Errorf("StageToSQL[%d] = %d, want 9", sid, app.StageToSQL[sid])
		}
	}
}

func TestOnJobStartWithoutSQLPropertyKeepsSentinel(t *testing.T) {
	app := NewApplication()
	a := NewAggregator(app)
	a.OnJobStart(1, []int{0}, 0, nil)
	if app.Jobs[1].SQLExecutionID != -1 {
		t.Errorf("expected -1 sentinel, got %d", app.Jobs[1].SQLExecutionID)
	}
	if _, ok := app.StageToSQL[0]; ok {
		t.Errorf("StageToSQL should be empty for SQL-less job")
	}
}

func TestOnNodeAndExecutorBlacklistedAppendOrder(t *testing.T) {
	app := NewApplication()
	a := NewAggregator(app)
	a.OnNodeBlacklisted(1000, "host-a", 5, 4)
	a.OnExecutorBlacklisted(1100, "exec-7", 5, 3)
	if len(app.Blacklists) != 2 {
		t.Fatalf("blacklist len = %d, want 2", len(app.Blacklists))
	}
	if app.Blacklists[0].Kind != "node" || app.Blacklists[0].Target != "host-a" {
		t.Errorf("first event = %+v", app.Blacklists[0])
	}
	if app.Blacklists[1].Kind != "executor" || app.Blacklists[1].Target != "exec-7" {
		t.Errorf("second event = %+v", app.Blacklists[1])
	}
}
