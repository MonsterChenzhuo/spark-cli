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

func TestOnTaskEndAggregatesSparkMeasureAIMetrics(t *testing.T) {
	app := NewApplication()
	a := NewAggregator(app)
	a.OnExecutorAdded("1", "worker-1", 4, 0)
	a.OnStageSubmitted(3, 0, "shuffle", 2, 0)
	a.OnTaskEnd(TaskEnd{
		StageID:     3,
		Attempt:     0,
		ExecutorID:  "1",
		LaunchMs:    1000,
		FinishMs:    2000,
		Speculative: true,
		Metrics: TaskMetrics{
			RunMs:                      600,
			ExecutorCPUMs:              420,
			ExecutorDeserializeMs:      50,
			ResultSerializationMs:      25,
			GettingResultMs:            10,
			ResultSizeBytes:            4096,
			PeakExecutionMemoryBytes:   128 << 20,
			InputRecords:               10,
			OutputBytes:                2048,
			OutputRecords:              4,
			ShuffleLocalBytesRead:      100,
			ShuffleRemoteBytesRead:     900,
			ShuffleTotalBlocksFetched:  9,
			ShuffleLocalBlocksFetched:  1,
			ShuffleRemoteBlocksFetched: 8,
			ShuffleRecordsRead:         99,
			ShuffleWriteRecords:        7,
		},
	})
	st := app.Stages[StageKey{ID: 3, Attempt: 0}]
	if st.TotalTaskDurationMs != 1000 {
		t.Fatalf("TotalTaskDurationMs=%d want 1000", st.TotalTaskDurationMs)
	}
	if st.TotalSchedulerDelayMs != 315 {
		t.Fatalf("TotalSchedulerDelayMs=%d want 315", st.TotalSchedulerDelayMs)
	}
	if st.TotalExecutorCPUMs != 420 || app.TotalExecutorCPUMs != 420 {
		t.Fatalf("executor cpu stage/app=%d/%d want 420/420", st.TotalExecutorCPUMs, app.TotalExecutorCPUMs)
	}
	if st.TotalShuffleReadBytes != 1000 || st.TotalShuffleRemoteReadBytes != 900 || st.TotalShuffleLocalReadBytes != 100 {
		t.Fatalf("shuffle read bytes local/remote/total=%d/%d/%d", st.TotalShuffleLocalReadBytes, st.TotalShuffleRemoteReadBytes, st.TotalShuffleReadBytes)
	}
	if st.SpeculativeTasks != 1 || app.SpeculativeTasks != 1 {
		t.Fatalf("speculative stage/app=%d/%d want 1/1", st.SpeculativeTasks, app.SpeculativeTasks)
	}
	if st.MaxResultSizeBytes != 4096 || st.PeakExecutionMemoryBytes != 128<<20 {
		t.Fatalf("max result/peak memory=%d/%d", st.MaxResultSizeBytes, st.PeakExecutionMemoryBytes)
	}
	if app.TotalOutputBytes != 2048 || st.TotalOutputRecords != 4 || st.TotalShuffleReadRecords != 99 || st.TotalShuffleWriteRecords != 7 {
		t.Fatalf("records/output aggregation wrong: appOutput=%d stage=%+v", app.TotalOutputBytes, st)
	}
	exec := app.Executors["1"]
	if exec.TotalSchedulerDelayMs != 315 || exec.SpeculativeTasks != 1 {
		t.Fatalf("executor aggregation wrong: %+v", exec)
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
