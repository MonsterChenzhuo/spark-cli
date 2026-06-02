# SparkMeasure AI Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Absorb EventLog-backed `sparkMeasure` metric semantics into `spark-cli` as compact AI-facing diagnosis signals.

**Architecture:** Extend the existing EventLog decoder and `model.Aggregator` with additional task metrics, then expose only decision-oriented derived fields through `app-summary`, `slow-stages`, and `diagnose`. Preserve the one-envelope JSON contract and keep all new output numeric and agent-readable.

**Tech Stack:** Go 1.22, Cobra CLI, existing `internal/model`, `internal/eventlog`, `internal/scenario`, `internal/rules`, e2e contract tests.

---

## File Structure

- Modify `internal/model/model.go`: add raw aggregate fields to `Application`, `Stage`, `Executor`, `TaskMetrics`, and `TaskEnd`.
- Modify `internal/model/aggregator.go`: aggregate the new metrics, compute task wall duration and scheduler delay.
- Modify `internal/model/aggregator_test.go`: add failing tests for metric aggregation and clamping.
- Modify `internal/eventlog/events.go`: parse Spark task fields already present in EventLog JSON.
- Modify `internal/eventlog/decoder.go`: convert parsed task fields into `model.TaskEnd`.
- Modify `internal/eventlog/decoder_test.go`: add a focused decode test for unit conversion and boolean/speculative parsing.
- Modify `internal/scenario/app_summary.go` and test: add compact app-level AI ratios.
- Modify `internal/scenario/slow_stages.go` and test: add bounded per-stage AI diagnosis fields.
- Modify `internal/rules/rule.go`: register new rules.
- Create `internal/rules/scheduler_delay_rule.go`, `remote_shuffle_rule.go`, `speculative_tasks_rule.go`.
- Modify `internal/rules/rule_test.go`: add rule tests.
- Modify `tests/e2e/e2e_test.go` only if exact output assertions need updates.
- Modify `AGENTS.md`, `CLAUDE.md`, `.agents/skills/spark/SKILL.md`, `.claude/skills/spark/SKILL.md`, `README.md`, `README.zh.md`, `CHANGELOG.md`, `CHANGELOG.zh.md`.

---

### Task 1: Model Aggregation

**Files:**
- Modify: `internal/model/model.go`
- Modify: `internal/model/aggregator.go`
- Test: `internal/model/aggregator_test.go`

- [ ] **Step 1: Write the failing aggregation test**

Add this test to `internal/model/aggregator_test.go`:

```go
func TestOnTaskEndAggregatesSparkMeasureAIMetrics(t *testing.T) {
	app := NewApplication()
	a := NewAggregator(app)
	a.OnExecutorAdded("1", "worker-1", 4, 0)
	a.OnStageSubmitted(3, 0, "shuffle", 2, 0)
	a.OnTaskEnd(TaskEnd{
		StageID: 3, Attempt: 0, ExecutorID: "1",
		LaunchMs: 1000, FinishMs: 2000, Speculative: true,
		Metrics: TaskMetrics{
			RunMs: 600, ExecutorCPUMs: 420, ExecutorDeserializeMs: 50,
			ResultSerializationMs: 25, GettingResultMs: 10,
			ResultSizeBytes: 4096, PeakExecutionMemoryBytes: 128 << 20,
			InputRecords: 10, OutputBytes: 2048, OutputRecords: 4,
			ShuffleLocalBytesRead: 100, ShuffleRemoteBytesRead: 900,
			ShuffleTotalBlocksFetched: 9, ShuffleLocalBlocksFetched: 1, ShuffleRemoteBlocksFetched: 8,
			ShuffleRecordsRead: 99, ShuffleWriteRecords: 7,
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/model -run TestOnTaskEndAggregatesSparkMeasureAIMetrics -count=1`

Expected: compile failure for missing fields such as `ExecutorCPUMs`.

- [ ] **Step 3: Implement model fields and aggregation**

Add fields with these exact names:

```go
// Application
TotalTaskDurationMs int64
TotalExecutorCPUMs int64
TotalSchedulerDelayMs int64
TotalResultSizeBytes int64
PeakExecutionMemoryBytes int64
TotalInputRecords int64
TotalOutputRecords int64
TotalShuffleReadRecords int64
TotalShuffleWriteRecords int64
TotalShuffleLocalReadBytes int64
TotalShuffleRemoteReadBytes int64
TotalShuffleTotalBlocksFetched int64
TotalShuffleLocalBlocksFetched int64
TotalShuffleRemoteBlocksFetched int64
SpeculativeTasks int64

// Executor
TotalTaskDurationMs int64
TotalExecutorCPUMs int64
TotalSchedulerDelayMs int64
SpeculativeTasks int64

// Stage
TotalTaskDurationMs int64
TotalExecutorCPUMs int64
TotalSchedulerDelayMs int64
TotalResultSerializationMs int64
TotalGettingResultMs int64
MaxResultSizeBytes int64
PeakExecutionMemoryBytes int64
TotalInputRecords int64
TotalOutputBytes int64
TotalOutputRecords int64
TotalShuffleReadRecords int64
TotalShuffleWriteRecords int64
TotalShuffleLocalReadBytes int64
TotalShuffleRemoteReadBytes int64
TotalShuffleTotalBlocksFetched int64
TotalShuffleLocalBlocksFetched int64
TotalShuffleRemoteBlocksFetched int64
SpeculativeTasks int64

// TaskMetrics
ExecutorCPUMs int64
ExecutorDeserializeMs int64
ResultSerializationMs int64
GettingResultMs int64
ResultSizeBytes int64
PeakExecutionMemoryBytes int64
InputRecords int64
OutputBytes int64
OutputRecords int64
ShuffleLocalBytesRead int64
ShuffleRemoteBytesRead int64
ShuffleTotalBlocksFetched int64
ShuffleLocalBlocksFetched int64
ShuffleRemoteBlocksFetched int64
ShuffleRecordsRead int64
ShuffleWriteRecords int64

// TaskEnd
Speculative bool
```

In `OnTaskEnd`, compute:

```go
taskDuration := t.FinishMs - t.LaunchMs
if taskDuration < 0 {
	taskDuration = 0
}
schedulerDelay := taskDuration - t.Metrics.RunMs - t.Metrics.ExecutorDeserializeMs - t.Metrics.ResultSerializationMs - t.Metrics.GettingResultMs
if schedulerDelay < 0 {
	schedulerDelay = 0
}
```

Aggregate the fields to stage, app, and executor. `TotalShuffleReadBytes` remains local plus remote.

- [ ] **Step 4: Run model tests**

Run: `go test ./internal/model -count=1`

Expected: PASS.

---

### Task 2: EventLog Decoding

**Files:**
- Modify: `internal/eventlog/events.go`
- Modify: `internal/eventlog/decoder.go`
- Test: `internal/eventlog/decoder_test.go`

- [ ] **Step 1: Write the failing decoder test**

Add a test that decodes one `SparkListenerTaskEnd` with all relevant Spark fields:

```go
func TestDecodeTaskEndSparkMeasureAIMetrics(t *testing.T) {
	body := strings.Join([]string{
		`{"Event":"SparkListenerApplicationStart","App Name":"metrics","App ID":"application_metrics","Timestamp":0,"User":"alice"}`,
		`{"Event":"SparkListenerExecutorAdded","Timestamp":1,"Executor ID":"1","Executor Info":{"Host":"worker-1","Total Cores":4}}`,
		`{"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":3,"Stage Attempt ID":0,"Stage Name":"shuffle","Number of Tasks":1,"Submission Time":10}}`,
		`{"Event":"SparkListenerTaskEnd","Stage ID":3,"Stage Attempt ID":0,"Task Info":{"Task ID":1,"Executor ID":"1","Host":"worker-1","Launch Time":1000,"Finish Time":2000,"Speculative":true,"Getting Result Time":1900,"Failed":false,"Killed":false},"Task Metrics":{"Executor Run Time":600,"Executor CPU Time":420000000,"Executor Deserialize Time":50,"Executor Deserialize CPU Time":3000000,"Result Serialization Time":25,"JVM GC Time":5,"Result Size":4096,"Peak Execution Memory":134217728,"Input Metrics":{"Bytes Read":1024,"Records Read":10},"Output Metrics":{"Bytes Written":2048,"Records Written":4},"Shuffle Read Metrics":{"Remote Bytes Read":900,"Local Bytes Read":100,"Fetch Wait Time":30,"Total Blocks Fetched":9,"Local Blocks Fetched":1,"Remote Blocks Fetched":8,"Total Records Read":99},"Shuffle Write Metrics":{"Shuffle Bytes Written":4096,"Shuffle Write Time":7000000,"Shuffle Records Written":7}}}`,
	}, "\n")
	app := model.NewApplication()
	_, err := Decode(strings.NewReader(body), model.NewAggregator(app))
	if err != nil {
		t.Fatal(err)
	}
	st := app.Stages[model.StageKey{ID: 3, Attempt: 0}]
	if st.TotalExecutorCPUMs != 420 {
		t.Fatalf("TotalExecutorCPUMs=%d want 420", st.TotalExecutorCPUMs)
	}
	if st.TotalGettingResultMs != 100 {
		t.Fatalf("TotalGettingResultMs=%d want 100", st.TotalGettingResultMs)
	}
	if st.TotalSchedulerDelayMs != 225 {
		t.Fatalf("TotalSchedulerDelayMs=%d want 225", st.TotalSchedulerDelayMs)
	}
	if st.SpeculativeTasks != 1 || st.TotalShuffleRemoteBlocksFetched != 8 {
		t.Fatalf("speculative/remote blocks wrong: %+v", st)
	}
}
```

- [ ] **Step 2: Run decoder test to verify it fails**

Run: `go test ./internal/eventlog -run TestDecodeTaskEndSparkMeasureAIMetrics -count=1`

Expected: compile failure or zero-value assertion failure.

- [ ] **Step 3: Parse task fields**

Add fields to `evtTaskEnd.TaskInfo`:

```go
Speculative bool  `json:"Speculative"`
GettingResultTime int64 `json:"Getting Result Time"`
```

Add fields to `TaskMetrics` JSON struct:

```go
ExecutorCPUTime int64 `json:"Executor CPU Time"`
ExecutorDeserializeCPUTime int64 `json:"Executor Deserialize CPU Time"`
ResultSerializationTime int64 `json:"Result Serialization Time"`
ResultSize int64 `json:"Result Size"`
PeakExecutionMemory int64 `json:"Peak Execution Memory"`
```

Add shuffle block fields:

```go
TotalBlocksFetched int64 `json:"Total Blocks Fetched"`
LocalBlocksFetched int64 `json:"Local Blocks Fetched"`
RemoteBlocksFetched int64 `json:"Remote Blocks Fetched"`
```

In `taskEndFromEvent`, convert nanoseconds to milliseconds:

```go
m.ExecutorCPUMs = e.TaskMetrics.ExecutorCPUTime / 1_000_000
m.ResultSerializationMs = e.TaskMetrics.ResultSerializationTime
m.GettingResultMs = gettingResultMs(e.TaskInfo.GettingResultTime, e.TaskInfo.FinishTime)
```

Implement `gettingResultMs(start, finish int64) int64` returning zero when `start == 0` or `finish <= start`.

- [ ] **Step 4: Run eventlog tests**

Run: `go test ./internal/eventlog -count=1`

Expected: PASS.

---

### Task 3: Scenario AI Fields

**Files:**
- Modify: `internal/scenario/app_summary.go`
- Modify: `internal/scenario/app_summary_test.go`
- Modify: `internal/scenario/slow_stages.go`
- Modify: `internal/scenario/slow_stages_test.go`

- [ ] **Step 1: Write failing scenario tests**

Add app summary test assertions for:

```go
if row.AvgActiveTasks < 2.99 || row.AvgActiveTasks > 3.01 {
	t.Fatalf("avg_active_tasks=%v want ~3.0", row.AvgActiveTasks)
}
if row.ExecutorCPURatio < 0.41 || row.ExecutorCPURatio > 0.43 {
	t.Fatalf("executor_cpu_ratio=%v want ~0.42", row.ExecutorCPURatio)
}
if row.SchedulerDelayRatio < 0.31 || row.SchedulerDelayRatio > 0.32 {
	t.Fatalf("scheduler_delay_ratio=%v want ~0.315", row.SchedulerDelayRatio)
}
if row.RemoteShuffleReadRatio < 0.89 || row.RemoteShuffleReadRatio > 0.91 {
	t.Fatalf("remote_shuffle_read_ratio=%v want ~0.9", row.RemoteShuffleReadRatio)
}
if row.SpeculativeTasks != 1 {
	t.Fatalf("speculative_tasks=%d want 1", row.SpeculativeTasks)
}
```

Add slow stage test assertions for:

```go
if rows[0].ExecutorCPURatio != 0.7 {
	t.Fatalf("executor_cpu_ratio=%v want 0.7", rows[0].ExecutorCPURatio)
}
if rows[0].SchedulerDelayMs != 315 {
	t.Fatalf("scheduler_delay_ms=%d want 315", rows[0].SchedulerDelayMs)
}
if rows[0].RemoteShuffleReadRatio != 0.9 {
	t.Fatalf("remote_shuffle_read_ratio=%v want 0.9", rows[0].RemoteShuffleReadRatio)
}
if rows[0].ShuffleRemoteBlocks != 8 {
	t.Fatalf("shuffle_remote_blocks=%d want 8", rows[0].ShuffleRemoteBlocks)
}
if rows[0].SpeculativeTasks != 1 {
	t.Fatalf("speculative_tasks=%d want 1", rows[0].SpeculativeTasks)
}
```

- [ ] **Step 2: Run scenario tests to verify failure**

Run: `go test ./internal/scenario -run 'Test(AppSummary|SlowStages)' -count=1`

Expected: compile failure for missing row fields.

- [ ] **Step 3: Add AI fields**

Add to `AppSummaryRow` and `AppSummaryColumns()`:

```go
AvgActiveTasks float64 `json:"avg_active_tasks"`
ExecutorCPURatio float64 `json:"executor_cpu_ratio"`
SchedulerDelayRatio float64 `json:"scheduler_delay_ratio"`
RemoteShuffleReadRatio float64 `json:"remote_shuffle_read_ratio"`
SpeculativeTasks int64 `json:"speculative_tasks"`
PeakExecutionMemoryGB float64 `json:"peak_execution_memory_gb"`
```

Add to `SlowStageRow` and `SlowStagesColumns()`:

```go
ExecutorCPURatio float64 `json:"executor_cpu_ratio"`
SchedulerDelayMs int64 `json:"scheduler_delay_ms"`
SchedulerDelayRatio float64 `json:"scheduler_delay_ratio"`
RemoteShuffleReadRatio float64 `json:"remote_shuffle_read_ratio"`
ShuffleRemoteBlocks int64 `json:"shuffle_remote_blocks"`
ShuffleTotalBlocks int64 `json:"shuffle_total_blocks"`
RecordsRead int64 `json:"records_read"`
RecordsWritten int64 `json:"records_written"`
ShuffleRecordsRead int64 `json:"shuffle_records_read"`
ShuffleRecordsWritten int64 `json:"shuffle_records_written"`
PeakExecutionMemoryGB float64 `json:"peak_execution_memory_gb"`
SpeculativeTasks int64 `json:"speculative_tasks"`
```

Use helpers:

```go
func ratio(num, den int64) float64 {
	if den <= 0 {
		return 0
	}
	return round3(float64(num) / float64(den))
}
```

- [ ] **Step 4: Run scenario tests**

Run: `go test ./internal/scenario -count=1`

Expected: PASS.

---

### Task 4: Diagnosis Rules

**Files:**
- Create: `internal/rules/scheduler_delay_rule.go`
- Create: `internal/rules/remote_shuffle_rule.go`
- Create: `internal/rules/speculative_tasks_rule.go`
- Modify: `internal/rules/rule.go`
- Test: `internal/rules/rule_test.go`

- [ ] **Step 1: Write failing rule tests**

Add tests that create one high-wall-share stage for each new signal and assert `Severity != "ok"`, `Evidence["stage_id"]`, and ratio fields.

Thresholds:

```go
scheduler_delay: stage wall_share >= 0.05, TotalSchedulerDelayMs >= 30000, scheduler_delay_ratio >= 0.25
remote_shuffle: stage wall_share >= 0.05, TotalShuffleReadBytes >= 1 GiB, remote_shuffle_read_ratio >= 0.5
speculative_tasks: stage wall_share >= 0.05, SpeculativeTasks >= 2, speculative_ratio >= 0.05
```

- [ ] **Step 2: Run rule tests to verify failure**

Run: `go test ./internal/rules -run 'Test(SchedulerDelayRule|RemoteShuffleRule|SpeculativeTasksRule)' -count=1`

Expected: compile failure for missing rule types.

- [ ] **Step 3: Implement rules**

Each rule scans stages, picks primary by `wallShare` descending, and returns `okFinding` when no candidate passes thresholds. Evidence must include:

```go
"stage_id": s.ID
"wall_share": round3(wallShare(s, app))
```

Add rule-specific fields:

```go
// scheduler_delay
"scheduler_delay_ms": s.TotalSchedulerDelayMs
"scheduler_delay_ratio": round3(float64(s.TotalSchedulerDelayMs) / float64(s.TotalTaskDurationMs))

// remote_shuffle
"shuffle_read_gb": round3(float64(s.TotalShuffleReadBytes) / 1024 / 1024 / 1024)
"remote_shuffle_read_ratio": round3(float64(s.TotalShuffleRemoteReadBytes) / float64(s.TotalShuffleReadBytes))

// speculative_tasks
"speculative_tasks": s.SpeculativeTasks
"speculative_ratio": round3(float64(s.SpeculativeTasks) / float64(s.TaskDurations.Count()))
```

Register rules in `All()` after spill/GC-related rules so existing high-confidence rules remain first, while `diagnose.summary.top_findings_by_impact` still controls ROI order.

- [ ] **Step 4: Run rules tests**

Run: `go test ./internal/rules -count=1`

Expected: PASS.

---

### Task 5: Documentation And Contract

**Files:**
- Modify: `AGENTS.md`
- Modify: `CLAUDE.md`
- Modify: `.agents/skills/spark/SKILL.md`
- Modify: `.claude/skills/spark/SKILL.md`
- Modify: `README.md`
- Modify: `README.zh.md`
- Modify: `CHANGELOG.md`
- Modify: `CHANGELOG.zh.md`

- [ ] **Step 1: Update docs**

Document:

- `app-summary` now exposes AI-only ratios for active tasks, CPU, scheduler delay, remote shuffle, speculative tasks, and peak memory.
- `slow-stages` now exposes per-stage scheduler delay, remote shuffle, records, blocks, CPU ratio, peak memory, and speculative signals.
- `diagnose` may emit `scheduler_delay`, `remote_shuffle`, and `speculative_tasks` findings.
- Agents should prioritize `top_findings_by_impact`, not severity strings.

- [ ] **Step 2: Run docs/contract tests**

Run: `go test ./tests/e2e -run TestDocs -count=1`

Expected: PASS, or update exact doc assertions if they intentionally track new field names.

---

### Task 6: Full Verification

**Files:**
- All modified files.

- [ ] **Step 1: Run unit tests**

Run: `make unit-test`

Expected: PASS.

- [ ] **Step 2: Run e2e**

Run: `make e2e`

Expected: PASS.

- [ ] **Step 3: Run lint gates**

Run: `make tidy && make lint`

Expected: PASS and no unintended `go.mod` / `go.sum` diff.

- [ ] **Step 4: Smoke JSON envelope**

Run:

```bash
mkdir -p /tmp/spark-cli-smoke
cp tests/testdata/tiny_app.json /tmp/spark-cli-smoke/application_1_1
go run . diagnose application_1_1 --log-dirs file:///tmp/spark-cli-smoke
go run . app-summary application_1_1 --log-dirs file:///tmp/spark-cli-smoke
```

Expected: each stdout is one JSON object with `contract_version: 1`.
