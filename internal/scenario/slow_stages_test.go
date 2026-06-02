package scenario

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

// 契约: SlowStagesColumns() 必须与 SlowStageRow JSON 字段完全对应,否则下游
// 按 columns 解析 data 会丢字段。镜像 TestAppSummaryColumnsMatchRowFields。
func TestSlowStagesColumnsMatchRowFields(t *testing.T) {
	row := SlowStageRow{}
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := make([]string, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	sort.Strings(got)
	want := append([]string{}, SlowStagesColumns()...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columns mismatch\n got=%v\nwant=%v", got, want)
	}
}

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
	if got[1].SQLExecutionID != 7 {
		t.Errorf("stage 1 SQL link wrong: id=%d", got[1].SQLExecutionID)
	}
	if got[2].SQLExecutionID != -1 {
		t.Errorf("stage 2 should not link to SQL: id=%d", got[2].SQLExecutionID)
	}
}

func TestSlowStagesEmitsBusyRatioAndPartitionSize(t *testing.T) {
	app := model.NewApplication()
	app.MaxConcurrentExecutors = 50

	s := model.NewStage(1, 0, "shuffle-heavy", 200, 0)
	s.SubmitMs = 0
	s.CompleteMs = 60_000
	s.Status = "succeeded"
	s.TotalRunMs = 60_000 * 50 / 2 // 50 slots, 50% busy
	// 200 tasks × 5 MiB shuffle read per task = 1000 MiB total
	s.TotalShuffleReadBytes = 200 * 5 * 1024 * 1024
	// 200 tasks × 7 MiB input per task = 1400 MiB total
	s.TotalInputBytes = 200 * 7 * 1024 * 1024
	// 200 tasks × 3 MiB shuffle write per task = 600 MiB total
	s.TotalShuffleWriteBytes = 200 * 3 * 1024 * 1024
	for i := 0; i < 200; i++ {
		s.TaskDurations.Add(15_000)
	}
	app.Stages[model.StageKey{ID: 1}] = s

	rows := SlowStages(app, 0)
	if len(rows) != 1 {
		t.Fatalf("rows=%d", len(rows))
	}
	if got := rows[0].BusyRatio; got < 0.49 || got > 0.51 {
		t.Errorf("busy_ratio=%v want ~0.5", got)
	}
	if got := rows[0].ShuffleReadMBPerTask; got < 4.99 || got > 5.01 {
		t.Errorf("shuffle_read_mb_per_task=%v want ~5.0", got)
	}
	if got := rows[0].InputMBPerTask; got < 6.99 || got > 7.01 {
		t.Errorf("input_mb_per_task=%v want ~7.0", got)
	}
	if got := rows[0].ShuffleWriteMBPerTask; got < 2.99 || got > 3.01 {
		t.Errorf("shuffle_write_mb_per_task=%v want ~3.0", got)
	}
}

func TestSlowStagesIncludesSparkMeasureAISignals(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "ai-signals", 10, 0)
	s.SubmitMs = 0
	s.CompleteMs = 1_000
	s.Status = "succeeded"
	s.TotalRunMs = 600
	s.TotalTaskDurationMs = 1_000
	s.TotalExecutorCPUMs = 420
	s.TotalSchedulerDelayMs = 315
	s.TotalShuffleReadBytes = 1_000
	s.TotalShuffleRemoteReadBytes = 900
	s.TotalShuffleTotalBlocksFetched = 9
	s.TotalShuffleRemoteBlocksFetched = 8
	s.TotalInputRecords = 10
	s.TotalOutputRecords = 4
	s.TotalShuffleReadRecords = 99
	s.TotalShuffleWriteRecords = 7
	s.PeakExecutionMemoryBytes = 2 * 1024 * 1024 * 1024
	s.SpeculativeTasks = 1
	for i := 0; i < 10; i++ {
		s.TaskDurations.Add(100)
	}
	app.Stages[model.StageKey{ID: 1}] = s

	rows := SlowStages(app, 0)
	if len(rows) != 1 {
		t.Fatalf("rows=%d", len(rows))
	}
	if rows[0].ExecutorCPURatio != 0.7 {
		t.Fatalf("executor_cpu_ratio=%v want 0.7", rows[0].ExecutorCPURatio)
	}
	if rows[0].SchedulerDelayMs != 315 {
		t.Fatalf("scheduler_delay_ms=%d want 315", rows[0].SchedulerDelayMs)
	}
	if rows[0].SchedulerDelayRatio != 0.315 {
		t.Fatalf("scheduler_delay_ratio=%v want 0.315", rows[0].SchedulerDelayRatio)
	}
	if rows[0].RemoteShuffleReadRatio != 0.9 {
		t.Fatalf("remote_shuffle_read_ratio=%v want 0.9", rows[0].RemoteShuffleReadRatio)
	}
	if rows[0].ShuffleRemoteBlocks != 8 || rows[0].ShuffleTotalBlocks != 9 {
		t.Fatalf("shuffle blocks remote/total=%d/%d want 8/9", rows[0].ShuffleRemoteBlocks, rows[0].ShuffleTotalBlocks)
	}
	if rows[0].RecordsRead != 10 || rows[0].RecordsWritten != 4 {
		t.Fatalf("records read/written=%d/%d want 10/4", rows[0].RecordsRead, rows[0].RecordsWritten)
	}
	if rows[0].ShuffleRecordsRead != 99 || rows[0].ShuffleRecordsWritten != 7 {
		t.Fatalf("shuffle records read/written=%d/%d want 99/7", rows[0].ShuffleRecordsRead, rows[0].ShuffleRecordsWritten)
	}
	if rows[0].PeakExecutionMemoryGB != 2 {
		t.Fatalf("peak_execution_memory_gb=%v want 2", rows[0].PeakExecutionMemoryGB)
	}
	if rows[0].SpeculativeTasks != 1 {
		t.Fatalf("speculative_tasks=%d want 1", rows[0].SpeculativeTasks)
	}
}

func TestSlowStagesShuffleReadPerTaskZeroOnZeroTasks(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "no-tasks", 0, 0)
	s.SubmitMs = 0
	s.CompleteMs = 100
	s.Status = "succeeded"
	s.TotalShuffleReadBytes = 999 * 1024 * 1024
	s.TotalInputBytes = 800 * 1024 * 1024
	s.TotalShuffleWriteBytes = 700 * 1024 * 1024
	app.Stages[model.StageKey{ID: 1}] = s

	rows := SlowStages(app, 0)
	if len(rows) != 1 {
		t.Fatalf("rows=%d", len(rows))
	}
	if rows[0].ShuffleReadMBPerTask != 0 {
		t.Errorf("shuffle_read_mb_per_task=%v want 0 when num_tasks=0", rows[0].ShuffleReadMBPerTask)
	}
	if rows[0].InputMBPerTask != 0 {
		t.Errorf("input_mb_per_task=%v want 0 when num_tasks=0", rows[0].InputMBPerTask)
	}
	if rows[0].ShuffleWriteMBPerTask != 0 {
		t.Errorf("shuffle_write_mb_per_task=%v want 0 when num_tasks=0", rows[0].ShuffleWriteMBPerTask)
	}
}

func TestSlowStagesComputesGCRatio(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "stage", 10, 0)
	s.SubmitMs = 0
	s.CompleteMs = 1000
	s.Status = "succeeded"
	s.TotalRunMs = 1000
	s.TotalGCMs = 300
	for i := 0; i < 10; i++ {
		s.TaskDurations.Add(100)
	}
	app.Stages[model.StageKey{ID: 1}] = s

	rows := SlowStages(app, 0)
	if len(rows) != 1 {
		t.Fatalf("rows=%d", len(rows))
	}
	if rows[0].GCRatio < 0.299 || rows[0].GCRatio > 0.301 {
		t.Errorf("gc_ratio=%v want ~0.3", rows[0].GCRatio)
	}
}

func TestSlowStagesGCRatioZeroDivision(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "stage", 10, 0)
	s.SubmitMs = 0
	s.CompleteMs = 1000
	s.Status = "succeeded"
	s.TotalRunMs = 0
	s.TotalGCMs = 50
	app.Stages[model.StageKey{ID: 1}] = s

	rows := SlowStages(app, 0)
	if len(rows) != 1 || rows[0].GCRatio != 0 {
		t.Fatalf("expect gc_ratio=0 on zero run_ms, got %+v", rows)
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
