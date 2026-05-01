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
}

func TestSlowStagesShuffleReadPerTaskZeroOnZeroTasks(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "no-tasks", 0, 0)
	s.SubmitMs = 0
	s.CompleteMs = 100
	s.Status = "succeeded"
	s.TotalShuffleReadBytes = 999 * 1024 * 1024
	app.Stages[model.StageKey{ID: 1}] = s

	rows := SlowStages(app, 0)
	if len(rows) != 1 || rows[0].ShuffleReadMBPerTask != 0 {
		t.Fatalf("expect shuffle_read_mb_per_task=0 when num_tasks=0, got %+v", rows)
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
