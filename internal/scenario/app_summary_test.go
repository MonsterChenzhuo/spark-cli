package scenario

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

// 契约: AppSummaryColumns() 必须与 AppSummaryRow JSON 字段完全对应,
// 否则下游按 columns 解析 data 会丢字段。
func TestAppSummaryColumnsMatchRowFields(t *testing.T) {
	row := AppSummary(model.NewApplication())
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
	want := append([]string{}, AppSummaryColumns()...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columns mismatch\n got=%v\nwant=%v", got, want)
	}
}

func TestAppSummaryComputesGCRatioAndTopStages(t *testing.T) {
	app := model.NewApplication()
	app.ID = "application_1_1"
	app.Name = "etl"
	app.User = "alice"
	app.StartMs = 1000
	app.EndMs = 4000
	app.JobsTotal = 2
	app.TasksTotal = 100
	app.TasksFailed = 1
	app.TotalRunMs = 10000
	app.TotalGCMs = 1000

	for i, dur := range []int64{500, 200, 800} {
		s := model.NewStage(i, 0, "stage", 10, 0)
		s.SubmitMs = 0
		s.CompleteMs = dur
		s.Status = "succeeded"
		app.Stages[model.StageKey{ID: i, Attempt: 0}] = s
	}
	app.MaxConcurrentExecutors = 5

	row := AppSummary(app)
	if row.GCRatio < 0.099 || row.GCRatio > 0.101 {
		t.Errorf("gc_ratio=%v want ~0.1", row.GCRatio)
	}
	if len(row.TopStagesByDuration) != 3 {
		t.Fatalf("top_stages_by_duration=%d want 3", len(row.TopStagesByDuration))
	}
	if row.TopStagesByDuration[0].StageID != 2 || row.TopStagesByDuration[0].DurationMs != 800 {
		t.Errorf("top stage order wrong: %+v", row.TopStagesByDuration)
	}
}

func TestAppSummaryTopStagesIncludeBusyRatio(t *testing.T) {
	app := model.NewApplication()
	app.MaxConcurrentExecutors = 10

	idle := model.NewStage(1, 0, "idle", 100, 0)
	idle.SubmitMs = 0
	idle.CompleteMs = 100_000
	idle.TotalRunMs = 2_000
	idle.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = idle

	busy := model.NewStage(2, 0, "busy", 100, 0)
	busy.SubmitMs = 0
	busy.CompleteMs = 10_000
	busy.TotalRunMs = 80_000
	busy.Status = "succeeded"
	app.Stages[model.StageKey{ID: 2}] = busy

	row := AppSummary(app)
	tops := map[int]TopStage{}
	for _, ts := range row.TopStagesByDuration {
		tops[ts.StageID] = ts
	}
	if got := tops[1].BusyRatio; got < 0.001 || got > 0.003 {
		t.Errorf("idle stage busy_ratio=%v want ~0.002", got)
	}
	if got := tops[2].BusyRatio; got < 0.79 || got > 0.81 {
		t.Errorf("busy stage busy_ratio=%v want ~0.8", got)
	}
}

// 真实 ETL 经常一边有大量 driver-side 等待 stage(busy_ratio 接近 0)、一边有
// 几个 executor 真正吃 CPU 的 stage(busy_ratio ~1)。`top_stages_by_duration`
// 按 wall 排会被 driver-side 等待 stage 占据,看不到真 CPU 瓶颈。
// `top_busy_stages` 单独按 busy_ratio*duration 排,只收 busy_ratio>0.8 的 stage,
// 让 agent / 用户一眼锁定真值得优化的 executor 热点。
func TestAppSummaryTopBusyStagesFiltersAndRanks(t *testing.T) {
	app := model.NewApplication()
	app.MaxConcurrentExecutors = 10

	// idle: busy_ratio ≈ 0.02,wall 100s —— 不应入榜
	idle := model.NewStage(1, 0, "idle", 100, 0)
	idle.SubmitMs = 0
	idle.CompleteMs = 100_000
	idle.TotalRunMs = 20_000
	idle.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = idle

	// hot1: busy_ratio = 1.0,wall 50s
	hot1 := model.NewStage(2, 0, "hot1", 100, 0)
	hot1.SubmitMs = 0
	hot1.CompleteMs = 50_000
	hot1.TotalRunMs = 600_000 // 远超 wall*slots
	hot1.Status = "succeeded"
	app.Stages[model.StageKey{ID: 2}] = hot1

	// hot2: busy_ratio ≈ 0.9,wall 80s,score = 80*0.9 > hot1
	hot2 := model.NewStage(3, 0, "hot2", 100, 0)
	hot2.SubmitMs = 0
	hot2.CompleteMs = 80_000
	hot2.TotalRunMs = 720_000
	hot2.Status = "succeeded"
	app.Stages[model.StageKey{ID: 3}] = hot2

	// borderline: busy_ratio = 0.5 不入榜(< 0.8 阈值)
	mid := model.NewStage(4, 0, "mid", 100, 0)
	mid.SubmitMs = 0
	mid.CompleteMs = 60_000
	mid.TotalRunMs = 300_000
	mid.Status = "succeeded"
	app.Stages[model.StageKey{ID: 4}] = mid

	row := AppSummary(app)
	if len(row.TopBusyStages) != 2 {
		t.Fatalf("top_busy_stages=%d want 2 (only stages with busy_ratio>0.8)", len(row.TopBusyStages))
	}
	// hot2 score 大于 hot1,应当排第一
	if row.TopBusyStages[0].StageID != 3 || row.TopBusyStages[1].StageID != 2 {
		t.Errorf("rank wrong: %+v", row.TopBusyStages)
	}
	for _, ts := range row.TopBusyStages {
		if ts.BusyRatio < 0.8 {
			t.Errorf("stage %d busy_ratio=%v should not appear in top_busy_stages", ts.StageID, ts.BusyRatio)
		}
	}
}

func TestAppSummaryBusyRatioClampsToOne(t *testing.T) {
	app := model.NewApplication()
	app.MaxConcurrentExecutors = 1
	s := model.NewStage(1, 0, "weird", 1, 0)
	s.SubmitMs = 0
	s.CompleteMs = 1_000
	s.TotalRunMs = 5_000
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = s

	row := AppSummary(app)
	if got := row.TopStagesByDuration[0].BusyRatio; got != 1.0 {
		t.Errorf("busy_ratio=%v want clamped to 1.0", got)
	}
}
