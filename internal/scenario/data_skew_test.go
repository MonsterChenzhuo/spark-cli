package scenario

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

// 契约: DataSkewColumns() 必须与 DataSkewRow JSON 字段完全对应。
func TestDataSkewColumnsMatchRowFields(t *testing.T) {
	row := DataSkewRow{}
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
	want := append([]string{}, DataSkewColumns()...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columns mismatch\n got=%v\nwant=%v", got, want)
	}
}

func TestDataSkewFiltersAndRanks(t *testing.T) {
	app := model.NewApplication()

	skewed := model.NewStage(1, 0, "skewed", 100, 0)
	for i := 0; i < 99; i++ {
		skewed.TaskDurations.Add(100)
		skewed.TaskInputBytes.Add(1024 * 1024)
	}
	skewed.TaskDurations.Add(20000)
	skewed.TaskInputBytes.Add(500 * 1024 * 1024)
	skewed.MaxInputBytes = 500 * 1024 * 1024
	skewed.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = skewed

	tooFew := model.NewStage(2, 0, "small", 10, 0)
	for i := 0; i < 10; i++ {
		tooFew.TaskDurations.Add(5000)
	}
	tooFew.Status = "succeeded"
	app.Stages[model.StageKey{ID: 2}] = tooFew

	short := model.NewStage(3, 0, "fast", 100, 0)
	for i := 0; i < 100; i++ {
		short.TaskDurations.Add(50)
	}
	short.Status = "succeeded"
	app.Stages[model.StageKey{ID: 3}] = short

	rows := DataSkew(app, 10)
	if len(rows) != 1 || rows[0].StageID != 1 {
		t.Fatalf("expected only stage 1, got %+v", rows)
	}
	if rows[0].Verdict != "severe" {
		t.Errorf("verdict=%s want severe", rows[0].Verdict)
	}
}

func TestDataSkewVerdictDowngradesOnUniformInput(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "uniform", 100, 0)
	for i := 0; i < 95; i++ {
		s.TaskDurations.Add(100)
		s.TaskInputBytes.Add(1024 * 1024)
	}
	for i := 0; i < 5; i++ {
		s.TaskDurations.Add(1700)
		s.TaskInputBytes.Add(1024 * 1024)
	}
	s.MaxInputBytes = 1024 * 1024
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = s

	rows := DataSkew(app, 10)
	if len(rows) != 1 || rows[0].Verdict != "warn" {
		t.Fatalf("expect single row with verdict=warn, got %+v", rows)
	}
}

func TestDataSkewVerdictDowngradesToMildOnNegligibleWallShare(t *testing.T) {
	app := model.NewApplication()
	app.DurationMs = 1_000_000 // 1000 秒
	s := model.NewStage(1, 0, "tiny-but-skewed", 100, 0)
	s.SubmitMs = 0
	s.CompleteMs = 5_000 // 5 秒,wall_share = 0.5%
	s.Status = "succeeded"
	for i := 0; i < 95; i++ {
		s.TaskDurations.Add(100)
		s.TaskInputBytes.Add(1024)
	}
	for i := 0; i < 5; i++ {
		s.TaskDurations.Add(1500) // p99/p50 = 15, 中等(<20)
		s.TaskInputBytes.Add(50 * 1024 * 1024)
	}
	s.MaxInputBytes = 50 * 1024 * 1024 // 高 input_skew → f 极大,触发 critical ladder
	app.Stages[model.StageKey{ID: 1}] = s

	rows := DataSkew(app, 10)
	if len(rows) != 1 {
		t.Fatalf("rows=%d", len(rows))
	}
	if rows[0].Verdict != "mild" {
		t.Errorf("verdict=%s want mild (negligible wall_share should downgrade)", rows[0].Verdict)
	}
	if rows[0].WallShare > 0.01 {
		t.Errorf("wall_share=%v want < 0.01", rows[0].WallShare)
	}
}

func TestDataSkewKeepsSevereOnExtremeRatioEvenWithSmallWall(t *testing.T) {
	app := model.NewApplication()
	app.DurationMs = 1_000_000
	s := model.NewStage(1, 0, "tiny-but-extreme", 100, 0)
	s.SubmitMs = 0
	s.CompleteMs = 5_000 // wall_share = 0.5%
	s.Status = "succeeded"
	for i := 0; i < 95; i++ {
		s.TaskDurations.Add(100)
		s.TaskInputBytes.Add(1024)
	}
	for i := 0; i < 5; i++ {
		s.TaskDurations.Add(2500) // p99/p50 = 25, 极端
		s.TaskInputBytes.Add(50 * 1024 * 1024)
	}
	s.MaxInputBytes = 50 * 1024 * 1024
	app.Stages[model.StageKey{ID: 1}] = s

	rows := DataSkew(app, 10)
	if len(rows) != 1 || rows[0].Verdict != "severe" {
		t.Fatalf("verdict should stay severe on extreme ratio, got %+v", rows)
	}
}

func TestDataSkewWallShareUnknownDoesNotTriggerDowngrade(t *testing.T) {
	app := model.NewApplication()
	// app.DurationMs = 0 → wall_share=0 → 闸门不应触发
	s := model.NewStage(1, 0, "no-app-end", 100, 0)
	s.SubmitMs = 0
	s.CompleteMs = 5_000
	s.Status = "succeeded"
	for i := 0; i < 95; i++ {
		s.TaskDurations.Add(100)
		s.TaskInputBytes.Add(1024)
	}
	for i := 0; i < 5; i++ {
		s.TaskDurations.Add(1500) // p99/p50 = 15, 中等
		s.TaskInputBytes.Add(50 * 1024 * 1024)
	}
	s.MaxInputBytes = 50 * 1024 * 1024 // input_skew_factor 很大,触发 critical ladder
	app.Stages[model.StageKey{ID: 1}] = s

	rows := DataSkew(app, 10)
	if len(rows) != 1 {
		t.Fatalf("rows=%d", len(rows))
	}
	if rows[0].Verdict != "severe" {
		t.Errorf("verdict=%s want severe (wall_share unknown should not downgrade)", rows[0].Verdict)
	}
	if rows[0].WallShare != 0 {
		t.Errorf("wall_share=%v want 0 when app.DurationMs=0", rows[0].WallShare)
	}
}
