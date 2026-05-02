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

// data-skew row 排序应当按 wall_share 倒序(平局回退 skew_factor),与 SkewRule
// 选 primary 用 wall_share 的策略一致。历史按 f 排会把"f 极端但 wall 低"的
// stage 排到顶,真正最值得修的高 wall_share stage 反而被推到末尾。
func TestDataSkewSortsByWallShareThenSkewFactor(t *testing.T) {
	app := model.NewApplication()
	app.DurationMs = 1_000_000

	// stage 1: wall_share 0.3, skew f≈30(极端 ratio,但 wall 占比中等)
	s1 := model.NewStage(1, 0, "low-wall-extreme", 100, 0)
	s1.SubmitMs = 0
	s1.CompleteMs = 300_000
	s1.Status = "succeeded"
	for i := 0; i < 95; i++ {
		s1.TaskDurations.Add(100)
		s1.TaskInputBytes.Add(1024)
	}
	for i := 0; i < 5; i++ {
		s1.TaskDurations.Add(3000)
		s1.TaskInputBytes.Add(50 * 1024 * 1024)
	}
	s1.MaxInputBytes = 50 * 1024 * 1024
	app.Stages[model.StageKey{ID: 1}] = s1

	// stage 2: wall_share 0.7, skew f≈10(中等 ratio,但 wall 大)
	s2 := model.NewStage(2, 0, "high-wall-mid", 100, 0)
	s2.SubmitMs = 0
	s2.CompleteMs = 700_000
	s2.Status = "succeeded"
	for i := 0; i < 95; i++ {
		s2.TaskDurations.Add(1000)
		s2.TaskInputBytes.Add(10 * 1024 * 1024)
	}
	for i := 0; i < 5; i++ {
		s2.TaskDurations.Add(15_000)
		s2.TaskInputBytes.Add(20 * 1024 * 1024)
	}
	s2.MaxInputBytes = 20 * 1024 * 1024
	app.Stages[model.StageKey{ID: 2}] = s2

	rows := DataSkew(app, 10)
	if len(rows) != 2 {
		t.Fatalf("rows=%d want 2", len(rows))
	}
	if rows[0].StageID != 2 {
		t.Errorf("first row stage_id=%d want 2 (max wall_share)", rows[0].StageID)
	}
	if rows[1].StageID != 1 {
		t.Errorf("second row stage_id=%d want 1", rows[1].StageID)
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

// 镜像 SkewRule 的紧致闸门:任务时长 P99/P50 < 1.5 时 verdict 直接降到 mild,
// 不论 input_skew_factor 多大(任务时长均匀=没有真正倾斜)。wall_share 故意
// 设到 5%(> 1% 闸门阈值),确保命中的是新闸门而非既有 wall_share 闸门。
func TestDataSkewVerdictDowngradesToMildOnTightP99P50(t *testing.T) {
	app := model.NewApplication()
	app.DurationMs = 100_000
	s := model.NewStage(1, 0, "uniform-time", 200, 0)
	s.SubmitMs = 0
	s.CompleteMs = 5_000 // wall_share = 5%
	s.Status = "succeeded"
	for i := 0; i < 200; i++ {
		s.TaskDurations.Add(4_000)
		s.TaskInputBytes.Add(1024)
	}
	s.MaxInputBytes = 30_000_000 // input_skew_factor 极大,会触发 critical ladder
	app.Stages[model.StageKey{ID: 1}] = s

	rows := DataSkew(app, 10)
	if len(rows) != 1 {
		t.Fatalf("rows=%d", len(rows))
	}
	if rows[0].Verdict != "mild" {
		t.Errorf("verdict=%s want mild (tight P99/P50 应当降到 mild)", rows[0].Verdict)
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
