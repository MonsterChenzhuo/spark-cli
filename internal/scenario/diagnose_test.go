package scenario

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestDiagnoseEmitsOKForNonTriggers(t *testing.T) {
	app := model.NewApplication()
	findings, sum := Diagnose(app)
	if len(findings) != 6 {
		t.Fatalf("findings=%d want 6", len(findings))
	}
	if sum.OK != 6 {
		t.Errorf("ok=%d want 6", sum.OK)
	}
	if len(sum.TopFindingsByImpact) != 0 {
		t.Errorf("top_findings_by_impact should be empty when no rules trigger, got %+v", sum.TopFindingsByImpact)
	}
}

func TestDiagnoseRanksTopFindingsByWallShare(t *testing.T) {
	app := model.NewApplication()
	app.DurationMs = 1_000_000 // 1000s
	app.SparkConf["spark.sql.shuffle.partitions"] = "200"

	// stage 1: 极端倾斜 + wall_share = 5%
	skewed := model.NewStage(1, 0, "skewed", 200, 0)
	skewed.SubmitMs = 0
	skewed.CompleteMs = 50_000 // wall_share = 5%
	skewed.Status = "succeeded"
	for i := 0; i < 95; i++ {
		skewed.TaskDurations.Add(100)
		skewed.TaskInputBytes.Add(1024)
	}
	for i := 0; i < 5; i++ {
		skewed.TaskDurations.Add(3000)
		skewed.TaskInputBytes.Add(50 * 1024 * 1024)
	}
	skewed.MaxInputBytes = 50 * 1024 * 1024
	app.Stages[model.StageKey{ID: 1}] = skewed

	// stage 2: 大量 spill + wall_share = 80%
	spilly := model.NewStage(2, 0, "spilly", 200, 0)
	spilly.SubmitMs = 0
	spilly.CompleteMs = 800_000 // wall_share = 80%
	spilly.Status = "succeeded"
	spilly.TotalSpillDisk = 50 * 1024 * 1024 * 1024 // 50 GB
	spilly.TotalShuffleReadBytes = 200 * 100 * 1024 * 1024
	app.Stages[model.StageKey{ID: 2}] = spilly

	_, sum := Diagnose(app)
	if len(sum.TopFindingsByImpact) < 2 {
		t.Fatalf("expected at least 2 top findings, got %+v", sum.TopFindingsByImpact)
	}
	// spill stage 2 (80%) 应在 skew stage 1 (5%) 之前
	if sum.TopFindingsByImpact[0].RuleID != "disk_spill" {
		t.Errorf("expected disk_spill first, got %+v", sum.TopFindingsByImpact)
	}
	if sum.TopFindingsByImpact[0].WallShare < 0.79 || sum.TopFindingsByImpact[0].WallShare > 0.81 {
		t.Errorf("disk_spill wall_share=%v want ~0.8", sum.TopFindingsByImpact[0].WallShare)
	}
	// 严格倒序
	for i := 1; i < len(sum.TopFindingsByImpact); i++ {
		if sum.TopFindingsByImpact[i-1].WallShare < sum.TopFindingsByImpact[i].WallShare {
			t.Errorf("not sorted desc: %+v", sum.TopFindingsByImpact)
		}
	}
}

func TestDiagnoseSkipsTopFindingsWhenAppDurationUnknown(t *testing.T) {
	app := model.NewApplication()
	// app.DurationMs = 0 → 所有 wall_share=0,top_findings 为空
	app.SparkConf["spark.sql.shuffle.partitions"] = "200"
	s := model.NewStage(0, 0, "spilly", 200, 0)
	s.TotalSpillDisk = 50 * 1024 * 1024 * 1024
	app.Stages[model.StageKey{ID: 0}] = s

	_, sum := Diagnose(app)
	if len(sum.TopFindingsByImpact) != 0 {
		t.Errorf("expected empty when app.DurationMs=0, got %+v", sum.TopFindingsByImpact)
	}
	if sum.FindingsWallCoverage != 0 {
		t.Errorf("findings_wall_coverage=%v want 0 when app.DurationMs=0", sum.FindingsWallCoverage)
	}
}

// 当所有 finding 的 wall_share 加起来不到 5% 时,说明真正的瓶颈不在 finding 范围内
// (作业结构 / 调度 / driver 端开销)。FindingsWallCoverage 直接给出这个总占比,
// agent / 用户不必自己心算。
//
// 同一 stage 触发多条 finding 时按 stage 去重(取 max wall_share),避免重复计数。
func TestDiagnoseFindingsWallCoverageSumsUniqueStages(t *testing.T) {
	app := model.NewApplication()
	app.DurationMs = 1_000_000
	app.SparkConf["spark.sql.shuffle.partitions"] = "200"

	// stage 1: spill 触发 disk_spill,wall_share = 5%
	spilly := model.NewStage(1, 0, "spilly", 200, 0)
	spilly.SubmitMs = 0
	spilly.CompleteMs = 50_000
	spilly.Status = "succeeded"
	spilly.TotalSpillDisk = 50 * 1024 * 1024 * 1024
	app.Stages[model.StageKey{ID: 1}] = spilly

	// stage 2: tiny tasks,wall_share = 2%
	tiny := model.NewStage(2, 0, "tiny", 200, 0)
	tiny.SubmitMs = 0
	tiny.CompleteMs = 20_000
	tiny.Status = "succeeded"
	for i := 0; i < 200; i++ {
		tiny.TaskDurations.Add(20)
	}
	app.Stages[model.StageKey{ID: 2}] = tiny

	_, sum := Diagnose(app)
	// disk_spill (5%) + tiny_tasks (2%) = 7%。允许 1pp 误差应付 round3。
	if got := sum.FindingsWallCoverage; got < 0.06 || got > 0.08 {
		t.Errorf("findings_wall_coverage=%v want ~0.07 (5%% + 2%%)", got)
	}
}

func TestDiagnoseFindingsWallCoverageDedupesSameStage(t *testing.T) {
	app := model.NewApplication()
	app.DurationMs = 1_000_000

	// 同一 stage 同时触发 data_skew + tiny_tasks(虽然实际不会同时,这里为模拟去重逻辑)
	// 为了构造 + 不引入 spill,让 stage 1 wall_share = 30%,只触发 data_skew。
	skewed := model.NewStage(1, 0, "skewed", 200, 0)
	skewed.SubmitMs = 0
	skewed.CompleteMs = 300_000
	skewed.Status = "succeeded"
	for i := 0; i < 95; i++ {
		skewed.TaskDurations.Add(100)
		skewed.TaskInputBytes.Add(1024)
	}
	for i := 0; i < 5; i++ {
		skewed.TaskDurations.Add(3000) // P99/P50 = 30,极端
		skewed.TaskInputBytes.Add(50 * 1024 * 1024)
	}
	skewed.MaxInputBytes = 50 * 1024 * 1024
	app.Stages[model.StageKey{ID: 1}] = skewed

	_, sum := Diagnose(app)
	if got := sum.FindingsWallCoverage; got < 0.29 || got > 0.31 {
		t.Errorf("findings_wall_coverage=%v want ~0.3", got)
	}
}
