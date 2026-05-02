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

// SkewRule 现在在多 stage 命中时会塞 similar_stages,top_findings_by_impact 必须把
// primary + similar_stages 都纳入计算,取 max wall_share 作为本 finding 的影响代表。
// 历史 bug:SkewRule 只报一个 stage,导致 top_findings_by_impact 的 wall_share 严重低估。
func TestDiagnoseTopFindingMaxAcrossSimilarStages(t *testing.T) {
	app := model.NewApplication()
	app.DurationMs = 1_000_000

	// stage 1: wall_share 0.3,极端 ratio(进 similar_stages)
	s1 := model.NewStage(1, 0, "skew-low-wall", 100, 0)
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

	// stage 2: wall_share 0.7,中等 ratio(成为 primary)
	s2 := model.NewStage(2, 0, "skew-high-wall", 100, 0)
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

	_, sum := Diagnose(app)
	var tf TopFinding
	for _, x := range sum.TopFindingsByImpact {
		if x.RuleID == "data_skew" {
			tf = x
			break
		}
	}
	if tf.RuleID == "" {
		t.Fatalf("data_skew not in top_findings_by_impact: %+v", sum.TopFindingsByImpact)
	}
	if tf.WallShare < 0.69 || tf.WallShare > 0.71 {
		t.Errorf("data_skew wall_share=%v want ~0.7 (max across primary + similar_stages)", tf.WallShare)
	}
}

// findings_wall_coverage 必须把 SkewRule 的 similar_stages 一并纳入 perStage 去重 sum,
// 否则历史上 SkewRule 只报一个 stage,coverage 会严重低估。
func TestDiagnoseFindingsWallCoverageIncludesSimilarStages(t *testing.T) {
	app := model.NewApplication()
	app.DurationMs = 1_000_000

	// 两个 skew stage,wall_share 0.3 / 0.4(都进 SkewRule 的候选,primary 为后者)
	s1 := model.NewStage(1, 0, "skew-1", 100, 0)
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

	s2 := model.NewStage(2, 0, "skew-2", 100, 0)
	s2.SubmitMs = 0
	s2.CompleteMs = 400_000
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

	_, sum := Diagnose(app)
	// 0.3 + 0.4 = 0.7,容差 1pp
	if got := sum.FindingsWallCoverage; got < 0.69 || got > 0.71 {
		t.Errorf("findings_wall_coverage=%v want ~0.7 (0.3 + 0.4 across SkewRule similar_stages)", got)
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
