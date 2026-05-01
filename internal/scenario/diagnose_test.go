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
}
