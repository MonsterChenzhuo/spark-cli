package rules

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

// 1 个 task 真正只跑了 0.3s,但 stage 持续了 6 分钟 → critical。
func TestIdleStageCriticalWhenSingleTaskWaits(t *testing.T) {
	app := model.NewApplication()
	app.MaxConcurrentExecutors = 7
	s := model.NewStage(5, 0, "broadcast wait", 1, 0)
	s.SubmitMs = 0
	s.CompleteMs = 375_000
	s.TotalRunMs = 317
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 5}] = s

	f := IdleStageRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical, evidence=%+v", f.Severity, f.Evidence)
	}
	if f.Evidence["stage_id"].(int) != 5 {
		t.Errorf("stage_id=%v want 5", f.Evidence["stage_id"])
	}
}

// 满载运行的 stage 不应触发。
func TestIdleStageQuietWhenSlotsAreBusy(t *testing.T) {
	app := model.NewApplication()
	app.MaxConcurrentExecutors = 7
	s := model.NewStage(3, 0, "main compute", 101, 0)
	s.SubmitMs = 0
	s.CompleteMs = 474_000
	s.TotalRunMs = 101 * 27_000 // 平均 27s/任务,7 个 slot ≈ 满载
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 3}] = s

	f := IdleStageRule{}.Eval(app)
	if f.Severity != "ok" {
		t.Fatalf("severity=%s want ok, evidence=%+v", f.Severity, f.Evidence)
	}
}

// 短 stage(<30s) 不被纳入,避免噪音。
func TestIdleStageIgnoresShortStages(t *testing.T) {
	app := model.NewApplication()
	app.MaxConcurrentExecutors = 4
	s := model.NewStage(1, 0, "quick", 1, 0)
	s.SubmitMs = 0
	s.CompleteMs = 5_000
	s.TotalRunMs = 1
	app.Stages[model.StageKey{ID: 1}] = s

	if f := (IdleStageRule{}).Eval(app); f.Severity != "ok" {
		t.Errorf("short stage should be ok, got %s", f.Severity)
	}
}
