package rules

import (
	"strings"
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

func TestIdleStageIncludesDriverAndBroadcastConfig(t *testing.T) {
	app := model.NewApplication()
	app.MaxConcurrentExecutors = 7
	app.SparkConf["spark.driver.memory"] = "4G"
	app.SparkConf["spark.driver.memoryOverhead"] = "1024"
	app.SparkConf["spark.sql.autoBroadcastJoinThreshold"] = "10485760"
	app.SparkConf["spark.sql.broadcastTimeout"] = "-1"

	s := model.NewStage(12, 0, "broadcast wait", 1, 0)
	s.SubmitMs = 0
	s.CompleteMs = 546_350
	s.TotalRunMs = 224
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 12}] = s

	f := IdleStageRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical", f.Severity)
	}
	if f.Evidence["spark_driver_memory"] != "4G" {
		t.Fatalf("spark_driver_memory evidence=%v want 4G", f.Evidence["spark_driver_memory"])
	}
	if f.Evidence["spark_sql_auto_broadcast_join_threshold"] != "10485760" {
		t.Fatalf("auto broadcast threshold evidence=%v", f.Evidence["spark_sql_auto_broadcast_join_threshold"])
	}
	if f.Evidence["spark_sql_broadcast_timeout"] != "-1" {
		t.Fatalf("broadcast timeout evidence=%v want -1", f.Evidence["spark_sql_broadcast_timeout"])
	}
	if !strings.Contains(f.Suggestion, "spark.driver.memory=4G") {
		t.Fatalf("suggestion should mention current driver memory, got %q", f.Suggestion)
	}
	if !strings.Contains(f.Suggestion, "spark.sql.broadcastTimeout=-1") {
		t.Fatalf("suggestion should mention infinite broadcast timeout, got %q", f.Suggestion)
	}
}
