package rules

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestSkewRuleTriggers(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "x", 100, 0)
	for i := 0; i < 99; i++ {
		s.TaskDurations.Add(100)
	}
	s.TaskDurations.Add(20000)
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = s

	f := SkewRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical", f.Severity)
	}
	if f.Evidence == nil {
		t.Errorf("evidence missing")
	}
}

func TestSkewRuleDowngradesOnUniformInput(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "uniform-but-jittery", 100, 0)
	for i := 0; i < 95; i++ {
		s.TaskDurations.Add(100)
		s.TaskInputBytes.Add(1024 * 1024)
	}
	for i := 0; i < 5; i++ {
		s.TaskDurations.Add(1700) // p99/p50 = 17, below 20 cutoff
		s.TaskInputBytes.Add(1024 * 1024)
	}
	s.MaxInputBytes = 1024 * 1024 // input_skew_factor = 1.0
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = s

	f := SkewRule{}.Eval(app)
	if f.Severity != "warn" {
		t.Fatalf("severity=%s want warn (uniform input + moderate ratio should downgrade)", f.Severity)
	}
}

func TestSkewRuleStaysCriticalOnExtremeRatio(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "extreme", 100, 0)
	for i := 0; i < 95; i++ {
		s.TaskDurations.Add(100)
		s.TaskInputBytes.Add(1024 * 1024)
	}
	for i := 0; i < 5; i++ {
		s.TaskDurations.Add(2500) // p99/p50 = 25, above 20 cutoff
		s.TaskInputBytes.Add(1024 * 1024)
	}
	s.MaxInputBytes = 1024 * 1024
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = s

	f := SkewRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical (ratio>=20 should bypass uniform-input gate)", f.Severity)
	}
}

func TestSkewRuleSkipsIdleStageCandidate(t *testing.T) {
	app := model.NewApplication()
	app.MaxConcurrentExecutors = 10

	// idle stage with high skew_factor: wall=120s, sum(task_run)=53.5s,
	// slots=10 → busy_ratio≈0.045 (below idle threshold 0.2).
	idle := model.NewStage(1, 0, "idle-skewed", 100, 0)
	idle.SubmitMs = 0
	idle.CompleteMs = 120_000
	idle.TotalRunMs = 53_500
	idle.Status = "succeeded"
	for i := 0; i < 95; i++ {
		idle.TaskDurations.Add(300)
		idle.TaskInputBytes.Add(1024 * 1024)
	}
	for i := 0; i < 5; i++ {
		idle.TaskDurations.Add(5_000) // p99/p50 ≈ 16.7 — would normally win
		idle.TaskInputBytes.Add(1024 * 1024)
	}
	idle.MaxInputBytes = 1024 * 1024
	app.Stages[model.StageKey{ID: 1}] = idle

	// busy stage with smaller skew but real input skew: wall=60s, run=540s,
	// slots=10 → busy_ratio=0.9, well above idle threshold.
	busy := model.NewStage(2, 0, "busy-skewed", 100, 0)
	busy.SubmitMs = 0
	busy.CompleteMs = 60_000
	busy.TotalRunMs = 540_000
	busy.Status = "succeeded"
	for i := 0; i < 95; i++ {
		busy.TaskDurations.Add(5_000)
		busy.TaskInputBytes.Add(10 * 1024 * 1024)
	}
	for i := 0; i < 5; i++ {
		busy.TaskDurations.Add(60_000) // p99/p50 = 12
		busy.TaskInputBytes.Add(120 * 1024 * 1024)
	}
	busy.MaxInputBytes = 120 * 1024 * 1024 // input_skew_factor = 12
	app.Stages[model.StageKey{ID: 2}] = busy

	f := SkewRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical, evidence=%+v", f.Severity, f.Evidence)
	}
	if got, _ := f.Evidence["stage_id"].(int); got != 2 {
		t.Errorf("reported stage_id=%v want 2 (idle stage 1 should be suppressed)", got)
	}
}

func TestFailedTasksRuleQuiet(t *testing.T) {
	app := model.NewApplication()
	app.TasksTotal = 10000
	app.TasksFailed = 1
	f := FailedTasksRule{}.Eval(app)
	if f.Severity != "ok" {
		t.Errorf("severity=%s want ok", f.Severity)
	}
}

func TestFailedTasksRuleEscalatesOnRepeatedNodeBlacklist(t *testing.T) {
	app := model.NewApplication()
	app.TasksTotal = 10000
	app.TasksFailed = 100 // 1% — would be "warn" alone
	app.Blacklists = []model.BlacklistEvent{
		{Time: 1, Kind: "node", Target: "host-bad", StageID: 5, Failures: 4},
		{Time: 2, Kind: "node", Target: "host-bad", StageID: 7, Failures: 3},
		{Time: 3, Kind: "executor", Target: "exec-9", StageID: 7, Failures: 2},
	}
	f := FailedTasksRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Errorf("severity=%s want critical (concentrated node failures)", f.Severity)
	}
	if hosts, ok := f.Evidence["blacklisted_hosts"].([]string); !ok || len(hosts) == 0 || hosts[0] != "host-bad" {
		t.Errorf("blacklisted_hosts evidence missing/wrong: %v", f.Evidence)
	}
	if f.Evidence["blacklist_node_events"] != 2 {
		t.Errorf("blacklist_node_events = %v, want 2", f.Evidence["blacklist_node_events"])
	}
	if f.Evidence["blacklist_executor_events"] != 1 {
		t.Errorf("blacklist_executor_events = %v, want 1", f.Evidence["blacklist_executor_events"])
	}
}

func TestSpillRuleEmbedsSparkConf(t *testing.T) {
	app := model.NewApplication()
	app.SparkConf["spark.sql.shuffle.partitions"] = "200"
	app.SparkConf["spark.executor.memory"] = "8g"
	s := model.NewStage(0, 0, "spilly", 1, 0)
	s.TotalSpillDisk = 12 * 1024 * 1024 * 1024 // 12 GB → critical
	app.Stages[model.StageKey{ID: 0}] = s
	f := SpillRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical", f.Severity)
	}
	if f.Evidence["spark_sql_shuffle_partitions"] != "200" {
		t.Errorf("evidence missing shuffle.partitions: %+v", f.Evidence)
	}
	if f.Evidence["spark_executor_memory"] != "8g" {
		t.Errorf("evidence missing executor.memory: %+v", f.Evidence)
	}
}
