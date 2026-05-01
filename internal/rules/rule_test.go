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
