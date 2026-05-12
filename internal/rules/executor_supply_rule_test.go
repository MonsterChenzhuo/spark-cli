package rules

import (
	"strings"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestExecutorSupplyRuleWarnsWhenStaticExecutorsUnderAllocated(t *testing.T) {
	app := model.NewApplication()
	app.SparkConf["spark.dynamicAllocation.enabled"] = "false"
	app.SparkConf["spark.executor.instances"] = "50"
	app.SparkConf["spark.executor.cores"] = "4"
	app.SparkConf["spark.executor.memory"] = "4G"
	app.SparkConf["spark.executor.memoryOverhead"] = "1024"
	app.SparkConf["spark.yarn.queue"] = "root.bigdata"
	app.ExecutorsAdded = 5
	app.MaxConcurrentExecutors = 5

	f := ExecutorSupplyRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical", f.Severity)
	}
	if got := f.Evidence["configured_executor_instances"]; got != 50 {
		t.Errorf("configured_executor_instances=%v want 50", got)
	}
	if got := f.Evidence["max_concurrent_executors"]; got != 5 {
		t.Errorf("max_concurrent_executors=%v want 5", got)
	}
	if got := f.Evidence["executor_cores"]; got != "4" {
		t.Errorf("executor_cores=%v want 4", got)
	}
	if got := f.Evidence["executor_memory"]; got != "4G" {
		t.Errorf("executor_memory=%v want 4G", got)
	}
	if !strings.Contains(f.Suggestion, "EventLog") || !strings.Contains(f.Suggestion, "YARN") {
		t.Fatalf("suggestion should explain EventLog/YARN boundary, got %q", f.Suggestion)
	}
}

func TestExecutorSupplyRuleOKWhenDynamicAllocationEnabled(t *testing.T) {
	app := model.NewApplication()
	app.SparkConf["spark.dynamicAllocation.enabled"] = "true"
	app.SparkConf["spark.executor.instances"] = "50"
	app.ExecutorsAdded = 5
	app.MaxConcurrentExecutors = 5

	f := ExecutorSupplyRule{}.Eval(app)
	if f.Severity != "ok" {
		t.Fatalf("severity=%s want ok for dynamic allocation", f.Severity)
	}
}

func TestExecutorSupplyRuleOKWhenStaticRequestSatisfied(t *testing.T) {
	app := model.NewApplication()
	app.SparkConf["spark.dynamicAllocation.enabled"] = "false"
	app.SparkConf["spark.executor.instances"] = "10"
	app.ExecutorsAdded = 10
	app.MaxConcurrentExecutors = 10

	f := ExecutorSupplyRule{}.Eval(app)
	if f.Severity != "ok" {
		t.Fatalf("severity=%s want ok when request is satisfied", f.Severity)
	}
}
