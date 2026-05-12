package rules

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

// ExecutorSupplyRule flags static executor requests that were not satisfied by
// the executors observed in the EventLog. The EventLog can prove the gap
// between Spark configuration and registered executors, but it does not carry
// the ResourceManager scheduler decision that explains why containers were not
// allocated.
type ExecutorSupplyRule struct{}

func (ExecutorSupplyRule) ID() string    { return "executor_supply" }
func (ExecutorSupplyRule) Title() string { return "Executor supply" }

func (ExecutorSupplyRule) Eval(app *model.Application) Finding {
	if strings.EqualFold(confValue(app, "spark.dynamicAllocation.enabled"), "true") {
		return okFinding(ExecutorSupplyRule{}.ID(), ExecutorSupplyRule{}.Title())
	}
	requested, ok := positiveIntConf(app, "spark.executor.instances")
	if !ok {
		return okFinding(ExecutorSupplyRule{}.ID(), ExecutorSupplyRule{}.Title())
	}
	actual := app.MaxConcurrentExecutors
	if app.ExecutorsAdded > actual {
		actual = app.ExecutorsAdded
	}
	if actual >= requested {
		return okFinding(ExecutorSupplyRule{}.ID(), ExecutorSupplyRule{}.Title())
	}
	missing := requested - actual
	if missing <= 0 {
		return okFinding(ExecutorSupplyRule{}.ID(), ExecutorSupplyRule{}.Title())
	}
	ratio := float64(actual) / float64(requested)
	sev := "warn"
	if requested >= 4 && ratio < 0.5 {
		sev = "critical"
	}
	evidence := map[string]any{
		"dynamic_allocation_enabled":    confValue(app, "spark.dynamicAllocation.enabled"),
		"configured_executor_instances": requested,
		"executors_added":               app.ExecutorsAdded,
		"max_concurrent_executors":      app.MaxConcurrentExecutors,
		"missing_executors":             missing,
		"satisfied_ratio":               round3(ratio),
	}
	addIfPresent(evidence, "executor_cores", confValue(app, "spark.executor.cores"))
	addIfPresent(evidence, "executor_memory", confValue(app, "spark.executor.memory"))
	addIfPresent(evidence, "executor_memory_overhead", confValue(app, "spark.executor.memoryOverhead"))
	addIfPresent(evidence, "yarn_queue", confValue(app, "spark.yarn.queue"))
	addIfPresent(evidence, "scheduler_min_registered_resources_ratio", confValue(app, "spark.scheduler.minRegisteredResourcesRatio"))
	addIfPresent(evidence, "scheduler_max_registered_resources_waiting_time", confValue(app, "spark.scheduler.maxRegisteredResourcesWaitingTime"))
	return Finding{
		RuleID:   ExecutorSupplyRule{}.ID(),
		Severity: sev,
		Title:    ExecutorSupplyRule{}.Title(),
		Evidence: evidence,
		Suggestion: fmt.Sprintf("Spark 静态配置请求 %d 个 executor,但 EventLog 只观察到最多 %d 个 executor 注册。EventLog 只能证明 executor 供给不足,不能证明 YARN ResourceManager 为什么未继续分配;请到 YARN RM/AM 日志检查 pending containers、user limit、队列容量、节点标签、reserved containers 和 container 资源碎片。",
			requested, actual),
	}
}

func positiveIntConf(app *model.Application, key string) (int, bool) {
	raw := strings.TrimSpace(confValue(app, key))
	if raw == "" {
		return 0, false
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

func addIfPresent(m map[string]any, key, value string) {
	if value != "" {
		m[key] = value
	}
}
