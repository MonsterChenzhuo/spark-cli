package rules

import (
	"fmt"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type FailedTasksRule struct{}

func (FailedTasksRule) ID() string    { return "failed_tasks" }
func (FailedTasksRule) Title() string { return "Failed tasks" }

func (FailedTasksRule) Eval(app *model.Application) Finding {
	if app.TasksTotal == 0 {
		return okFinding(FailedTasksRule{}.ID(), FailedTasksRule{}.Title())
	}
	ratio := float64(app.TasksFailed) / float64(app.TasksTotal)
	if ratio < 0.005 && len(app.Blacklists) == 0 {
		return okFinding(FailedTasksRule{}.ID(), FailedTasksRule{}.Title())
	}
	sev := "warn"
	if ratio >= 0.05 {
		sev = "critical"
	}
	nodeEvents, execEvents, hotHosts := summarizeBlacklists(app.Blacklists)
	if len(hotHosts) > 0 && sev == "warn" {
		// Concentrated node failures bump severity even at low overall ratio:
		// node-level pattern is qualitatively different from random task flakiness.
		sev = "critical"
	}
	evidence := map[string]any{
		"tasks_failed": app.TasksFailed,
		"tasks_total":  app.TasksTotal,
		"failure_rate": round3(ratio),
	}
	if nodeEvents > 0 || execEvents > 0 {
		evidence["blacklist_node_events"] = nodeEvents
		evidence["blacklist_executor_events"] = execEvents
	}
	if len(hotHosts) > 0 {
		evidence["blacklisted_hosts"] = hotHosts
	}
	return Finding{
		RuleID:     FailedTasksRule{}.ID(),
		Severity:   sev,
		Title:      FailedTasksRule{}.Title(),
		Evidence:   evidence,
		Suggestion: failedTasksSuggestion(app.TasksFailed, app.TasksTotal, ratio, nodeEvents, hotHosts),
	}
}

// summarizeBlacklists returns counts of node/executor events and hosts that
// were blacklisted/excluded multiple times — a strong signal that the failure
// is node-level rather than random task flakiness.
func summarizeBlacklists(events []model.BlacklistEvent) (nodeEvents, execEvents int, hotHosts []string) {
	hostCount := map[string]int{}
	for _, e := range events {
		switch e.Kind {
		case "node":
			nodeEvents++
			hostCount[e.Target]++
		case "executor":
			execEvents++
		}
	}
	for h, c := range hostCount {
		if c >= 2 {
			hotHosts = append(hotHosts, h)
		}
	}
	// Stable order for deterministic JSON output.
	for i := 0; i < len(hotHosts); i++ {
		for j := i + 1; j < len(hotHosts); j++ {
			if hotHosts[j] < hotHosts[i] {
				hotHosts[i], hotHosts[j] = hotHosts[j], hotHosts[i]
			}
		}
	}
	return nodeEvents, execEvents, hotHosts
}

func failedTasksSuggestion(failed, total int64, ratio float64, nodeEvents int, hotHosts []string) string {
	base := fmt.Sprintf("应用有 %.1f%% 任务失败 (%d/%d)", ratio*100, failed, total)
	if len(hotHosts) > 0 {
		return fmt.Sprintf("%s，集中出现在节点 %v 上 (%d 次 NodeBlacklisted/Excluded)，请优先排查这些节点的硬件/网络/磁盘故障，再查 driver 日志。",
			base, hotHosts, nodeEvents)
	}
	if nodeEvents > 0 {
		return fmt.Sprintf("%s，附带 %d 次 NodeBlacklisted/Excluded 事件，建议结合 driver 日志按节点维度排查。",
			base, nodeEvents)
	}
	return fmt.Sprintf("%s，检查 driver 日志中的 task failure reason。", base)
}
