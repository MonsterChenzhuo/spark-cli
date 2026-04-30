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
	if ratio < 0.005 {
		return okFinding(FailedTasksRule{}.ID(), FailedTasksRule{}.Title())
	}
	sev := "warn"
	if ratio >= 0.05 {
		sev = "critical"
	}
	return Finding{
		RuleID:   FailedTasksRule{}.ID(),
		Severity: sev,
		Title:    FailedTasksRule{}.Title(),
		Evidence: map[string]any{
			"tasks_failed": app.TasksFailed,
			"tasks_total":  app.TasksTotal,
			"failure_rate": round3(ratio),
		},
		Suggestion: fmt.Sprintf("应用有 %.1f%% 任务失败 (%d/%d)，检查 driver 日志中的 task failure reason。",
			ratio*100, app.TasksFailed, app.TasksTotal),
	}
}
