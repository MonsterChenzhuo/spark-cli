package rules

import (
	"fmt"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type TinyTasksRule struct{}

func (TinyTasksRule) ID() string    { return "tiny_tasks" }
func (TinyTasksRule) Title() string { return "Tiny tasks" }

func (TinyTasksRule) Eval(app *model.Application) Finding {
	for _, s := range app.Stages {
		tasks := int64(s.TaskDurations.Count())
		if tasks < 200 {
			continue
		}
		p50 := s.TaskDurations.Quantile(0.5)
		if p50 >= 100 {
			continue
		}
		return Finding{
			RuleID:   TinyTasksRule{}.ID(),
			Severity: "warn",
			Title:    TinyTasksRule{}.Title(),
			Evidence: map[string]any{
				"stage_id":    s.ID,
				"tasks":       tasks,
				"p50_task_ms": int64(p50),
			},
			Suggestion: fmt.Sprintf("stage %d 有 %d 个任务但 P50 仅 %dms，分区过细，考虑 coalesce/repartition。",
				s.ID, tasks, int64(p50)),
		}
	}
	return okFinding(TinyTasksRule{}.ID(), TinyTasksRule{}.Title())
}
