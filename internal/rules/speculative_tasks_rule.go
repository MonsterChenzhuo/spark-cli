package rules

import (
	"fmt"
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SpeculativeTasksRule struct{}

func (SpeculativeTasksRule) ID() string    { return "speculative_tasks" }
func (SpeculativeTasksRule) Title() string { return "Speculative tasks" }

const (
	speculativeTasksMinWallShare = 0.05
	speculativeTasksMinCount     = 2
	speculativeTasksMinRatio     = 0.05
)

type speculativeCandidate struct {
	s     *model.Stage
	ratio float64
	wall  float64
}

func (SpeculativeTasksRule) Eval(app *model.Application) Finding {
	cands := make([]speculativeCandidate, 0)
	for _, s := range app.Stages {
		taskCount := int64(s.TaskDurations.Count())
		if s.SpeculativeTasks < speculativeTasksMinCount || taskCount <= 0 {
			continue
		}
		r := float64(s.SpeculativeTasks) / float64(taskCount)
		ws := wallShare(s, app)
		if r < speculativeTasksMinRatio || (ws > 0 && ws < speculativeTasksMinWallShare) {
			continue
		}
		cands = append(cands, speculativeCandidate{s: s, ratio: r, wall: ws})
	}
	if len(cands) == 0 {
		return okFinding(SpeculativeTasksRule{}.ID(), SpeculativeTasksRule{}.Title())
	}
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].wall != cands[j].wall {
			return cands[i].wall > cands[j].wall
		}
		return cands[i].ratio > cands[j].ratio
	})
	primary := cands[0]
	sev := "warn"
	if primary.ratio >= 0.2 && primary.wall >= 0.2 {
		sev = "critical"
	}
	evidence := map[string]any{
		"stage_id":          primary.s.ID,
		"speculative_tasks": primary.s.SpeculativeTasks,
		"speculative_ratio": round3(primary.ratio),
	}
	if primary.wall > 0 {
		evidence["wall_share"] = round3(primary.wall)
	}
	return Finding{
		RuleID:   SpeculativeTasksRule{}.ID(),
		Severity: sev,
		Title:    SpeculativeTasksRule{}.Title(),
		Evidence: evidence,
		Suggestion: fmt.Sprintf("stage %d speculative tasks 占比 %.1f%%,优先检查慢节点、数据倾斜、GC 抖动和 speculative 配置是否掩盖真实 straggler。",
			primary.s.ID, primary.ratio*100),
	}
}
