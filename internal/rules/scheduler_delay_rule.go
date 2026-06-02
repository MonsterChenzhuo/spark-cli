package rules

import (
	"fmt"
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SchedulerDelayRule struct{}

func (SchedulerDelayRule) ID() string    { return "scheduler_delay" }
func (SchedulerDelayRule) Title() string { return "Scheduler delay" }

const (
	schedulerDelayMinWallShare = 0.05
	schedulerDelayMinMs        = 30_000
	schedulerDelayMinRatio     = 0.25
)

type schedulerDelayCandidate struct {
	s     *model.Stage
	ratio float64
	wall  float64
}

func (SchedulerDelayRule) Eval(app *model.Application) Finding {
	cands := make([]schedulerDelayCandidate, 0)
	for _, s := range app.Stages {
		if s.TotalTaskDurationMs <= 0 || s.TotalSchedulerDelayMs < schedulerDelayMinMs {
			continue
		}
		r := float64(s.TotalSchedulerDelayMs) / float64(s.TotalTaskDurationMs)
		ws := wallShare(s, app)
		if r < schedulerDelayMinRatio || (ws > 0 && ws < schedulerDelayMinWallShare) {
			continue
		}
		cands = append(cands, schedulerDelayCandidate{s: s, ratio: r, wall: ws})
	}
	if len(cands) == 0 {
		return okFinding(SchedulerDelayRule{}.ID(), SchedulerDelayRule{}.Title())
	}
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].wall != cands[j].wall {
			return cands[i].wall > cands[j].wall
		}
		return cands[i].s.TotalSchedulerDelayMs > cands[j].s.TotalSchedulerDelayMs
	})
	primary := cands[0]
	sev := "warn"
	if primary.ratio >= 0.5 && primary.wall >= 0.2 {
		sev = "critical"
	}
	evidence := map[string]any{
		"stage_id":              primary.s.ID,
		"scheduler_delay_ms":    primary.s.TotalSchedulerDelayMs,
		"scheduler_delay_ratio": round3(primary.ratio),
	}
	if primary.wall > 0 {
		evidence["wall_share"] = round3(primary.wall)
	}
	return Finding{
		RuleID:   SchedulerDelayRule{}.ID(),
		Severity: sev,
		Title:    SchedulerDelayRule{}.Title(),
		Evidence: evidence,
		Suggestion: fmt.Sprintf("stage %d scheduler delay 占 task wall %.1f%%,优先检查 executor 供给、task locality、资源队列等待和过多小 task。",
			primary.s.ID, primary.ratio*100),
	}
}
