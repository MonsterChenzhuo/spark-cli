package rules

import (
	"fmt"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

// IdleStageRule flags stages whose wall-clock duration is much larger than
// the cumulative task run time across the slots that could have run them.
// Typical causes: driver-side broadcast collection, scheduler delay, single
// large task waiting on upstream dependencies.
type IdleStageRule struct{}

func (IdleStageRule) ID() string    { return "idle_stage" }
func (IdleStageRule) Title() string { return "Idle stage" }

func (IdleStageRule) Eval(app *model.Application) Finding {
	var (
		hot       *model.Stage
		hotRatio  float64
		hotWallMs int64
		hotSlots  int64
	)
	for _, s := range app.Stages {
		wall := s.CompleteMs - s.SubmitMs
		if wall < 30_000 {
			continue
		}
		if s.TotalRunMs <= 0 {
			continue
		}
		slots := int64(s.NumTasks)
		if lim := int64(app.MaxConcurrentExecutors); lim > 0 && lim < slots {
			slots = lim
		}
		if slots <= 0 {
			continue
		}
		ratio := float64(s.TotalRunMs) / float64(wall*slots)
		if hot == nil || ratio < hotRatio {
			hot = s
			hotRatio = ratio
			hotWallMs = wall
			hotSlots = slots
		}
	}
	if hot == nil || hotRatio >= 0.2 {
		return okFinding(IdleStageRule{}.ID(), IdleStageRule{}.Title())
	}
	sev := "warn"
	if hotWallMs >= 60_000 && hotRatio < 0.05 {
		sev = "critical"
	}
	evidence := map[string]any{
		"stage_id":        hot.ID,
		"wall_ms":         hotWallMs,
		"total_run_ms":    hot.TotalRunMs,
		"effective_slots": hotSlots,
		"busy_ratio":      round3(hotRatio),
		"num_tasks":       hot.NumTasks,
	}
	if ws := wallShare(hot, app); ws > 0 {
		evidence["wall_share"] = round3(ws)
	}
	return Finding{
		RuleID:   IdleStageRule{}.ID(),
		Severity: sev,
		Title:    IdleStageRule{}.Title(),
		Evidence: evidence,
		Suggestion: fmt.Sprintf("stage %d wall-clock %.1fs 但 executor 实际工作占比仅 %.1f%%(%d 个有效 slot),通常是 driver 端 broadcast/串行计算或调度等待,检查执行计划是否触发了大表 broadcast 或同步收集。",
			hot.ID, float64(hotWallMs)/1000.0, hotRatio*100, hotSlots),
	}
}
