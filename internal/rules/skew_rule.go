package rules

import (
	"fmt"
	"math"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SkewRule struct{}

func (SkewRule) ID() string    { return "data_skew" }
func (SkewRule) Title() string { return "Data skew detected" }

// uniformInputThreshold: input_skew_factor below this is considered uniform
// distribution; combined with moderate p99/p50 ratio, the long tail is more
// likely jitter than real skew, so we downgrade critical to warn.
const uniformInputThreshold = 1.2

// extremeRatioBypass: p99/p50 ratios at or above this stay critical even on
// uniform input — extreme task-time spread is anomalous regardless of data.
const extremeRatioBypass = 20.0

func (SkewRule) Eval(app *model.Application) Finding {
	var bestF float64
	var bestStage *model.Stage
	var bestP50, bestP99, bestInputSkew float64
	for _, s := range app.Stages {
		if s.Status != "succeeded" || int64(s.TaskDurations.Count()) < 50 {
			continue
		}
		p99 := s.TaskDurations.Quantile(0.99)
		if p99 < 1000 {
			continue
		}
		// Suppress candidates that match idle_stage criteria — task-time spread
		// on idle stages is jitter, not skew.
		if isIdleStage(s, app) {
			continue
		}
		median := s.TaskDurations.Quantile(0.5)
		if median < 1 {
			median = 1
		}
		medianB := s.TaskInputBytes.Quantile(0.5)
		if medianB < 1 {
			medianB = 1
		}
		inputSkew := float64(s.MaxInputBytes) / medianB
		f := math.Max(p99/median, inputSkew)
		if f > bestF {
			bestF = f
			bestStage = s
			bestP50 = median
			bestP99 = p99
			bestInputSkew = inputSkew
		}
	}
	if bestStage == nil || bestF < 4 {
		return okFinding(SkewRule{}.ID(), SkewRule{}.Title())
	}
	sev := skewSeverity(bestF, bestInputSkew, bestP50, bestP99)
	evidence := map[string]any{
		"stage_id":          bestStage.ID,
		"skew_factor":       round3(bestF),
		"p50_task_ms":       int64(bestP50),
		"p99_task_ms":       int64(bestP99),
		"input_skew_factor": round3(bestInputSkew),
	}
	aqe := confValue(app, "spark.sql.adaptive.enabled")
	skewJoin := confValue(app, "spark.sql.adaptive.skewJoin.enabled")
	if aqe != "" {
		evidence["spark_sql_adaptive_enabled"] = aqe
	}
	if skewJoin != "" {
		evidence["spark_sql_adaptive_skewjoin_enabled"] = skewJoin
	}
	return Finding{
		RuleID:     SkewRule{}.ID(),
		Severity:   sev,
		Title:      SkewRule{}.Title(),
		Evidence:   evidence,
		Suggestion: skewSuggestion(bestStage.ID, int64(bestP50), int64(bestP99), aqe, skewJoin),
	}
}

// skewSeverity downgrades critical to warn when input is uniform AND the
// p99/p50 spread is moderate. Extreme spread (>= extremeRatioBypass) keeps
// critical regardless of input distribution.
func skewSeverity(f, inputSkew, p50, p99 float64) string {
	if f < 10 {
		return "warn"
	}
	if inputSkew < uniformInputThreshold && (p99/p50) < extremeRatioBypass {
		return "warn"
	}
	return "critical"
}

// isIdleStage mirrors IdleStageRule's wall+busy_ratio thresholds. Kept here
// rather than exported so the two rules can evolve independently if the
// idle definition changes.
func isIdleStage(s *model.Stage, app *model.Application) bool {
	wall := s.CompleteMs - s.SubmitMs
	if wall < 30_000 || s.TotalRunMs <= 0 {
		return false
	}
	slots := int64(s.NumTasks)
	if lim := int64(app.MaxConcurrentExecutors); lim > 0 && lim < slots {
		slots = lim
	}
	if slots <= 0 {
		return false
	}
	return float64(s.TotalRunMs)/float64(wall*slots) < 0.2
}

func skewSuggestion(stageID int, p50, p99 int64, aqe, skewJoin string) string {
	hint := "检查 join key 分布或开启 AQE skew join"
	if skewJoin == "true" {
		hint = "AQE skewJoin 已开启仍长尾，检查 join key 分布或调整 spark.sql.adaptive.skewJoin.skewedPartitionFactor"
	} else if skewJoin == "false" || (aqe != "" && aqe != "true") {
		hint = "建议启用 spark.sql.adaptive.enabled=true 与 spark.sql.adaptive.skewJoin.enabled=true，并复查 join key 分布"
	}
	return fmt.Sprintf("stage %d 任务长尾严重，median %dms / P99 %dms。%s。",
		stageID, p50, p99, hint)
}

func round3(f float64) float64 {
	x := f * 1000
	if x < 0 {
		x -= 0.5
	} else {
		x += 0.5
	}
	return float64(int64(x)) / 1000
}
