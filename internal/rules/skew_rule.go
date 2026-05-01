package rules

import (
	"fmt"
	"math"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SkewRule struct{}

func (SkewRule) ID() string    { return "data_skew" }
func (SkewRule) Title() string { return "Data skew detected" }

func (SkewRule) Eval(app *model.Application) Finding {
	var bestF float64
	var bestStage *model.Stage
	var bestP50, bestP99 float64
	for _, s := range app.Stages {
		if s.Status != "succeeded" || int64(s.TaskDurations.Count()) < 50 {
			continue
		}
		p99 := s.TaskDurations.Quantile(0.99)
		if p99 < 1000 {
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
		f := math.Max(p99/median, float64(s.MaxInputBytes)/medianB)
		if f > bestF {
			bestF = f
			bestStage = s
			bestP50 = median
			bestP99 = p99
		}
	}
	if bestStage == nil || bestF < 4 {
		return okFinding(SkewRule{}.ID(), SkewRule{}.Title())
	}
	sev := "warn"
	if bestF >= 10 {
		sev = "critical"
	}
	evidence := map[string]any{
		"stage_id":    bestStage.ID,
		"skew_factor": round3(bestF),
		"p50_task_ms": int64(bestP50),
		"p99_task_ms": int64(bestP99),
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
