package rules

import (
	"fmt"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type GCRule struct{}

func (GCRule) ID() string    { return "gc_pressure" }
func (GCRule) Title() string { return "GC pressure" }

func (GCRule) Eval(app *model.Application) Finding {
	var bestRatio float64
	var bestExec *model.Executor
	for _, e := range app.Executors {
		if e.TotalRunMs < 60000 {
			continue
		}
		r := float64(e.TotalGCMs) / float64(e.TotalRunMs)
		if r > bestRatio {
			bestRatio = r
			bestExec = e
		}
	}
	if bestExec == nil || bestRatio < 0.10 {
		return okFinding(GCRule{}.ID(), GCRule{}.Title())
	}
	sev := "warn"
	if bestRatio >= 0.20 {
		sev = "critical"
	}
	evidence := map[string]any{
		"executor_id":  bestExec.ID,
		"gc_ratio":     round3(bestRatio),
		"total_gc_ms":  bestExec.TotalGCMs,
		"total_run_ms": bestExec.TotalRunMs,
	}
	mem := confValue(app, "spark.executor.memory")
	memOverhead := confValue(app, "spark.executor.memoryOverhead")
	if mem != "" {
		evidence["spark_executor_memory"] = mem
	}
	if memOverhead != "" {
		evidence["spark_executor_memory_overhead"] = memOverhead
	}
	return Finding{
		RuleID:     GCRule{}.ID(),
		Severity:   sev,
		Title:      GCRule{}.Title(),
		Evidence:   evidence,
		Suggestion: gcSuggestion(bestExec.ID, bestRatio, mem),
	}
}

func gcSuggestion(executorID string, ratio float64, mem string) string {
	cur := ""
	if mem != "" {
		cur = fmt.Sprintf("(当前 spark.executor.memory=%s)", mem)
	}
	return fmt.Sprintf("executor %s GC 占比 %.1f%%，考虑增大 spark.executor.memory %s 或减小分区。",
		executorID, ratio*100, cur)
}
