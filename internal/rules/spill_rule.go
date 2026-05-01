package rules

import (
	"fmt"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SpillRule struct{}

func (SpillRule) ID() string    { return "disk_spill" }
func (SpillRule) Title() string { return "Disk spill" }

func (SpillRule) Eval(app *model.Application) Finding {
	var maxSpill int64
	var hot *model.Stage
	for _, s := range app.Stages {
		if s.TotalSpillDisk > maxSpill {
			maxSpill = s.TotalSpillDisk
			hot = s
		}
	}
	gb := float64(maxSpill) / (1024 * 1024 * 1024)
	if hot == nil || gb < 1 {
		return okFinding(SpillRule{}.ID(), SpillRule{}.Title())
	}
	sev := "warn"
	if gb >= 10 {
		sev = "critical"
	}
	evidence := map[string]any{
		"stage_id":      hot.ID,
		"spill_disk_gb": round3(gb),
	}
	shufflePartitions := confValue(app, "spark.sql.shuffle.partitions")
	executorMem := confValue(app, "spark.executor.memory")
	if shufflePartitions != "" {
		evidence["spark_sql_shuffle_partitions"] = shufflePartitions
	}
	if executorMem != "" {
		evidence["spark_executor_memory"] = executorMem
	}
	return Finding{
		RuleID:     SpillRule{}.ID(),
		Severity:   sev,
		Title:      SpillRule{}.Title(),
		Evidence:   evidence,
		Suggestion: spillSuggestion(hot.ID, gb, shufflePartitions, executorMem),
	}
}

func spillSuggestion(stageID int, gb float64, shufflePartitions, executorMem string) string {
	cur := ""
	if shufflePartitions != "" {
		cur = fmt.Sprintf("(当前 spark.sql.shuffle.partitions=%s) ", shufflePartitions)
	}
	mem := ""
	if executorMem != "" {
		mem = fmt.Sprintf("，或加大 spark.executor.memory(当前 %s)", executorMem)
	}
	return fmt.Sprintf("stage %d 磁盘溢写 %.2f GB，考虑提高 spark.sql.shuffle.partitions %s%s。",
		stageID, gb, cur, mem)
}
