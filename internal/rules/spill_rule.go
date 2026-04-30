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
	return Finding{
		RuleID:   SpillRule{}.ID(),
		Severity: sev,
		Title:    SpillRule{}.Title(),
		Evidence: map[string]any{
			"stage_id":      hot.ID,
			"spill_disk_gb": round3(gb),
		},
		Suggestion: fmt.Sprintf("stage %d 磁盘溢写 %.2f GB，考虑提高 spark.sql.shuffle.partitions 或增大 executor 内存。",
			hot.ID, gb),
	}
}
