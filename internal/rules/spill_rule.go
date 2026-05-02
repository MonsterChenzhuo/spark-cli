package rules

import (
	"fmt"
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SpillRule struct{}

func (SpillRule) ID() string    { return "disk_spill" }
func (SpillRule) Title() string { return "Disk spill" }

// spillCandidateGB:进入 SpillRule 候选的最低 spill 阈值(沿用历史 1 GB)。
const spillCandidateGB = 1.0

// targetPartitionMB:建议的合理 partition 粒度。AQE shuffle 默认 64m
// (spark.sql.adaptive.advisoryPartitionSizeInBytes)。我们用它来反推
// "应当让 partition 数从当前 N 调到 X"的具体数字建议。
const targetPartitionMB = 64.0

type spillCandidate struct {
	s             *model.Stage
	gb            float64
	estPartSizeMB float64
}

func (SpillRule) Eval(app *model.Application) Finding {
	var cands []spillCandidate
	for _, s := range app.Stages {
		gb := float64(s.TotalSpillDisk) / (1024 * 1024 * 1024)
		if gb < spillCandidateGB {
			continue
		}
		var part float64
		if s.NumTasks > 0 {
			part = round3(float64(s.TotalShuffleReadBytes) / float64(s.NumTasks) / (1024 * 1024))
		}
		cands = append(cands, spillCandidate{s: s, gb: round3(gb), estPartSizeMB: part})
	}
	if len(cands) == 0 {
		return okFinding(SpillRule{}.ID(), SpillRule{}.Title())
	}
	sort.SliceStable(cands, func(i, j int) bool { return cands[i].gb > cands[j].gb })
	primary := cands[0]

	sev := "warn"
	if primary.gb >= 10 {
		sev = "critical"
	}
	evidence := map[string]any{
		"stage_id":      primary.s.ID,
		"spill_disk_gb": primary.gb,
	}
	if primary.s.NumTasks > 0 {
		evidence["partitions"] = primary.s.NumTasks
		evidence["est_partition_size_mb"] = primary.estPartSizeMB
	}
	shufflePartitions := confValue(app, "spark.sql.shuffle.partitions")
	executorMem := confValue(app, "spark.executor.memory")
	if shufflePartitions != "" {
		evidence["spark_sql_shuffle_partitions"] = shufflePartitions
	}
	if executorMem != "" {
		evidence["spark_executor_memory"] = executorMem
	}

	// similar_stages:其余命中 spill 的 stage,按 spill_disk_gb 倒序最多 4 条。
	// agent 一眼看到"全应用 N 个 stage 都在 spill"而不必再跑 slow-stages 找补。
	if len(cands) > 1 {
		var similar []map[string]any
		for _, c := range cands[1:] {
			entry := map[string]any{
				"stage_id":      c.s.ID,
				"spill_disk_gb": c.gb,
			}
			if c.s.NumTasks > 0 {
				entry["partitions"] = c.s.NumTasks
				entry["est_partition_size_mb"] = c.estPartSizeMB
			}
			similar = append(similar, entry)
			if len(similar) >= similarStagesLimit {
				break
			}
		}
		if len(similar) > 0 {
			evidence["similar_stages"] = similar
		}
	}

	similarCount := 0
	if v, ok := evidence["similar_stages"].([]map[string]any); ok {
		similarCount = len(v)
	}
	return Finding{
		RuleID:     SpillRule{}.ID(),
		Severity:   sev,
		Title:      SpillRule{}.Title(),
		Evidence:   evidence,
		Suggestion: spillSuggestion(primary, shufflePartitions, executorMem, similarCount),
	}
}

// spillSuggestion 输出带数字的建议。当 est_partition_size_mb 已知且超过
// targetPartitionMB 时,直接算出"应当把 partition 数从 N 提到 X"的具体方案
// —— 这是 AQE coalescePartitions 把 partition 合得太激进的典型场景,改
// `spark.sql.adaptive.advisoryPartitionSizeInBytes` 比单纯改
// `spark.sql.shuffle.partitions` 更对症。
func spillSuggestion(c spillCandidate, shufflePartitions, executorMem string, similarCount int) string {
	multi := ""
	if similarCount > 0 {
		multi = fmt.Sprintf("(还命中 %d 个 stage 见 evidence.similar_stages)", similarCount)
	}
	if c.estPartSizeMB > 0 && c.estPartSizeMB > targetPartitionMB && c.s.NumTasks > 0 {
		targetParts := int(float64(c.s.NumTasks) * c.estPartSizeMB / targetPartitionMB)
		base := fmt.Sprintf(
			"stage %d 磁盘溢写 %.2f GB%s,单 partition ~%.0f MB(超合理 %.0f MB);AQE 合并 partition 太激进,建议设 `spark.sql.adaptive.advisoryPartitionSizeInBytes=%dm` 让 partition 从 %d 提到 ~%d",
			c.s.ID, c.gb, multi, c.estPartSizeMB, targetPartitionMB,
			int(targetPartitionMB), c.s.NumTasks, targetParts,
		)
		if executorMem != "" {
			base += fmt.Sprintf(",或加大 spark.executor.memory(当前 %s)", executorMem)
		}
		return base + "。"
	}
	cur := ""
	if shufflePartitions != "" {
		cur = fmt.Sprintf("(当前 spark.sql.shuffle.partitions=%s) ", shufflePartitions)
	}
	mem := ""
	if executorMem != "" {
		mem = fmt.Sprintf(",或加大 spark.executor.memory(当前 %s)", executorMem)
	}
	return fmt.Sprintf("stage %d 磁盘溢写 %.2f GB%s,考虑提高 spark.sql.shuffle.partitions %s%s。",
		c.s.ID, c.gb, multi, cur, mem)
}
