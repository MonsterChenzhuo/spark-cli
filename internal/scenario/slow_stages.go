package scenario

import (
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SlowStageRow struct {
	StageID        int     `json:"stage_id"`
	Attempt        int     `json:"attempt"`
	Name           string  `json:"name"`
	DurationMs     int64   `json:"duration_ms"`
	Tasks          int64   `json:"tasks"`
	FailedTasks    int64   `json:"failed_tasks"`
	P50TaskMs      int64   `json:"p50_task_ms"`
	P99TaskMs      int64   `json:"p99_task_ms"`
	InputGB        float64 `json:"input_gb"`
	ShuffleReadGB  float64 `json:"shuffle_read_gb"`
	ShuffleWriteGB float64 `json:"shuffle_write_gb"`
	SpillDiskGB    float64 `json:"spill_disk_gb"`
	GCMs           int64   `json:"gc_ms"`
	GCRatio        float64 `json:"gc_ratio"`
	SQLExecutionID int64   `json:"sql_execution_id"`
	SQLDescription string  `json:"sql_description"`
}

func SlowStagesColumns() []string {
	return []string{
		"stage_id", "attempt", "name", "duration_ms", "tasks", "failed_tasks",
		"p50_task_ms", "p99_task_ms", "input_gb", "shuffle_read_gb",
		"shuffle_write_gb", "spill_disk_gb", "gc_ms", "gc_ratio",
		"sql_execution_id", "sql_description",
	}
}

func SlowStages(app *model.Application, top int) []SlowStageRow {
	rows := make([]SlowStageRow, 0, len(app.Stages))
	for _, s := range app.Stages {
		dur := s.CompleteMs - s.SubmitMs
		if dur < 0 {
			dur = 0
		}
		sqlID, sqlDesc := stageSQL(app, s.ID)
		rows = append(rows, SlowStageRow{
			StageID:        s.ID,
			Attempt:        s.Attempt,
			Name:           s.Name,
			DurationMs:     dur,
			Tasks:          int64(s.TaskDurations.Count()),
			FailedTasks:    int64(s.FailedTasks),
			P50TaskMs:      int64(s.TaskDurations.Quantile(0.5)),
			P99TaskMs:      int64(s.TaskDurations.Quantile(0.99)),
			InputGB:        bytesToGB(s.TotalInputBytes),
			ShuffleReadGB:  bytesToGB(s.TotalShuffleReadBytes),
			ShuffleWriteGB: bytesToGB(s.TotalShuffleWriteBytes),
			SpillDiskGB:    bytesToGB(s.TotalSpillDisk),
			GCMs:           s.TotalGCMs,
			GCRatio:        gcRatio(s.TotalGCMs, s.TotalRunMs),
			SQLExecutionID: sqlID,
			SQLDescription: sqlDesc,
		})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].DurationMs > rows[j].DurationMs })
	if top > 0 && len(rows) > top {
		rows = rows[:top]
	}
	return rows
}

func gcRatio(gcMs, runMs int64) float64 {
	if runMs <= 0 {
		return 0
	}
	return round3(float64(gcMs) / float64(runMs))
}
