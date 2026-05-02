package scenario

import (
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SlowStageRow struct {
	StageID    int    `json:"stage_id"`
	Attempt    int    `json:"attempt"`
	Name       string `json:"name"`
	DurationMs int64  `json:"duration_ms"`
	// WallShare = stage.duration / app.duration,直接给 agent ROI 信号,免得
	// 再用 envelope.app_duration_ms 自己除。app.DurationMs == 0(没 ApplicationEnd
	// 事件)时为 0。
	WallShare      float64 `json:"wall_share"`
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
	BusyRatio      float64 `json:"busy_ratio"`
	// 三档 *_mb_per_task 让用户在不同侧的 stage 都能一眼看到分区粒度:
	//   - shuffle-read 端 stage:看 shuffle_read_mb_per_task,过粗 → spill / OOM
	//   - shuffle-write 端 stage(map / aggregate 写出):看 shuffle_write_mb_per_task
	//   - source 扫描 stage(parquet / orc):看 input_mb_per_task,过粗 → 单 task 过慢
	// num_tasks=0 时三个字段都为 0(避免误导)。
	InputMBPerTask        float64 `json:"input_mb_per_task"`
	ShuffleReadMBPerTask  float64 `json:"shuffle_read_mb_per_task"`
	ShuffleWriteMBPerTask float64 `json:"shuffle_write_mb_per_task"`
	SQLExecutionID        int64   `json:"sql_execution_id"`
}

func SlowStagesColumns() []string {
	return []string{
		"stage_id", "attempt", "name", "duration_ms", "wall_share", "tasks", "failed_tasks",
		"p50_task_ms", "p99_task_ms", "input_gb", "shuffle_read_gb",
		"shuffle_write_gb", "spill_disk_gb", "gc_ms", "gc_ratio",
		"busy_ratio",
		"input_mb_per_task", "shuffle_read_mb_per_task", "shuffle_write_mb_per_task",
		"sql_execution_id",
	}
}

func SlowStages(app *model.Application, top int) []SlowStageRow {
	rows := make([]SlowStageRow, 0, len(app.Stages))
	for _, s := range app.Stages {
		dur := s.CompleteMs - s.SubmitMs
		if dur < 0 {
			dur = 0
		}
		sqlID, _ := stageSQL(app, s.ID)
		rows = append(rows, SlowStageRow{
			StageID:               s.ID,
			Attempt:               s.Attempt,
			Name:                  s.Name,
			DurationMs:            dur,
			WallShare:             round3(dataSkewWallShare(s, app)),
			Tasks:                 int64(s.TaskDurations.Count()),
			FailedTasks:           int64(s.FailedTasks),
			P50TaskMs:             int64(s.TaskDurations.Quantile(0.5)),
			P99TaskMs:             int64(s.TaskDurations.Quantile(0.99)),
			InputGB:               bytesToGB(s.TotalInputBytes),
			ShuffleReadGB:         bytesToGB(s.TotalShuffleReadBytes),
			ShuffleWriteGB:        bytesToGB(s.TotalShuffleWriteBytes),
			SpillDiskGB:           bytesToGB(s.TotalSpillDisk),
			GCMs:                  s.TotalGCMs,
			GCRatio:               gcRatio(s.TotalGCMs, s.TotalRunMs),
			BusyRatio:             stageBusyRatio(s, app.MaxConcurrentExecutors),
			InputMBPerTask:        bytesPerTaskToMB(s.TotalInputBytes, s.NumTasks),
			ShuffleReadMBPerTask:  bytesPerTaskToMB(s.TotalShuffleReadBytes, s.NumTasks),
			ShuffleWriteMBPerTask: bytesPerTaskToMB(s.TotalShuffleWriteBytes, s.NumTasks),
			SQLExecutionID:        sqlID,
		})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].DurationMs > rows[j].DurationMs })
	if top > 0 && len(rows) > top {
		rows = rows[:top]
	}
	return rows
}

// bytesPerTaskToMB 把 stage 维度的总字节数除以 task 数转 MiB,守 0 除避免误导
// (NumTasks=0 的 stage 通常是 driver-side 占位 stage,任何 *_mb_per_task 都没意义)。
func bytesPerTaskToMB(total int64, numTasks int) float64 {
	if numTasks <= 0 {
		return 0
	}
	return round3(float64(total) / float64(numTasks) / float64(mb))
}

func gcRatio(gcMs, runMs int64) float64 {
	if runMs <= 0 {
		return 0
	}
	return round3(float64(gcMs) / float64(runMs))
}
