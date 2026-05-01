package scenario

import (
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type TopStage struct {
	StageID    int    `json:"stage_id"`
	Name       string `json:"name"`
	DurationMs int64  `json:"duration_ms"`
}

type AppSummaryRow struct {
	AppID                  string     `json:"app_id"`
	AppName                string     `json:"app_name"`
	User                   string     `json:"user"`
	DurationMs             int64      `json:"duration_ms"`
	ExecutorsAdded         int        `json:"executors_added"`
	ExecutorsRemoved       int        `json:"executors_removed"`
	MaxConcurrentExecutors int        `json:"max_concurrent_executors"`
	JobsTotal              int        `json:"jobs_total"`
	JobsFailed             int        `json:"jobs_failed"`
	StagesTotal            int        `json:"stages_total"`
	StagesFailed           int        `json:"stages_failed"`
	StagesSkipped          int        `json:"stages_skipped"`
	TasksTotal             int64      `json:"tasks_total"`
	TasksFailed            int64      `json:"tasks_failed"`
	TotalInputGB           float64    `json:"total_input_gb"`
	TotalOutputGB          float64    `json:"total_output_gb"`
	TotalShuffleReadGB     float64    `json:"total_shuffle_read_gb"`
	TotalShuffleWriteGB    float64    `json:"total_shuffle_write_gb"`
	TotalSpillDiskGB       float64    `json:"total_spill_disk_gb"`
	TotalGCMs              int64      `json:"total_gc_ms"`
	TotalRunMs             int64      `json:"total_run_ms"`
	GCRatio                float64    `json:"gc_ratio"`
	TopStagesByDuration    []TopStage `json:"top_stages_by_duration"`
}

func AppSummaryColumns() []string {
	return []string{
		"app_id", "app_name", "user", "duration_ms",
		"executors_added", "executors_removed", "max_concurrent_executors",
		"jobs_total", "jobs_failed",
		"stages_total", "stages_failed", "stages_skipped",
		"tasks_total", "tasks_failed",
		"total_input_gb", "total_output_gb",
		"total_shuffle_read_gb", "total_shuffle_write_gb",
		"total_spill_disk_gb",
		"total_gc_ms", "total_run_ms", "gc_ratio",
		"top_stages_by_duration",
	}
}

func AppSummary(app *model.Application) AppSummaryRow {
	row := AppSummaryRow{
		AppID:                  app.ID,
		AppName:                app.Name,
		User:                   app.User,
		ExecutorsAdded:         app.ExecutorsAdded,
		ExecutorsRemoved:       app.ExecutorsRemoved,
		MaxConcurrentExecutors: app.MaxConcurrentExecutors,
		JobsTotal:              app.JobsTotal,
		JobsFailed:             app.JobsFailed,
		StagesTotal:            len(app.Stages),
		TasksTotal:             app.TasksTotal,
		TasksFailed:            app.TasksFailed,
		TotalInputGB:           bytesToGB(app.TotalInputBytes),
		TotalOutputGB:          bytesToGB(app.TotalOutputBytes),
		TotalShuffleReadGB:     bytesToGB(app.TotalShuffleReadBytes),
		TotalShuffleWriteGB:    bytesToGB(app.TotalShuffleWriteBytes),
		TotalSpillDiskGB:       bytesToGB(app.TotalSpillDisk),
		TotalGCMs:              app.TotalGCMs,
		TotalRunMs:             app.TotalRunMs,
	}
	if app.EndMs > app.StartMs {
		row.DurationMs = app.EndMs - app.StartMs
	}
	if app.TotalRunMs > 0 {
		row.GCRatio = round3(float64(app.TotalGCMs) / float64(app.TotalRunMs))
	}

	type sd struct {
		id   int
		name string
		dur  int64
	}
	var all []sd
	for _, s := range app.Stages {
		switch s.Status {
		case "failed":
			row.StagesFailed++
		case "skipped":
			row.StagesSkipped++
		}
		dur := s.CompleteMs - s.SubmitMs
		if dur < 0 {
			dur = 0
		}
		all = append(all, sd{id: s.ID, name: s.Name, dur: dur})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].dur > all[j].dur })
	n := 3
	if len(all) < n {
		n = len(all)
	}
	for i := 0; i < n; i++ {
		row.TopStagesByDuration = append(row.TopStagesByDuration, TopStage{
			StageID: all[i].id, Name: all[i].name, DurationMs: all[i].dur,
		})
	}
	return row
}

const gb = 1024 * 1024 * 1024

func bytesToGB(b int64) float64 { return round3(float64(b) / float64(gb)) }

func round3(f float64) float64 {
	x := f * 1000
	if x < 0 {
		x -= 0.5
	} else {
		x += 0.5
	}
	return float64(int64(x)) / 1000
}
