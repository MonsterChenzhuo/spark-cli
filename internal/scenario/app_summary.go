package scenario

import (
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type TopStage struct {
	StageID    int     `json:"stage_id"`
	Name       string  `json:"name"`
	DurationMs int64   `json:"duration_ms"`
	BusyRatio  float64 `json:"busy_ratio"`
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
	// TopBusyStages 把 `top_stages_by_duration` 里 busy_ratio 接近 0 的 driver-side
	// 等待 stage 过滤掉,只保留 busy_ratio > 0.8 且按 busy_ratio*duration 倒序的
	// 真正 executor 热点 —— 这才是值得调并行 / 调 SQL plan 的优化候选。
	TopBusyStages []TopStage `json:"top_busy_stages"`
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
		"top_busy_stages",
	}
}

// topBusyStagesMinRatio: busy_ratio 阈值,低于此值认为是 driver-side 等待
// stage(broadcast / planning / collect / listing 等),不应进入热点榜。
const topBusyStagesMinRatio = 0.8

// topBusyStagesLimit: 输出前 N 个真热点。3 与 top_stages_by_duration 对齐,
// LLM / agent 一次扫到 3 个就够定方向。
const topBusyStagesLimit = 3

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
		busy float64
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
		all = append(all, sd{id: s.ID, name: s.Name, dur: dur, busy: stageBusyRatio(s, app.MaxConcurrentExecutors)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].dur > all[j].dur })
	n := 3
	if len(all) < n {
		n = len(all)
	}
	for i := 0; i < n; i++ {
		row.TopStagesByDuration = append(row.TopStagesByDuration, TopStage{
			StageID: all[i].id, Name: all[i].name, DurationMs: all[i].dur, BusyRatio: all[i].busy,
		})
	}

	// top_busy_stages: 过滤 + 按 busy*duration 倒序,与 by_duration 切面正交。
	busy := make([]sd, 0, len(all))
	for _, s := range all {
		if s.busy >= topBusyStagesMinRatio {
			busy = append(busy, s)
		}
	}
	sort.Slice(busy, func(i, j int) bool {
		return float64(busy[i].dur)*busy[i].busy > float64(busy[j].dur)*busy[j].busy
	})
	limit := topBusyStagesLimit
	if len(busy) < limit {
		limit = len(busy)
	}
	for i := 0; i < limit; i++ {
		row.TopBusyStages = append(row.TopBusyStages, TopStage{
			StageID: busy[i].id, Name: busy[i].name, DurationMs: busy[i].dur, BusyRatio: busy[i].busy,
		})
	}
	return row
}

// stageBusyRatio = TotalRunMs / (wall * effective_slots), clamped to [0,1].
// effective_slots = min(NumTasks, MaxConcurrentExecutors), matching IdleStageRule.
// Returns 0 when wall <= 0, slots <= 0, or TotalRunMs <= 0.
func stageBusyRatio(s *model.Stage, maxExec int) float64 {
	wall := s.CompleteMs - s.SubmitMs
	if wall <= 0 || s.TotalRunMs <= 0 {
		return 0
	}
	slots := int64(s.NumTasks)
	if lim := int64(maxExec); lim > 0 && lim < slots {
		slots = lim
	}
	if slots <= 0 {
		return 0
	}
	r := float64(s.TotalRunMs) / float64(wall*slots)
	if r > 1.0 {
		r = 1.0
	}
	return round3(r)
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
