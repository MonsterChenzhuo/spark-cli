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

// TopIOBoundStage 描述"wall 占比大但 executor 不忙"的 stage —— 通常是任务在
// 大量 spill 落盘 / 等 shuffle / 网络阻塞,busy_ratio 因此被压得很低。
// 与 TopBusyStages(busy_ratio >= 0.8 的 CPU 热点)正交:看 spill 大头 stage
// 时只看 TopBusyStages 会全错过,这条数组就是把"占 wall 大但 IO 主导"的 stage
// 单独提出来,让 agent 直接盯到真正吃 wall 的 stage(如 spill 9.86 GB 但
// busy_ratio 仅 0.048 的场景)。
type TopIOBoundStage struct {
	StageID       int     `json:"stage_id"`
	Name          string  `json:"name"`
	DurationMs    int64   `json:"duration_ms"`
	BusyRatio     float64 `json:"busy_ratio"`
	SpillDiskGB   float64 `json:"spill_disk_gb"`
	ShuffleReadGB float64 `json:"shuffle_read_gb"`
	WallShare     float64 `json:"wall_share"`
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
	// TopIOBoundStages 与 TopBusyStages 正交,补充 spill / shuffle 主导的 stage。
	// 真实 ETL 经常出现 stage spill 9 GB+ 但 busy_ratio < 0.1(任务都在等盘),
	// 它们被 TopBusyStages 阈值过滤掉但才是真瓶颈。本字段按 wall_share 倒序
	// 输出 spill_disk_gb >= 0.5 或 shuffle_read_gb >= 1 的非忙 stage(busy_ratio < 0.8),
	// 让 agent 直接锁定"占 wall 大但 IO 主导"的优化候选。
	TopIOBoundStages []TopIOBoundStage `json:"top_io_bound_stages"`
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
		"top_io_bound_stages",
	}
}

// topBusyStagesMinRatio: busy_ratio 阈值,低于此值认为是 driver-side 等待
// stage(broadcast / planning / collect / listing 等),不应进入热点榜。
const topBusyStagesMinRatio = 0.8

// topBusyStagesMinWallShare: stage wall 占应用 wall 不到此阈值时,即使 busy_ratio
// 很高也不当"值得调优的真热点" —— 几秒级 stage 调好了对整体 wall 影响 < 1%,
// 把这种 stage 列进 top_busy_stages 反而误导 agent。`app.DurationMs == 0` 时
// wall_share 算 0,被视作"未知",此阈值不生效(避免没 ApplicationEnd 事件
// 的日志全线被排除)。
const topBusyStagesMinWallShare = 0.01

// topBusyStagesLimit: 输出前 N 个真热点。3 与 top_stages_by_duration 对齐,
// LLM / agent 一次扫到 3 个就够定方向。
const topBusyStagesLimit = 3

// IO-bound 阈值:一项满足即可入候选(spill 与 shuffle_read 各自的"算大头"门槛)。
// 设计意图:不让"几十 MB 的小 spill / 小 shuffle"挤掉真正 GB 级的 IO 热点。
const (
	topIOBoundMinSpillBytes       int64 = 512 * 1024 * 1024  // 0.5 GB
	topIOBoundMinShuffleReadBytes int64 = 1024 * 1024 * 1024 // 1 GB
	topIOBoundMaxBusyRatio              = 0.8                // 与 top_busy_stages 阈值刚好互补,不会双面命中
	topIOBoundLimit                     = 3
)

type stageDigest struct {
	id           int
	name         string
	dur          int64
	busy         float64
	spillBytes   int64
	shuffleBytes int64
	wallShare    float64
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

	all := collectStageDigests(app, &row)
	sort.Slice(all, func(i, j int) bool { return all[i].dur > all[j].dur })
	row.TopStagesByDuration = pickTopStagesByDuration(all)
	row.TopBusyStages = pickTopBusyStages(all)
	row.TopIOBoundStages = pickTopIOBoundStages(all)
	return row
}

// collectStageDigests 把 app.Stages 拍平成 stageDigest 切片,顺便累加
// row.StagesFailed / StagesSkipped(原始数据本来就是单次循环里完成的,
// 不抽出来 AppSummary 直接破 gocyclo 阈值)。
func collectStageDigests(app *model.Application, row *AppSummaryRow) []stageDigest {
	var all []stageDigest
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
		all = append(all, stageDigest{
			id:           s.ID,
			name:         s.Name,
			dur:          dur,
			busy:         stageBusyRatio(s, app.MaxConcurrentExecutors),
			spillBytes:   s.TotalSpillDisk,
			shuffleBytes: s.TotalShuffleReadBytes,
			wallShare:    dataSkewWallShare(s, app),
		})
	}
	return all
}

// pickTopStagesByDuration 取按 dur 倒序的前 3,包含 driver-side 等待 stage。
// 调用方应已先按 dur 倒序排好。
func pickTopStagesByDuration(all []stageDigest) []TopStage {
	n := 3
	if len(all) < n {
		n = len(all)
	}
	out := make([]TopStage, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, TopStage{
			StageID: all[i].id, Name: all[i].name, DurationMs: all[i].dur, BusyRatio: all[i].busy,
		})
	}
	return out
}

// pickTopBusyStages 过滤 + 按 busy*duration 倒序,与 by_duration 切面正交。
// wall_share 已知(>0)且 < 1% 的 stage 被排除 —— 即便 busy_ratio 高,几秒级
// stage 不是真正值得调优的 CPU 热点。wall_share == 0 视为未知,不触发过滤。
func pickTopBusyStages(all []stageDigest) []TopStage {
	busy := make([]stageDigest, 0, len(all))
	for _, s := range all {
		if s.busy < topBusyStagesMinRatio {
			continue
		}
		if s.wallShare > 0 && s.wallShare < topBusyStagesMinWallShare {
			continue
		}
		busy = append(busy, s)
	}
	sort.Slice(busy, func(i, j int) bool {
		return float64(busy[i].dur)*busy[i].busy > float64(busy[j].dur)*busy[j].busy
	})
	limit := topBusyStagesLimit
	if len(busy) < limit {
		limit = len(busy)
	}
	out := make([]TopStage, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, TopStage{
			StageID: busy[i].id, Name: busy[i].name, DurationMs: busy[i].dur, BusyRatio: busy[i].busy,
		})
	}
	return out
}

// pickTopIOBoundStages 取 busy_ratio < 0.8 但 spill / shuffle_read 一项达标的 stage,
// 按 wall_share 倒序输出。与 TopBusyStages 互补,锁定"占 wall 大但 IO 主导"的瓶颈。
func pickTopIOBoundStages(all []stageDigest) []TopIOBoundStage {
	ioBound := make([]stageDigest, 0, len(all))
	for _, s := range all {
		if s.dur <= 0 {
			continue
		}
		if s.busy >= topIOBoundMaxBusyRatio {
			continue
		}
		if s.spillBytes < topIOBoundMinSpillBytes && s.shuffleBytes < topIOBoundMinShuffleReadBytes {
			continue
		}
		ioBound = append(ioBound, s)
	}
	sort.Slice(ioBound, func(i, j int) bool {
		// wall_share 0 时(app.DurationMs==0)退化为按 dur 倒序,保持稳定排序
		if ioBound[i].wallShare != ioBound[j].wallShare {
			return ioBound[i].wallShare > ioBound[j].wallShare
		}
		return ioBound[i].dur > ioBound[j].dur
	})
	limit := topIOBoundLimit
	if len(ioBound) < limit {
		limit = len(ioBound)
	}
	out := make([]TopIOBoundStage, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, TopIOBoundStage{
			StageID:       ioBound[i].id,
			Name:          ioBound[i].name,
			DurationMs:    ioBound[i].dur,
			BusyRatio:     ioBound[i].busy,
			SpillDiskGB:   bytesToGB(ioBound[i].spillBytes),
			ShuffleReadGB: bytesToGB(ioBound[i].shuffleBytes),
			WallShare:     round3(ioBound[i].wallShare),
		})
	}
	return out
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
