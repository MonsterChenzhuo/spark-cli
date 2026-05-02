package scenario

import (
	"math"
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type DataSkewRow struct {
	StageID         int     `json:"stage_id"`
	Name            string  `json:"name"`
	Tasks           int64   `json:"tasks"`
	P50TaskMs       int64   `json:"p50_task_ms"`
	P99TaskMs       int64   `json:"p99_task_ms"`
	SkewFactor      float64 `json:"skew_factor"`
	MedianInputMB   float64 `json:"median_input_mb"`
	MaxInputMB      float64 `json:"max_input_mb"`
	InputSkewFactor float64 `json:"input_skew_factor"`
	WallShare       float64 `json:"wall_share"`
	Verdict         string  `json:"verdict"`
	SQLExecutionID  int64   `json:"sql_execution_id"`
}

func DataSkewColumns() []string {
	return []string{
		"stage_id", "name", "tasks", "p50_task_ms", "p99_task_ms",
		"skew_factor", "median_input_mb", "max_input_mb",
		"input_skew_factor", "wall_share", "verdict",
		"sql_execution_id",
	}
}

func DataSkew(app *model.Application, top int) []DataSkewRow {
	out := make([]DataSkewRow, 0)
	for _, s := range app.Stages {
		if s.Status != "succeeded" {
			continue
		}
		tasks := int64(s.TaskDurations.Count())
		if tasks < 50 {
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
		medianBytes := s.TaskInputBytes.Quantile(0.5)
		if medianBytes < 1 {
			medianBytes = 1
		}
		skew := p99 / median
		inputSkew := float64(s.MaxInputBytes) / medianBytes
		f := math.Max(skew, inputSkew)
		ws := dataSkewWallShare(s, app)
		v := skewVerdict(f, inputSkew, p99, median, ws)
		sqlID, _ := stageSQL(app, s.ID)
		out = append(out, DataSkewRow{
			StageID:         s.ID,
			Name:            s.Name,
			Tasks:           tasks,
			P50TaskMs:       int64(median),
			P99TaskMs:       int64(p99),
			SkewFactor:      round3(skew),
			MedianInputMB:   bytesToMB(int64(medianBytes)),
			MaxInputMB:      bytesToMB(s.MaxInputBytes),
			InputSkewFactor: round3(inputSkew),
			WallShare:       round3(ws),
			Verdict:         v,
			SQLExecutionID:  sqlID,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		fi := math.Max(out[i].SkewFactor, out[i].InputSkewFactor)
		fj := math.Max(out[j].SkewFactor, out[j].InputSkewFactor)
		return fi > fj
	})
	if top > 0 && len(out) > top {
		out = out[:top]
	}
	return out
}

const mb = 1024 * 1024

func bytesToMB(b int64) float64 { return round3(float64(b) / float64(mb)) }

const (
	dataSkewUniformInputThreshold = 1.2
	dataSkewExtremeRatioBypass    = 20.0
	// dataSkewNegligibleWallShare 镜像 rules.wallShareNegligible:
	// stage wall 占应用 wall 不到 1% 时,即使 skew_factor 极高也通常是
	// 短 stage 的尾抖动,优化收益接近 0。verdict 直接降到 mild,避免误导优先级。
	dataSkewNegligibleWallShare = 0.01
	// dataSkewTightTaskTimeRatio 镜像 rules.tightTaskTimeRatio:
	// 任务时长 P99/P50 < 1.5 → 时长高度均匀,不存在真正倾斜,verdict 强制 mild。
	dataSkewTightTaskTimeRatio = 1.5
)

// dataSkewWallShare mirrors rules.wallShare. Returns 0 (treated as "unknown,
// don't apply gate") when app duration or stage wall is missing or non-positive.
func dataSkewWallShare(s *model.Stage, app *model.Application) float64 {
	if app.DurationMs <= 0 {
		return 0
	}
	wall := s.CompleteMs - s.SubmitMs
	if wall <= 0 {
		return 0
	}
	return float64(wall) / float64(app.DurationMs)
}

// skewVerdict mirrors rules.skewSeverity but emits the verdict ladder used
// by DataSkew rows. 四道闸门(顺序依次应用):
//  1. 紧致任务时长(P99/P50 < 1.5)→ 直接降 mild,无视 input_skew_factor
//     (任务时长高度均匀就不存在真正倾斜;input_skew 巨大常是 min≈0 的伪影)
//  2. 均匀输入(input_skew_factor < 1.2)+ 中等 p99/p50(< 20)→ severe 降 warn
//  3. wall_share < 1% → 直接降 mild(短 stage 长尾不值得管,即使 ratio 极高)
//  4. extreme p99/p50 (>= 20) 仍保留 severe,不被任何闸门遮蔽
func skewVerdict(f, inputSkew, p99, median, wallShare float64) string {
	if f < 4 {
		return "mild"
	}
	if f < 10 {
		return "warn"
	}
	// 紧致任务时长闸门:median > 0 守 0 除,P99/P50 < 1.5 直接 mild
	if median > 0 && p99/median < dataSkewTightTaskTimeRatio {
		return "mild"
	}
	// wall_share 闸门:仅在 wall_share 已知(>0)且 ratio 不极端时降级到 mild
	if wallShare > 0 && wallShare < dataSkewNegligibleWallShare && (p99/median) < dataSkewExtremeRatioBypass {
		return "mild"
	}
	if inputSkew < dataSkewUniformInputThreshold && (p99/median) < dataSkewExtremeRatioBypass {
		return "warn"
	}
	return "severe"
}
