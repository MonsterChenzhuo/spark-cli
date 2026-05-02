package rules

import (
	"fmt"
	"math"
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SkewRule struct{}

func (SkewRule) ID() string    { return "data_skew" }
func (SkewRule) Title() string { return "Data skew detected" }

// uniformInputThreshold: input_skew_factor below this is considered uniform
// distribution; combined with moderate p99/p50 ratio, the long tail is more
// likely jitter than real skew, so we downgrade critical to warn.
const uniformInputThreshold = 1.2

// extremeRatioBypass: p99/p50 ratios at or above this stay critical even on
// uniform input — extreme task-time spread is anomalous regardless of data.
const extremeRatioBypass = 20.0

// tightTaskTimeRatio: 任务时长 P99/P50 低于此值时,任务时长本身就高度均匀,
// 不能算倾斜。哪怕 input_skew_factor 看起来很大(往往是单个极小任务把 min
// 拉到 0 制造的数值伪影),都不应让用户花时间在这条线上。SkewRule 直接降到
// ok,DataSkew row 降到 mild。
const tightTaskTimeRatio = 1.5

// similarStagesLimit: evidence.similar_stages 最多收录的非 primary 候选数。
// 4 条够覆盖一次诊断里"同一规则击中多 stage"的常见场景,又不让 evidence 爆炸。
const similarStagesLimit = 4

type skewCandidate struct {
	s         *model.Stage
	f         float64
	p50       float64
	p99       float64
	inputSkew float64
	wallShare float64
}

func (SkewRule) Eval(app *model.Application) Finding {
	// 阶段 1:收集所有过 4 道闸门(succeeded、≥50 任务、p99 ≥ 1s、非 idle、f ≥ 4)的候选
	var cands []skewCandidate
	for _, s := range app.Stages {
		if s.Status != "succeeded" || int64(s.TaskDurations.Count()) < 50 {
			continue
		}
		p99 := s.TaskDurations.Quantile(0.99)
		if p99 < 1000 {
			continue
		}
		// Suppress candidates that match idle_stage criteria — task-time spread
		// on idle stages is jitter, not skew.
		if isIdleStage(s, app) {
			continue
		}
		median := s.TaskDurations.Quantile(0.5)
		if median < 1 {
			median = 1
		}
		medianB := s.TaskInputBytes.Quantile(0.5)
		if medianB < 1 {
			medianB = 1
		}
		inputSkew := float64(s.MaxInputBytes) / medianB
		f := math.Max(p99/median, inputSkew)
		if f < 4 {
			continue
		}
		cands = append(cands, skewCandidate{
			s:         s,
			f:         f,
			p50:       median,
			p99:       p99,
			inputSkew: inputSkew,
			wallShare: wallShare(s, app),
		})
	}
	if len(cands) == 0 {
		return okFinding(SkewRule{}.ID(), SkewRule{}.Title())
	}
	// 阶段 2:选 primary —— 优先 wall_share 最大(代表本规则最值得修的 stage),
	// 平局或全 0(app.DurationMs==0)时按 skew_factor 倒序。原本按 f 排序漏报
	// "wall 大但 ratio 不是最极端"的 stage,这里直接用业务意义最强的指标排。
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].wallShare != cands[j].wallShare {
			return cands[i].wallShare > cands[j].wallShare
		}
		return cands[i].f > cands[j].f
	})
	primary := cands[0]

	// 紧致闸门:primary 的任务时长 P99/P50 < 1.5 → 整体降为 ok。
	// 即使 input_skew_factor 很大(常见伪影:单个极小任务把 min 拉爆),也不消耗用户注意力。
	if primary.p50 > 0 && primary.p99/primary.p50 < tightTaskTimeRatio {
		return okFinding(SkewRule{}.ID(), SkewRule{}.Title())
	}

	sev := skewSeverity(primary.f, primary.inputSkew, primary.p50, primary.p99, primary.wallShare)
	evidence := map[string]any{
		"stage_id":          primary.s.ID,
		"skew_factor":       round3(primary.f),
		"p50_task_ms":       int64(primary.p50),
		"p99_task_ms":       int64(primary.p99),
		"input_skew_factor": round3(primary.inputSkew),
	}
	if primary.wallShare > 0 {
		evidence["wall_share"] = round3(primary.wallShare)
	}

	// similar_stages:其余候选,按 wall_share 倒序仅收 wall_share > 0 的(为 0 时
	// 排序无意义,放进去 agent 也用不上),最多 similarStagesLimit 条。
	// diagnose.collectTopFindingsByImpact / computeFindingsWallCoverage 会跨 primary +
	// similar_stages 计算本规则真实覆盖的 wall 占比,避免 SkewRule 历史上"只报一条"
	// 让 top_findings_by_impact 严重低估真实瓶颈(stage 7 wall_share 0.26 上榜,
	// stage 14 wall_share 0.92 直接看不到)。
	if len(cands) > 1 {
		var similar []map[string]any
		for _, c := range cands[1:] {
			if c.wallShare <= 0 {
				continue
			}
			similar = append(similar, map[string]any{
				"stage_id":    c.s.ID,
				"wall_share":  round3(c.wallShare),
				"skew_factor": round3(c.f),
			})
			if len(similar) >= similarStagesLimit {
				break
			}
		}
		if len(similar) > 0 {
			evidence["similar_stages"] = similar
		}
	}

	aqe := confValue(app, "spark.sql.adaptive.enabled")
	skewJoin := confValue(app, "spark.sql.adaptive.skewJoin.enabled")
	if aqe != "" {
		evidence["spark_sql_adaptive_enabled"] = aqe
	}
	if skewJoin != "" {
		evidence["spark_sql_adaptive_skewjoin_enabled"] = skewJoin
	}
	return Finding{
		RuleID:     SkewRule{}.ID(),
		Severity:   sev,
		Title:      SkewRule{}.Title(),
		Evidence:   evidence,
		Suggestion: skewSuggestion(primary.s.ID, int64(primary.p50), int64(primary.p99), aqe, skewJoin),
	}
}

// skewSeverity 三道闸门(顺序应用)用于把 critical 降级:
//  1. f < 10        → warn(普通长尾)
//  2. wall_share 已知(>0)且 < 1% 且 ratio 不极端  → warn(短 stage 长尾收益太低)
//  3. input 均匀 + ratio < 20  → warn(数据均匀的长尾通常是抖动)
//
// 极端 ratio (>= extremeRatioBypass) 不被任何闸门遮蔽,始终保留 critical。
// wall_share == 0 当作"未知"处理,不触发闸门 2,避免没 ApplicationEnd 事件的
// 日志全线被降级。
func skewSeverity(f, inputSkew, p50, p99, ws float64) string {
	if f < 10 {
		return "warn"
	}
	ratio := p99 / p50
	if ws > 0 && ws < wallShareNegligible && ratio < extremeRatioBypass {
		return "warn"
	}
	if inputSkew < uniformInputThreshold && ratio < extremeRatioBypass {
		return "warn"
	}
	return "critical"
}

// isIdleStage mirrors IdleStageRule's wall+busy_ratio thresholds. Kept here
// rather than exported so the two rules can evolve independently if the
// idle definition changes.
func isIdleStage(s *model.Stage, app *model.Application) bool {
	wall := s.CompleteMs - s.SubmitMs
	if wall < 30_000 || s.TotalRunMs <= 0 {
		return false
	}
	slots := int64(s.NumTasks)
	if lim := int64(app.MaxConcurrentExecutors); lim > 0 && lim < slots {
		slots = lim
	}
	if slots <= 0 {
		return false
	}
	return float64(s.TotalRunMs)/float64(wall*slots) < 0.2
}

func skewSuggestion(stageID int, p50, p99 int64, aqe, skewJoin string) string {
	hint := "检查 join key 分布或开启 AQE skew join"
	if skewJoin == "true" {
		hint = "AQE skewJoin 已开启仍长尾，检查 join key 分布或调整 spark.sql.adaptive.skewJoin.skewedPartitionFactor"
	} else if skewJoin == "false" || (aqe != "" && aqe != "true") {
		hint = "建议启用 spark.sql.adaptive.enabled=true 与 spark.sql.adaptive.skewJoin.enabled=true，并复查 join key 分布"
	}
	return fmt.Sprintf("stage %d 任务长尾严重，median %dms / P99 %dms。%s。",
		stageID, p50, p99, hint)
}

func round3(f float64) float64 {
	x := f * 1000
	if x < 0 {
		x -= 0.5
	} else {
		x += 0.5
	}
	return float64(int64(x)) / 1000
}
