package scenario

import (
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
	"github.com/opay-bigdata/spark-cli/internal/rules"
)

func DiagnoseColumns() []string {
	return []string{"rule_id", "severity", "title", "evidence", "suggestion"}
}

func Diagnose(app *model.Application) ([]rules.Finding, DiagnoseSummary) {
	all := rules.All()
	out := make([]rules.Finding, 0, len(all))
	var sum DiagnoseSummary
	for _, r := range all {
		f := r.Eval(app)
		switch f.Severity {
		case "critical":
			sum.Critical++
		case "warn":
			sum.Warn++
		default:
			sum.OK++
		}
		out = append(out, f)
	}
	sum.TopFindingsByImpact = collectTopFindingsByImpact(app, out)
	sum.FindingsWallCoverage = computeFindingsWallCoverage(app, out)
	return out, sum
}

// computeFindingsWallCoverage 把"所有非 ok finding 关联的 stage 占应用 wall 的
// 比例"加和。同一 stage 出现多次取 max(避免规则间互相计数)。app.DurationMs
// <= 0(没 ApplicationEnd 事件)时返回 0,让顶层字段经 omitempty 缺失。
//
// 当 finding evidence 含 similar_stages(SkewRule 在多 stage 命中时输出)时,
// primary stage_id 与 similar_stages[*].stage_id 一并参与去重 sum,确保单条
// finding 覆盖多 stage 时不被低估。
func computeFindingsWallCoverage(app *model.Application, findings []rules.Finding) float64 {
	if app.DurationMs <= 0 {
		return 0
	}
	perStage := make(map[int]float64)
	for _, f := range findings {
		if f.Severity == "ok" || f.Evidence == nil {
			continue
		}
		for _, stageID := range evidenceStageIDs(f.Evidence) {
			s := findStageByID(app, stageID)
			if s == nil {
				continue
			}
			ws := dataSkewWallShare(s, app)
			if ws <= 0 {
				continue
			}
			if cur, ok := perStage[stageID]; !ok || ws > cur {
				perStage[stageID] = ws
			}
		}
	}
	var total float64
	for _, v := range perStage {
		total += v
	}
	return round3(total)
}

// collectTopFindingsByImpact 给每条 finding 算"代表性 wall_share"用于排序。
// 单 finding 覆盖多 stage 时(比如 SkewRule 同时报 stage 14/11/12),取 max
// wall_share 而非 sum:max 直观对应"本规则击中的最严重 stage 占整作业多少",
// sum 在并行 stage 时易超 1.0 让人迷惑;全局总覆盖留给 FindingsWallCoverage。
func collectTopFindingsByImpact(app *model.Application, findings []rules.Finding) []TopFinding {
	var ranked []TopFinding
	for _, f := range findings {
		if f.Severity == "ok" || f.Evidence == nil {
			continue
		}
		ids := evidenceStageIDs(f.Evidence)
		if len(ids) == 0 {
			continue
		}
		var maxWS float64
		for _, id := range ids {
			s := findStageByID(app, id)
			if s == nil {
				continue
			}
			ws := dataSkewWallShare(s, app)
			if ws > maxWS {
				maxWS = ws
			}
		}
		if maxWS <= 0 {
			continue
		}
		ranked = append(ranked, TopFinding{
			RuleID:    f.RuleID,
			Severity:  f.Severity,
			WallShare: round3(maxWS),
		})
	}
	sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].WallShare > ranked[j].WallShare })
	return ranked
}

// evidenceStageIDs 把 finding evidence 涉及的 stage_id 全部抽出来:primary 一个、
// similar_stages 里每条一个。SkewRule 在多 stage 命中时会输出 similar_stages,
// 其他规则只输出 primary —— 单 stage_id 走"primary 一个"分支即可。
func evidenceStageIDs(ev map[string]any) []int {
	if ev == nil {
		return nil
	}
	var out []int
	if id, ok := stageIDFromEvidence(ev["stage_id"]); ok {
		out = append(out, id)
	}
	if sims, ok := ev["similar_stages"]; ok {
		// SkewRule.Eval 直接构造 []map[string]any;diagnose 调用时还没经过
		// JSON marshal,所以仅匹配这种类型即可。如果未来 evidence 经过 JSON
		// round-trip 进来(例如缓存场景),这里再加 []any 分支。
		if list, ok := sims.([]map[string]any); ok {
			for _, m := range list {
				if id, ok := stageIDFromEvidence(m["stage_id"]); ok {
					out = append(out, id)
				}
			}
		}
	}
	return out
}

func stageIDFromEvidence(v any) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case int64:
		return int(x), true
	case float64:
		return int(x), true
	default:
		return 0, false
	}
}

func findStageByID(app *model.Application, id int) *model.Stage {
	for k, s := range app.Stages {
		if k.ID == id {
			return s
		}
	}
	return nil
}
