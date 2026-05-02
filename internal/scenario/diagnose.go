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
func computeFindingsWallCoverage(app *model.Application, findings []rules.Finding) float64 {
	if app.DurationMs <= 0 {
		return 0
	}
	perStage := make(map[int]float64)
	for _, f := range findings {
		if f.Severity == "ok" || f.Evidence == nil {
			continue
		}
		stageID, ok := stageIDFromEvidence(f.Evidence["stage_id"])
		if !ok {
			continue
		}
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
	var total float64
	for _, v := range perStage {
		total += v
	}
	return round3(total)
}

func collectTopFindingsByImpact(app *model.Application, findings []rules.Finding) []TopFinding {
	var ranked []TopFinding
	for _, f := range findings {
		if f.Severity == "ok" || f.Evidence == nil {
			continue
		}
		stageID, ok := stageIDFromEvidence(f.Evidence["stage_id"])
		if !ok {
			continue
		}
		s := findStageByID(app, stageID)
		if s == nil {
			continue
		}
		ws := dataSkewWallShare(s, app) // 复用 wall_share 公式(0 = 未知/不足以排名)
		if ws <= 0 {
			continue
		}
		ranked = append(ranked, TopFinding{
			RuleID:    f.RuleID,
			Severity:  f.Severity,
			WallShare: round3(ws),
		})
	}
	sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].WallShare > ranked[j].WallShare })
	return ranked
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
