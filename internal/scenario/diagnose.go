package scenario

import (
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
	return out, sum
}
