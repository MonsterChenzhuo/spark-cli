package scenarios

import (
	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
	"github.com/opay-bigdata/spark-cli/internal/model"
	"github.com/opay-bigdata/spark-cli/internal/scenario"
)

func buildScenarioBody(opts Options, app *model.Application, env *scenario.Envelope) error {
	switch opts.Scenario {
	case "app-summary":
		env.Columns = scenario.AppSummaryColumns()
		env.Data = []any{scenario.AppSummary(app)}
	case "slow-stages":
		env.Columns = scenario.SlowStagesColumns()
		rows := scenario.SlowStages(app, opts.Top)
		anyRows := make([]any, len(rows))
		for i, r := range rows {
			anyRows[i] = r
		}
		env.Data = anyRows
		env.SQLExecutions = scenario.BuildSQLExecutionMap(app, opts.SQLDetail)
	case "data-skew":
		env.Columns = scenario.DataSkewColumns()
		rows := scenario.DataSkew(app, opts.Top)
		anyRows := make([]any, len(rows))
		for i, r := range rows {
			anyRows[i] = r
		}
		env.Data = anyRows
		env.SQLExecutions = scenario.BuildSQLExecutionMap(app, opts.SQLDetail)
	case "gc-pressure":
		env.Columns = scenario.GCPressureColumns()
		rows := scenario.GCPressure(app, opts.Top)
		anyRows := make([]any, len(rows))
		for i, r := range rows {
			anyRows[i] = r
		}
		env.Data = anyRows
	case "diagnose":
		env.Columns = scenario.DiagnoseColumns()
		findings, sum := scenario.Diagnose(app)
		anyRows := make([]any, len(findings))
		for i, f := range findings {
			anyRows[i] = f
		}
		env.Data = anyRows
		env.Summary = sum
	default:
		return cerrors.New(cerrors.CodeInternal, "unknown scenario "+opts.Scenario, "")
	}
	return nil
}
