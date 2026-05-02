package scenarios

import (
	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
	"github.com/opay-bigdata/spark-cli/internal/model"
	"github.com/opay-bigdata/spark-cli/internal/scenario"
)

// rowsToAny 把 []T(typed scenario row 切片)转成 []any 让 envelope.Data 接收。
// Go 1.22 generics 让 4 个 scenario 共用一个 helper,免得每处复制 3 行循环。
func rowsToAny[T any](rows []T) []any {
	out := make([]any, len(rows))
	for i, r := range rows {
		out[i] = r
	}
	return out
}

func buildScenarioBody(opts Options, app *model.Application, env *scenario.Envelope) error {
	switch opts.Scenario {
	case "app-summary":
		env.Columns = scenario.AppSummaryColumns()
		env.Data = []any{scenario.AppSummary(app)}
	case "slow-stages":
		rows := scenario.SlowStages(app, opts.Top)
		env.Columns = scenario.SlowStagesColumns()
		env.Data = rowsToAny(rows)
		env.SQLExecutions = scenario.BuildSQLExecutionMap(app, opts.SQLDetail, scenario.CollectSlowStageSQLIDs(rows))
	case "data-skew":
		rows := scenario.DataSkew(app, opts.Top)
		env.Columns = scenario.DataSkewColumns()
		env.Data = rowsToAny(rows)
		env.SQLExecutions = scenario.BuildSQLExecutionMap(app, opts.SQLDetail, scenario.CollectDataSkewSQLIDs(rows))
	case "gc-pressure":
		env.Columns = scenario.GCPressureColumns()
		env.Data = rowsToAny(scenario.GCPressure(app, opts.Top))
	case "diagnose":
		findings, sum := scenario.Diagnose(app)
		env.Columns = scenario.DiagnoseColumns()
		env.Data = rowsToAny(findings)
		env.Summary = sum
	default:
		return cerrors.New(cerrors.CodeInternal, "unknown scenario "+opts.Scenario, "")
	}
	return nil
}
