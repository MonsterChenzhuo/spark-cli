package scenario

import (
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

// Envelope is the canonical JSON shape returned by every scenario.
// Data is `any` because gc-pressure returns an object, others return arrays.
// Columns mirrors data: []string for arrays, map[string][]string for gc-pressure.
type Envelope struct {
	Scenario     string `json:"scenario"`
	AppID        string `json:"app_id"`
	AppName      string `json:"app_name"`
	LogPath      string `json:"log_path"`
	LogFormat    string `json:"log_format"`
	Compression  string `json:"compression"`
	Incomplete   bool   `json:"incomplete"`
	ParsedEvents int64  `json:"parsed_events"`
	ElapsedMs    int64  `json:"elapsed_ms"`
	Columns      any    `json:"columns"`
	Data         any    `json:"data"`
	Summary      any    `json:"summary,omitempty"`
}

type DiagnoseSummary struct {
	Critical int `json:"critical"`
	Warn     int `json:"warn"`
	OK       int `json:"ok"`
}

// stageSQL looks up the Spark SQL execution that owns the given stage.
// Returns (-1, "") when the stage is not part of any tracked SQL execution
// (jobs without `spark.sql.execution.id` in their JobStart properties).
//
// When SparkListenerSQLExecutionStart.description carries Spark's default
// callsite ("getCallSite at SQLExecution.scala:74") or is empty — the
// typical case for DataFrame API jobs — fall back to the first non-empty
// line of details, which usually points at the user's call site.
func stageSQL(app *model.Application, stageID int) (int64, string) {
	id, ok := app.StageToSQL[stageID]
	if !ok {
		return -1, ""
	}
	e, ok := app.SQLExecutions[id]
	if !ok {
		return id, ""
	}
	if isCallSiteDescription(e.Description) {
		if first := firstNonEmptyLine(e.Details); first != "" {
			return id, first
		}
	}
	return id, e.Description
}

func isCallSiteDescription(desc string) bool {
	return desc == "" || strings.HasPrefix(desc, "getCallSite ")
}

func firstNonEmptyLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		if t := strings.TrimSpace(line); t != "" {
			return t
		}
	}
	return ""
}
