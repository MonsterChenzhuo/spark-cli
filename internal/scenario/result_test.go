package scenario

import (
	"encoding/json"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestEnvelopeMarshalsRequiredKeys(t *testing.T) {
	env := Envelope{
		Scenario:     "app-summary",
		AppID:        "application_1_1",
		AppName:      "etl",
		LogPath:      "file:///tmp/x",
		LogFormat:    "v1",
		Compression:  "none",
		Incomplete:   false,
		ParsedEvents: 10,
		ElapsedMs:    7,
		Columns:      []string{"app_id"},
		Data:         []any{map[string]any{"app_id": "application_1_1"}},
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, k := range []string{
		"scenario", "app_id", "app_name", "log_path", "log_format",
		"compression", "incomplete", "parsed_events", "elapsed_ms", "columns", "data",
	} {
		if _, ok := m[k]; !ok {
			t.Errorf("envelope missing key %q", k)
		}
	}
}

func TestEnvelopeOmitsSummaryWhenNil(t *testing.T) {
	env := Envelope{Scenario: "slow-stages"}
	b, _ := json.Marshal(env)
	if got := string(b); contains(got, "summary") {
		t.Errorf("expected summary omitted, got %s", got)
	}
}

func TestEnvelopeOmitsSQLExecutionsWhenNil(t *testing.T) {
	env := Envelope{Scenario: "slow-stages"}
	b, _ := json.Marshal(env)
	if got := string(b); contains(got, "sql_executions") {
		t.Errorf("expected sql_executions omitted, got %s", got)
	}
}

func TestBuildSQLExecutionMapDeduplicatesAndApplesFallback(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[5] = &model.SQLExecution{
		ID:          5,
		Description: "select * from t",
	}
	app.SQLExecutions[7] = &model.SQLExecution{
		ID:          7,
		Description: "getCallSite at SQLExecution.scala:74",
		Details:     "Execution: collect at MyJob.scala:42\n== Plan ==",
	}
	app.SQLExecutions[9] = &model.SQLExecution{ID: 9, Description: "", Details: ""}

	app.StageToSQL[1] = 5
	app.StageToSQL[2] = 5 // 重复指向同一个 SQL,map 应当去重
	app.StageToSQL[3] = 7
	app.StageToSQL[4] = 9 // 没有可用描述,应当跳过

	m := BuildSQLExecutionMap(app)

	if got := m[5]; got != "select * from t" {
		t.Errorf("id=5 desc=%q want %q", got, "select * from t")
	}
	if got := m[7]; got != "Execution: collect at MyJob.scala:42" {
		t.Errorf("id=7 desc=%q want details fallback first line", got)
	}
	if _, ok := m[9]; ok {
		t.Errorf("id=9 should be skipped (no description), got %q", m[9])
	}
	if len(m) != 2 {
		t.Errorf("map size=%d want 2 (id=5 + id=7)", len(m))
	}
}

func TestBuildSQLExecutionMapEmptyWhenNoLinks(t *testing.T) {
	app := model.NewApplication()
	if m := BuildSQLExecutionMap(app); m != nil {
		t.Errorf("expected nil map when no stages link to SQL, got %v", m)
	}
}

// 真实 DataFrame ETL 里 SparkContext.getCallSite(SparkContext.scala:2205) 形态
// 频繁出现在 description 里,常与 details 同样无意义。BuildSQLExecutionMap 应当
// 把这种"全噪音"条目当作"没有可用描述"过滤掉,避免 envelope 出现 80+ 行重复
// 字符串(本次诊断 sparkETL 应用就是这个症状)。
func TestBuildSQLExecutionMapDropsCallSiteOnlyEntries(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[10] = &model.SQLExecution{
		ID:          10,
		Description: "org.apache.spark.SparkContext.getCallSite(SparkContext.scala:2205)",
		Details:     "org.apache.spark.SparkContext.getCallSite(SparkContext.scala:2205)",
	}
	app.SQLExecutions[11] = &model.SQLExecution{
		ID:          11,
		Description: "getCallSite at SQLExecution.scala:74",
		Details:     "", // fallback 也拿不到
	}
	app.StageToSQL[1] = 10
	app.StageToSQL[2] = 11

	m := BuildSQLExecutionMap(app)
	if _, ok := m[10]; ok {
		t.Errorf("id=10 应被过滤(全是 callsite 噪音),实际 %q", m[10])
	}
	if _, ok := m[11]; ok {
		t.Errorf("id=11 应被过滤(callsite + 无 details),实际 %q", m[11])
	}
}

func TestBuildSQLExecutionMapNilWhenAllNoise(t *testing.T) {
	app := model.NewApplication()
	for i := int64(0); i < 80; i++ {
		app.SQLExecutions[i] = &model.SQLExecution{
			ID:          i,
			Description: "org.apache.spark.SparkContext.getCallSite(SparkContext.scala:2205)",
			Details:     "org.apache.spark.SparkContext.getCallSite(SparkContext.scala:2205)",
		}
		app.StageToSQL[int(i)] = i
	}
	if m := BuildSQLExecutionMap(app); m != nil {
		t.Errorf("80 个全噪音条目应让 map 整体 omit(返回 nil),实际 %d 项", len(m))
	}
}

func TestStageSQLPrefersRealDescription(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[5] = &model.SQLExecution{
		ID:          5,
		Description: "select count(*) from orders where dt = '2026-05-01'",
		Details:     "== Physical Plan ==\nHashAggregate ...\n",
	}
	app.StageToSQL[1] = 5

	id, desc := stageSQL(app, 1)
	if id != 5 || desc != "select count(*) from orders where dt = '2026-05-01'" {
		t.Errorf("want real SQL, got id=%d desc=%q", id, desc)
	}
}

func TestStageSQLFallsBackOnGetCallSiteDescription(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[7] = &model.SQLExecution{
		ID:          7,
		Description: "getCallSite at SQLExecution.scala:74",
		Details:     "Execution: collect at MyJob.scala:42\n== Parsed Logical Plan ==\n...",
	}
	app.StageToSQL[2] = 7

	id, desc := stageSQL(app, 2)
	if id != 7 {
		t.Errorf("id=%d want 7", id)
	}
	if desc != "Execution: collect at MyJob.scala:42" {
		t.Errorf("want details first line, got %q", desc)
	}
}

func TestStageSQLFallsBackOnEmptyDescription(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[3] = &model.SQLExecution{
		ID:          3,
		Description: "",
		Details:     "Execution: save at FileSink.scala:99",
	}
	app.StageToSQL[4] = 3

	id, desc := stageSQL(app, 4)
	if id != 3 || desc != "Execution: save at FileSink.scala:99" {
		t.Errorf("want details fallback, got id=%d desc=%q", id, desc)
	}
}

func TestStageSQLKeepsCallSiteWhenNoDetails(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[1] = &model.SQLExecution{
		ID:          1,
		Description: "getCallSite at SQLExecution.scala:74",
		Details:     "",
	}
	app.StageToSQL[9] = 1

	id, desc := stageSQL(app, 9)
	if id != 1 || desc != "getCallSite at SQLExecution.scala:74" {
		t.Errorf("want callsite preserved when no details, got id=%d desc=%q", id, desc)
	}
}

func TestStageSQLReturnsNegOneWhenNoLink(t *testing.T) {
	app := model.NewApplication()
	id, desc := stageSQL(app, 99)
	if id != -1 || desc != "" {
		t.Errorf("want (-1, \"\"), got id=%d desc=%q", id, desc)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
