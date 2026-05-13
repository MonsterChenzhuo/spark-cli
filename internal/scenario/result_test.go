package scenario

import (
	"encoding/json"
	"fmt"
	"strings"
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

// envelope 顶层 app_duration_ms 应当在为 0 时 omitempty 缺失;非 0 时保留。
// 让 agent 看 wall_share 时有绝对参照,不必再去 app-summary 拿 duration。
func TestEnvelopeOmitsAppDurationMsWhenZero(t *testing.T) {
	env := Envelope{Scenario: "diagnose", AppDurationMs: 0}
	b, _ := json.Marshal(env)
	if got := string(b); contains(got, "app_duration_ms") {
		t.Errorf("expected app_duration_ms omitted when 0, got %s", got)
	}
}

func TestEnvelopeIncludesAppDurationMsWhenNonZero(t *testing.T) {
	env := Envelope{Scenario: "diagnose", AppDurationMs: 4230802}
	b, _ := json.Marshal(env)
	if got := string(b); !contains(got, `"app_duration_ms":4230802`) {
		t.Errorf("expected app_duration_ms=4230802 present, got %s", got)
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

func TestEnvelopeIncludesYARNWhenPresent(t *testing.T) {
	env := Envelope{Scenario: "diagnose", YARN: map[string]any{"app": map[string]any{"state": "FAILED"}}}
	b, _ := json.Marshal(env)
	if got := string(b); !contains(got, `"yarn":`) || !contains(got, `"FAILED"`) {
		t.Errorf("expected yarn payload present, got %s", got)
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

	m := BuildSQLExecutionMap(app, "full", nil)

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
	if m := BuildSQLExecutionMap(app, "full", nil); m != nil {
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

	m := BuildSQLExecutionMap(app, "full", nil)
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
	if m := BuildSQLExecutionMap(app, "full", nil); m != nil {
		t.Errorf("80 个全噪音条目应让 map 整体 omit(返回 nil),实际 %d 项", len(m))
	}
}

// truncate 是默认行为(等价于不传 / 传空 / 传非法值),保护 envelope 不被几 KB
// 的 SQL 重复撑爆。truncate 模式应当输出前 sqlTruncateRunes 个 rune + 标记后缀。
func TestBuildSQLExecutionMapTruncatesByDefault(t *testing.T) {
	app := model.NewApplication()
	long := strings.Repeat("a", sqlTruncateRunes+200) // 700 个 rune,远超阈值
	app.SQLExecutions[5] = &model.SQLExecution{ID: 5, Description: long}
	app.StageToSQL[1] = 5

	for _, mode := range []string{"", "truncate", "garbage", "TRUNCATE"} {
		m := BuildSQLExecutionMap(app, mode, nil)
		got, ok := m[5]
		if !ok {
			t.Fatalf("mode=%q: id=5 missing", mode)
		}
		if !strings.HasSuffix(got, fmt.Sprintf("...(truncated, total %d chars)", len(long))) {
			tail := got
			if len(got) > 80 {
				tail = got[len(got)-80:]
			}
			t.Errorf("mode=%q: missing truncate suffix, tail=%q", mode, tail)
		}
		runeCount := len([]rune(got))
		if runeCount < sqlTruncateRunes || runeCount > sqlTruncateRunes+80 {
			t.Errorf("mode=%q: runeCount=%d want roughly sqlTruncateRunes(%d) + suffix",
				mode, runeCount, sqlTruncateRunes)
		}
	}
}

// truncate 不应破坏 UTF-8 多字节字符(中文 SQL 是真实场景),按 rune 切割是关键。
func TestBuildSQLExecutionMapTruncateRespectsUTF8(t *testing.T) {
	app := model.NewApplication()
	zh := strings.Repeat("中", sqlTruncateRunes+50)
	app.SQLExecutions[5] = &model.SQLExecution{ID: 5, Description: zh}
	app.StageToSQL[1] = 5
	m := BuildSQLExecutionMap(app, "truncate", nil)
	got := m[5]
	if strings.ContainsRune(got, '�') {
		t.Errorf("truncate broke UTF-8 (got replacement char in description)")
	}
}

// "none" 模式整段 sql_executions 走 omitempty 缺失,适合 envelope 极致瘦身的场景
// (agent 压根不需要 SQL 文本时)。
func TestBuildSQLExecutionMapNoneReturnsNil(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[5] = &model.SQLExecution{ID: 5, Description: "select * from t"}
	app.StageToSQL[1] = 5
	if m := BuildSQLExecutionMap(app, "none", nil); m != nil {
		t.Errorf("mode=none should return nil, got %v", m)
	}
}

// "full" 模式返回原始 SQL,与历史行为一致(供已经依赖完整 SQL 的程序消费方使用)。
func TestBuildSQLExecutionMapFullReturnsOriginal(t *testing.T) {
	app := model.NewApplication()
	body := strings.Repeat("a", sqlTruncateRunes+200)
	app.SQLExecutions[5] = &model.SQLExecution{ID: 5, Description: body}
	app.StageToSQL[1] = 5
	m := BuildSQLExecutionMap(app, "full", nil)
	if m[5] != body {
		t.Errorf("full mode should preserve original SQL")
	}
}

// onlyIDs 过滤:让 envelope.sql_executions 只包含 row 实际用到的 sql_id。
// 真实场景:slow-stages --top 5 截断后,本不该出现在 row 里的 SHOW DATABASES
// 之类 SQL 不应在 envelope 顶层冒出来让 agent 困惑。
func TestBuildSQLExecutionMapOnlyIDsFilters(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[2] = &model.SQLExecution{ID: 2, Description: "SHOW DATABASES"}
	app.SQLExecutions[5] = &model.SQLExecution{ID: 5, Description: "select * from t"}
	app.StageToSQL[10] = 2
	app.StageToSQL[11] = 5

	// 只取 id=5,id=2 不应出现
	m := BuildSQLExecutionMap(app, "full", map[int64]struct{}{5: {}})
	if _, ok := m[2]; ok {
		t.Errorf("id=2 should be filtered out (not in onlyIDs), got %v", m)
	}
	if m[5] != "select * from t" {
		t.Errorf("id=5 should be present, got %v", m)
	}

	// 空集合 → nil(没人用,整段 omit)
	if m := BuildSQLExecutionMap(app, "full", map[int64]struct{}{}); m != nil {
		t.Errorf("empty onlyIDs should yield nil, got %v", m)
	}

	// nil → 历史行为(都收录)
	full := BuildSQLExecutionMap(app, "full", nil)
	if _, ok := full[2]; !ok {
		t.Errorf("nil onlyIDs should keep id=2, got %v", full)
	}
}

func TestCollectSlowStageSQLIDs(t *testing.T) {
	rows := []SlowStageRow{
		{StageID: 1, SQLExecutionID: 5},
		{StageID: 2, SQLExecutionID: 5}, // dup
		{StageID: 3, SQLExecutionID: -1},
		{StageID: 4, SQLExecutionID: 7},
	}
	got := CollectSlowStageSQLIDs(rows)
	if _, ok := got[5]; !ok {
		t.Errorf("missing 5: %v", got)
	}
	if _, ok := got[7]; !ok {
		t.Errorf("missing 7: %v", got)
	}
	if _, ok := got[-1]; ok {
		t.Errorf("unexpected -1: %v", got)
	}
	if len(got) != 2 {
		t.Errorf("len=%d want 2 (5 + 7)", len(got))
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
