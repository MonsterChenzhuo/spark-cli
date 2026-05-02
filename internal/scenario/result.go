package scenario

import (
	"fmt"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

// SQL description 默认截断阈值。500 rune 大概 1.5 KB(英文)~ 1 KB(中文混合),
// 一次 SQL ETL 的核心意图通常前 500 字就够 agent 判读,不再拖累整个 envelope
// 体积。需要完整 SQL 时使用 --sql-detail=full。
const sqlTruncateRunes = 500

// SQLDetailModes 允许值;非法值由 normalize 落到 SQLDetailDefault。
const (
	SQLDetailFull     = "full"
	SQLDetailTruncate = "truncate"
	SQLDetailNone     = "none"
	SQLDetailDefault  = SQLDetailTruncate
)

// NormalizeSQLDetail 把空 / 未识别值规范化到默认 truncate。所有调用方都通过它
// 拿最终模式,免得每处自己判空。
func NormalizeSQLDetail(mode string) string {
	switch mode {
	case SQLDetailFull, SQLDetailTruncate, SQLDetailNone:
		return mode
	default:
		return SQLDetailDefault
	}
}

// truncateSQL 把 description 按 detail 模式裁剪。"full" 原样;"none" 返回空字符串
// (BuildSQLExecutionMap 配套整段 omit);其他(默认 truncate)取前 sqlTruncateRunes
// 个 rune,过长追加 "...(truncated, total N chars)" 让 agent 知道被截断了。
//
// 用 rune 而非 byte 切片,免得在中文 SQL 里把 UTF-8 多字节字符切坏。
func truncateSQL(s, mode string) string {
	switch NormalizeSQLDetail(mode) {
	case SQLDetailFull:
		return s
	case SQLDetailNone:
		return ""
	}
	runes := []rune(s)
	if len(runes) <= sqlTruncateRunes {
		return s
	}
	return string(runes[:sqlTruncateRunes]) + fmt.Sprintf("...(truncated, total %d chars)", len(runes))
}

// Envelope is the canonical JSON shape returned by every scenario.
// Data is `any` because gc-pressure returns an object, others return arrays.
// Columns mirrors data: []string for arrays, map[string][]string for gc-pressure.
//
// SQLExecutions is populated only by scenarios whose rows reference a
// `sql_execution_id` (slow-stages, data-skew). Rows store just the id; this
// top-level map provides description text once per execution to keep JSON
// payload small (production logs with multi-line SQL would otherwise repeat
// the same multi-KB string per row).
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
	// AppDurationMs 来自 SparkListenerApplicationEnd(没该事件时为 0,字段经
	// omitempty 缺失)。给 agent 一个绝对秒数参照,看 wall_share 时不必再去
	// app-summary 拿 duration_ms 才能换算。
	AppDurationMs int64            `json:"app_duration_ms,omitempty"`
	Columns       any              `json:"columns"`
	Data          any              `json:"data"`
	Summary       any              `json:"summary,omitempty"`
	SQLExecutions map[int64]string `json:"sql_executions,omitempty"`
}

type DiagnoseSummary struct {
	Critical            int          `json:"critical"`
	Warn                int          `json:"warn"`
	OK                  int          `json:"ok"`
	TopFindingsByImpact []TopFinding `json:"top_findings_by_impact,omitempty"`
	// FindingsWallCoverage 给出所有非 ok finding 涉及的 stage 占应用 wall 的总占比
	// (按 stage_id 去重,同一 stage 多个 finding 取最大 wall_share)。低于约 0.05
	// 时,几乎所有 wall 都不在 finding 范围内,瓶颈通常在作业结构 / 调度层 /
	// driver-side 开销而非 executor 内部 —— agent / 用户应当跳出 finding 列表去
	// 看 app-summary 的 top_busy_stages 或 stage 数量。
	//
	// 仅在 app.DurationMs > 0 时输出(omitempty);0 表示无法估算。
	FindingsWallCoverage float64 `json:"findings_wall_coverage,omitempty"`
}

// TopFinding 提供按 wall_share 倒序的 finding 摘要,让 agent / 用户不必自己
// 下钻 + 算耗时占比来定优先级。仅收录有 stage_id 关联且 wall_share > 0 的
// finding(其他 finding 没法估算耗时影响,统一不入榜)。
type TopFinding struct {
	RuleID    string  `json:"rule_id"`
	Severity  string  `json:"severity"`
	WallShare float64 `json:"wall_share"`
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

// isCallSiteDescription 识别 Spark 在 DataFrame API 路径上塞进 description /
// details 的 callsite 占位字符串。命中两种形态:
//   - 历史: "getCallSite at SQLExecution.scala:74"(SparkSQL 早期默认)
//   - 现代: "org.apache.spark.SparkContext.getCallSite(SparkContext.scala:2205)"
//     —— 用户交互式 / DataFrame 提交时 SparkContext 自己反射拿到的 callsite,
//     details 字段也经常带这串,导致 stageSQL fallback 拿到的还是噪音。
//
// stageSQL 据此触发 fallback;BuildSQLExecutionMap 据此过滤"fallback 后仍是
// 噪音"的条目,避免 envelope 出现一堆重复字符串。
func isCallSiteDescription(desc string) bool {
	if desc == "" {
		return true
	}
	if strings.HasPrefix(desc, "getCallSite ") {
		return true
	}
	if strings.Contains(desc, "SparkContext.getCallSite(") {
		return true
	}
	return false
}

func firstNonEmptyLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		if t := strings.TrimSpace(line); t != "" {
			return t
		}
	}
	return ""
}

// BuildSQLExecutionMap returns a {sql_execution_id → description} map covering
// every SQL execution referenced by at least one stage. Descriptions go through
// stageSQL's callsite-fallback (DataFrame jobs whose description is the default
// `getCallSite at SQLExecution.scala:74` get the first non-empty line of
// details instead). Executions with no usable description are omitted.
//
// 过滤规则:
//   - id < 0 / 空 desc → 跳过
//   - fallback 后的 desc 仍命中 isCallSiteDescription(description 与 details
//     都是 callsite 占位)→ 跳过,避免 envelope 出现一堆重复 callsite 文本
//
// Returns nil when no stage links to any SQL execution OR every linked execution
// only carries callsite noise — keeps the envelope `omitempty` field absent.
//
// detail 控制每条 description 的输出形态(见 SQLDetail* 常量):
//   - "none"    → 直接返回 nil(整段 sql_executions 走 omitempty 缺失)
//   - "truncate"(默认)→ 每条最多保留前 500 个 rune,过长追加 truncate 标记
//   - "full"    → 原样输出完整 SQL(可能是几 KB,适合人工排查不适合 agent)
//
// onlyIDs 控制哪些 SQL 进 map:nil 表示历史行为(全部 stage→sql 关联的 SQL 都
// 收录);非 nil 时只收录这个集合里的 id —— 给 dispatch 调用端用,免得
// `slow-stages --top 5` 截断后 envelope 里仍然冒出"SHOW DATABASES"等没在 row
// 里出现的 SQL,让 agent 困惑。
func BuildSQLExecutionMap(app *model.Application, detail string, onlyIDs map[int64]struct{}) map[int64]string {
	mode := NormalizeSQLDetail(detail)
	if mode == SQLDetailNone {
		return nil
	}
	if len(app.StageToSQL) == 0 {
		return nil
	}
	out := make(map[int64]string)
	seen := make(map[int64]struct{})
	for stageID := range app.StageToSQL {
		id, desc := stageSQL(app, stageID)
		if id < 0 || desc == "" || isCallSiteDescription(desc) {
			continue
		}
		if onlyIDs != nil {
			if _, ok := onlyIDs[id]; !ok {
				continue
			}
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out[id] = truncateSQL(desc, mode)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// CollectSlowStageSQLIDs / CollectDataSkewSQLIDs 从 row 切片提取出现的 sql_execution_id
// 集合(去掉 -1)。dispatch 用来过滤 BuildSQLExecutionMap 输出。
func CollectSlowStageSQLIDs(rows []SlowStageRow) map[int64]struct{} {
	out := make(map[int64]struct{}, len(rows))
	for _, r := range rows {
		if r.SQLExecutionID >= 0 {
			out[r.SQLExecutionID] = struct{}{}
		}
	}
	return out
}

func CollectDataSkewSQLIDs(rows []DataSkewRow) map[int64]struct{} {
	out := make(map[int64]struct{}, len(rows))
	for _, r := range rows {
		if r.SQLExecutionID >= 0 {
			out[r.SQLExecutionID] = struct{}{}
		}
	}
	return out
}
