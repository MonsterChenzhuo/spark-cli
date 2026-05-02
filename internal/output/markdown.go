package output

import (
	"fmt"
	"io"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/scenario"
)

func WriteMarkdown(w io.Writer, env scenario.Envelope) error {
	fmt.Fprintf(w, "## %s — `%s`\n\n", env.Scenario, env.AppID)
	fmt.Fprintf(w, "_log: `%s` · format: %s · compression: %s · events: %d · elapsed: %dms%s_\n\n",
		env.LogPath, env.LogFormat, env.Compression, env.ParsedEvents, env.ElapsedMs, formatAppDuration(env.AppDurationMs))
	if env.Scenario == "gc-pressure" {
		colMap := toMap(env.Columns)
		dataMap := toMap(env.Data)
		for _, seg := range []string{"by_stage", "by_executor"} {
			fmt.Fprintf(w, "### %s\n\n", seg)
			renderMD(w, toStringSlice(colMap[seg]), toRowSlice(dataMap[seg]))
			fmt.Fprintln(w)
		}
		return nil
	}
	cols := toStringSlice(env.Columns)
	rows := toRowSlice(env.Data)
	// app-summary 是 single-row 多列(含 nested 数组)场景,横向表格 25 列 +
	// nested JSON 单格几百字符宽,人类不可读。换成 key | value 纵向表格,
	// nested 字段单元格仍是 JSON 但每个字段独占一行,扫读时不会被超宽列推走。
	// 多 row 场景(slow-stages / data-skew / diagnose)仍然走横向表格 —— 横向
	// 适合"扫多个 stage / finding 选感兴趣的"的浏览动作。
	if len(rows) == 1 {
		renderMDKeyValue(w, cols, rows[0])
		return nil
	}
	renderMD(w, cols, rows)
	return nil
}

// formatAppDuration 把 envelope.app_duration_ms 渲染成 markdown / table 头部
// 可读字串。0(没 ApplicationEnd 事件)时返回空 — 头部行直接省略这部分。
func formatAppDuration(ms int64) string {
	if ms <= 0 {
		return ""
	}
	if ms < 60_000 {
		return fmt.Sprintf(" · app: %.1fs", float64(ms)/1000.0)
	}
	mins := float64(ms) / 60_000.0
	return fmt.Sprintf(" · app: %.1fmin", mins)
}

// renderMDKeyValue 把单 row 渲染成 "field | value" 两列纵向表格。nested
// array / map 仍用 inline JSON(stringify)塞 value 列,但每个字段独占一行,
// 视觉上比横向 25 列表格清爽得多。
func renderMDKeyValue(w io.Writer, cols []string, row map[string]any) {
	fmt.Fprintln(w, "| field | value |")
	fmt.Fprintln(w, "| --- | --- |")
	for _, c := range cols {
		fmt.Fprintln(w, "| "+c+" | "+stringify(row[c])+" |")
	}
}

func renderMD(w io.Writer, cols []string, rows []map[string]any) {
	if len(cols) == 0 {
		return
	}
	fmt.Fprintln(w, "| "+strings.Join(cols, " | ")+" |")
	seps := make([]string, len(cols))
	for i := range cols {
		seps[i] = "---"
	}
	fmt.Fprintln(w, "| "+strings.Join(seps, " | ")+" |")
	for _, row := range rows {
		cells := make([]string, len(cols))
		for i, c := range cols {
			cells[i] = stringify(row[c])
		}
		fmt.Fprintln(w, "| "+strings.Join(cells, " | ")+" |")
	}
}
