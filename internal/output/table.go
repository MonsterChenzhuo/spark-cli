package output

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/scenario"
)

func WriteTable(w io.Writer, env scenario.Envelope) error {
	fmt.Fprintf(w, "# %s  app=%s  events=%d  elapsed=%dms\n",
		env.Scenario, env.AppID, env.ParsedEvents, env.ElapsedMs)
	if env.Scenario == "gc-pressure" {
		return writeGCSegments(w, env)
	}
	cols := toStringSlice(env.Columns)
	rows := toRowSlice(env.Data)
	// app-summary 是 single-row 多列(含 nested 数组)场景:横向布局会变成
	// 1200+ 字符宽的一行,nested 字段被 stringify 成几百字符 inline JSON,
	// 终端完全不可读。改成 "key | value" 纵向输出 —— 与 markdown 模式一致。
	if len(rows) == 1 {
		renderKeyValue(w, cols, rows[0])
		return nil
	}
	return renderTable(w, cols, rows)
}

// renderKeyValue 把单 row 渲染成两列纵向输出:左列字段名,右列值。nested
// array / map 仍用 inline JSON(stringify)塞 value 列,但每个字段独占一行,
// 视觉上比 1200 字符宽的横向行清爽得多。
func renderKeyValue(w io.Writer, cols []string, row map[string]any) {
	keyWidth := len("field")
	for _, c := range cols {
		if len(c) > keyWidth {
			keyWidth = len(c)
		}
	}
	fmt.Fprintf(w, "%s  value\n", padRight("field", keyWidth))
	fmt.Fprintf(w, "%s  %s\n", strings.Repeat("-", keyWidth), strings.Repeat("-", 5))
	for _, c := range cols {
		fmt.Fprintf(w, "%s  %s\n", padRight(c, keyWidth), stringify(row[c]))
	}
}

func writeGCSegments(w io.Writer, env scenario.Envelope) error {
	colMap := toMap(env.Columns)
	dataMap := toMap(env.Data)
	for _, seg := range []string{"by_stage", "by_executor"} {
		fmt.Fprintf(w, "\n## %s\n", seg)
		cols := toStringSlice(colMap[seg])
		rows := toRowSlice(dataMap[seg])
		if err := renderTable(w, cols, rows); err != nil {
			return err
		}
	}
	return nil
}

func renderTable(w io.Writer, cols []string, rows []map[string]any) error {
	if len(cols) == 0 {
		return nil
	}
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c)
	}
	cells := make([][]string, len(rows))
	for ri, row := range rows {
		cells[ri] = make([]string, len(cols))
		for ci, c := range cols {
			s := stringify(row[c])
			cells[ri][ci] = s
			if len(s) > widths[ci] {
				widths[ci] = len(s)
			}
		}
	}
	writeRow(w, cols, widths)
	writeSep(w, widths)
	for _, row := range cells {
		writeRow(w, row, widths)
	}
	return nil
}

func writeRow(w io.Writer, cells []string, widths []int) {
	parts := make([]string, len(cells))
	for i, s := range cells {
		parts[i] = padRight(s, widths[i])
	}
	fmt.Fprintln(w, strings.Join(parts, "  "))
}

func writeSep(w io.Writer, widths []int) {
	parts := make([]string, len(widths))
	for i, n := range widths {
		parts[i] = strings.Repeat("-", n)
	}
	fmt.Fprintln(w, strings.Join(parts, "  "))
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}

func stringify(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		return fmt.Sprintf("%.3f", x)
	case map[string]any, []any:
		b, _ := json.Marshal(x)
		return string(b)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func toStringSlice(v any) []string {
	switch x := v.(type) {
	case []string:
		return x
	case []any:
		out := make([]string, len(x))
		for i, e := range x {
			out[i] = fmt.Sprint(e)
		}
		return out
	default:
		return nil
	}
}

// toRowSlice 把 envelope.Data 拍平成 []map[string]any 供 table / markdown 渲染。
//
// 历史 bug:dispatch.go 把 scenario row(rules.Finding / AppSummaryRow /
// SlowStageRow 等 struct)塞进 []any,但本函数旧实现只接受 map[string]any,
// 导致 struct row 全部被丢弃 —— `--format table` / `--format markdown` 输出
// 只剩表头无数据。修法:struct row 通过 JSON round-trip 转成 map[string]any
// (struct 上已经有完整 JSON tag,序列化后字段名与 envelope.Columns 直接对齐)。
func toRowSlice(v any) []map[string]any {
	switch x := v.(type) {
	case []any:
		out := make([]map[string]any, 0, len(x))
		for _, e := range x {
			if m, ok := e.(map[string]any); ok {
				out = append(out, m)
				continue
			}
			if m, ok := structToMap(e); ok {
				out = append(out, m)
			}
		}
		return out
	case []map[string]any:
		return x
	default:
		return nil
	}
}

// structToMap 通过 JSON round-trip 把 struct(或包含 struct 的 any)转成 map。
// 失败一律返回 false,table / markdown formatter 跳过这一行,不会让 CLI 失败。
func structToMap(v any) (map[string]any, bool) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, false
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, false
	}
	return m, true
}

func toMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}
