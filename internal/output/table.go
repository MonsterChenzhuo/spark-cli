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
	return renderTable(w, cols, rows)
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

func toRowSlice(v any) []map[string]any {
	switch x := v.(type) {
	case []any:
		out := make([]map[string]any, 0, len(x))
		for _, e := range x {
			if m, ok := e.(map[string]any); ok {
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

func toMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}
