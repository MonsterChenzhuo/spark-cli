package output

import (
	"fmt"
	"io"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/scenario"
)

func WriteMarkdown(w io.Writer, env scenario.Envelope) error {
	fmt.Fprintf(w, "## %s — `%s`\n\n", env.Scenario, env.AppID)
	fmt.Fprintf(w, "_log: `%s` · format: %s · compression: %s · events: %d · elapsed: %dms_\n\n",
		env.LogPath, env.LogFormat, env.Compression, env.ParsedEvents, env.ElapsedMs)
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
	renderMD(w, toStringSlice(env.Columns), toRowSlice(env.Data))
	return nil
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
