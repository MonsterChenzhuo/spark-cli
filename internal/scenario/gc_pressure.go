package scenario

import (
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type GCExecRow struct {
	ExecutorID string  `json:"executor_id"`
	Host       string  `json:"host"`
	Tasks      int64   `json:"tasks"`
	RunMs      int64   `json:"run_ms"`
	GCMs       int64   `json:"gc_ms"`
	GCRatio    float64 `json:"gc_ratio"`
	Verdict    string  `json:"verdict"`
}

func GCPressureColumns() []string {
	return []string{"executor_id", "host", "tasks", "run_ms", "gc_ms", "gc_ratio", "verdict"}
}

func GCPressure(app *model.Application, top int) []GCExecRow {
	out := make([]GCExecRow, 0, len(app.Executors))
	for _, e := range app.Executors {
		if e.TotalRunMs <= 0 {
			continue
		}
		ratio := float64(e.TotalGCMs) / float64(e.TotalRunMs)
		v := "ok"
		switch {
		case ratio >= 0.3:
			v = "severe"
		case ratio >= 0.15:
			v = "warn"
		}
		out = append(out, GCExecRow{
			ExecutorID: e.ID,
			Host:       e.Host,
			Tasks:      int64(e.TaskCount),
			RunMs:      e.TotalRunMs,
			GCMs:       e.TotalGCMs,
			GCRatio:    round3(ratio),
			Verdict:    v,
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].GCRatio > out[j].GCRatio })
	if top > 0 && len(out) > top {
		out = out[:top]
	}
	return out
}
