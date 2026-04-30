package scenario

import (
	"math"
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type DataSkewRow struct {
	StageID         int     `json:"stage_id"`
	Name            string  `json:"name"`
	Tasks           int64   `json:"tasks"`
	P50TaskMs       int64   `json:"p50_task_ms"`
	P99TaskMs       int64   `json:"p99_task_ms"`
	SkewFactor      float64 `json:"skew_factor"`
	MedianInputMB   float64 `json:"median_input_mb"`
	MaxInputMB      float64 `json:"max_input_mb"`
	InputSkewFactor float64 `json:"input_skew_factor"`
	Verdict         string  `json:"verdict"`
}

func DataSkewColumns() []string {
	return []string{
		"stage_id", "name", "tasks", "p50_task_ms", "p99_task_ms",
		"skew_factor", "median_input_mb", "max_input_mb",
		"input_skew_factor", "verdict",
	}
}

func DataSkew(app *model.Application, top int) []DataSkewRow {
	out := make([]DataSkewRow, 0)
	for _, s := range app.Stages {
		if s.Status != "succeeded" {
			continue
		}
		tasks := int64(s.TaskDurations.Count())
		if tasks < 50 {
			continue
		}
		p99 := s.TaskDurations.Quantile(0.99)
		if p99 < 1000 {
			continue
		}
		median := s.TaskDurations.Quantile(0.5)
		if median < 1 {
			median = 1
		}
		medianBytes := s.TaskInputBytes.Quantile(0.5)
		if medianBytes < 1 {
			medianBytes = 1
		}
		skew := p99 / median
		inputSkew := float64(s.MaxInputBytes) / medianBytes
		f := math.Max(skew, inputSkew)
		v := "mild"
		switch {
		case f >= 10:
			v = "severe"
		case f >= 4:
			v = "warn"
		}
		out = append(out, DataSkewRow{
			StageID:         s.ID,
			Name:            s.Name,
			Tasks:           tasks,
			P50TaskMs:       int64(median),
			P99TaskMs:       int64(p99),
			SkewFactor:      round3(skew),
			MedianInputMB:   bytesToMB(int64(medianBytes)),
			MaxInputMB:      bytesToMB(s.MaxInputBytes),
			InputSkewFactor: round3(inputSkew),
			Verdict:         v,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		fi := math.Max(out[i].SkewFactor, out[i].InputSkewFactor)
		fj := math.Max(out[j].SkewFactor, out[j].InputSkewFactor)
		return fi > fj
	})
	if top > 0 && len(out) > top {
		out = out[:top]
	}
	return out
}

const mb = 1024 * 1024

func bytesToMB(b int64) float64 { return round3(float64(b) / float64(mb)) }
