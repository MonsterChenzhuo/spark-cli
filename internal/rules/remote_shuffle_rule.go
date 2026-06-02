package rules

import (
	"fmt"
	"sort"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type RemoteShuffleRule struct{}

func (RemoteShuffleRule) ID() string    { return "remote_shuffle" }
func (RemoteShuffleRule) Title() string { return "Remote shuffle" }

const (
	remoteShuffleMinWallShare = 0.05
	remoteShuffleMinBytes     = 1024 * 1024 * 1024
	remoteShuffleMinRatio     = 0.5
)

type remoteShuffleCandidate struct {
	s     *model.Stage
	ratio float64
	wall  float64
}

func (RemoteShuffleRule) Eval(app *model.Application) Finding {
	cands := make([]remoteShuffleCandidate, 0)
	for _, s := range app.Stages {
		if s.TotalShuffleReadBytes < remoteShuffleMinBytes || s.TotalShuffleReadBytes <= 0 {
			continue
		}
		r := float64(s.TotalShuffleRemoteReadBytes) / float64(s.TotalShuffleReadBytes)
		ws := wallShare(s, app)
		if r < remoteShuffleMinRatio || (ws > 0 && ws < remoteShuffleMinWallShare) {
			continue
		}
		cands = append(cands, remoteShuffleCandidate{s: s, ratio: r, wall: ws})
	}
	if len(cands) == 0 {
		return okFinding(RemoteShuffleRule{}.ID(), RemoteShuffleRule{}.Title())
	}
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].wall != cands[j].wall {
			return cands[i].wall > cands[j].wall
		}
		return cands[i].s.TotalShuffleRemoteReadBytes > cands[j].s.TotalShuffleRemoteReadBytes
	})
	primary := cands[0]
	sev := "warn"
	if primary.ratio >= 0.8 && primary.wall >= 0.2 {
		sev = "critical"
	}
	evidence := map[string]any{
		"stage_id":                  primary.s.ID,
		"shuffle_read_gb":           round3(float64(primary.s.TotalShuffleReadBytes) / 1024 / 1024 / 1024),
		"remote_shuffle_read_ratio": round3(primary.ratio),
	}
	if primary.wall > 0 {
		evidence["wall_share"] = round3(primary.wall)
	}
	return Finding{
		RuleID:   RemoteShuffleRule{}.ID(),
		Severity: sev,
		Title:    RemoteShuffleRule{}.Title(),
		Evidence: evidence,
		Suggestion: fmt.Sprintf("stage %d remote shuffle read 占比 %.1f%%,优先检查数据本地性、executor 丢失/重启、shuffle 分区布局和跨机架网络。",
			primary.s.ID, primary.ratio*100),
	}
}
