package rules

import (
	"fmt"
	"sort"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type TinyTasksRule struct{}

func (TinyTasksRule) ID() string    { return "tiny_tasks" }
func (TinyTasksRule) Title() string { return "Tiny tasks" }

type tinyCandidate struct {
	s         *model.Stage
	tasks     int64
	p50       int64
	wallShare float64
	wallMs    int64
}

func (TinyTasksRule) Eval(app *model.Application) Finding {
	var cands []tinyCandidate
	for _, s := range app.Stages {
		tasks := int64(s.TaskDurations.Count())
		if tasks < 200 {
			continue
		}
		p50 := s.TaskDurations.Quantile(0.5)
		if p50 >= 100 {
			continue
		}
		wall := s.CompleteMs - s.SubmitMs
		if wall < 0 {
			wall = 0
		}
		cands = append(cands, tinyCandidate{
			s:         s,
			tasks:     tasks,
			p50:       int64(p50),
			wallShare: wallShare(s, app),
			wallMs:    wall,
		})
	}
	if len(cands) == 0 {
		return okFinding(TinyTasksRule{}.ID(), TinyTasksRule{}.Title())
	}
	// primary 按 wall_share 倒序(平局回退 tasks 多者),wall_share 全 0 时退化按 tasks。
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].wallShare != cands[j].wallShare {
			return cands[i].wallShare > cands[j].wallShare
		}
		return cands[i].tasks > cands[j].tasks
	})
	primary := cands[0]

	evidence := map[string]any{
		"stage_id":    primary.s.ID,
		"tasks":       primary.tasks,
		"p50_task_ms": primary.p50,
	}
	if primary.wallMs > 0 {
		evidence["wall_ms"] = primary.wallMs
	}
	if primary.wallShare > 0 {
		evidence["wall_share"] = round3(primary.wallShare)
	}

	// similar_stages:其余命中分区过细的 stage,按 wall_share 倒序最多 4 条
	if len(cands) > 1 {
		var similar []map[string]any
		for _, c := range cands[1:] {
			if c.wallShare <= 0 {
				continue
			}
			similar = append(similar, map[string]any{
				"stage_id":    c.s.ID,
				"tasks":       c.tasks,
				"p50_task_ms": c.p50,
				"wall_share":  round3(c.wallShare),
			})
			if len(similar) >= similarStagesLimit {
				break
			}
		}
		if len(similar) > 0 {
			evidence["similar_stages"] = similar
		}
	}

	similarCount := 0
	if v, ok := evidence["similar_stages"].([]map[string]any); ok {
		similarCount = len(v)
	}
	return Finding{
		RuleID:     TinyTasksRule{}.ID(),
		Severity:   "warn",
		Title:      TinyTasksRule{}.Title(),
		Evidence:   evidence,
		Suggestion: tinyTasksSuggestion(primary, similarCount),
	}
}

func tinyTasksSuggestion(c tinyCandidate, similarCount int) string {
	notes := []string{}
	if c.wallShare > 0 {
		notes = append(notes, fmt.Sprintf("wall_share %.1f%%", c.wallShare*100))
	}
	if similarCount > 0 {
		notes = append(notes, fmt.Sprintf("还命中 %d 个 stage 见 evidence.similar_stages", similarCount))
	}
	noteStr := ""
	if len(notes) > 0 {
		noteStr = "(" + strings.Join(notes, ";") + ")"
	}
	return fmt.Sprintf("stage %d 有 %d 个任务但 P50 仅 %dms%s,分区过细,考虑 coalesce/repartition 把分区降到 ~%d。",
		c.s.ID, c.tasks, c.p50, noteStr, suggestedTinyPartitions(c.tasks, c.p50))
}

// suggestedTinyPartitions 用经验规则:目标 task 时长 ~500ms,所以新分区数 ≈
// tasks * p50 / 500。给 agent / 用户一个具体可执行数字,而非"考虑 coalesce"。
// 下限 1,避免 0 分区。
func suggestedTinyPartitions(tasks, p50 int64) int64 {
	const targetTaskMs = 500
	target := tasks * p50 / targetTaskMs
	if target < 1 {
		target = 1
	}
	return target
}
