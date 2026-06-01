package rules

import (
	"fmt"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

// IdleStageRule flags stages whose wall-clock duration is much larger than
// the cumulative task run time across the slots that could have run them.
// Typical causes: driver-side broadcast collection, scheduler delay, single
// large task waiting on upstream dependencies.
type IdleStageRule struct{}

func (IdleStageRule) ID() string    { return "idle_stage" }
func (IdleStageRule) Title() string { return "Idle stage" }

func (IdleStageRule) Eval(app *model.Application) Finding {
	var (
		hot       *model.Stage
		hotRatio  float64
		hotWallMs int64
		hotSlots  int64
	)
	for _, s := range app.Stages {
		wall := s.CompleteMs - s.SubmitMs
		if wall < 30_000 {
			continue
		}
		if s.TotalRunMs <= 0 {
			continue
		}
		slots := int64(s.NumTasks)
		if lim := int64(app.MaxConcurrentExecutors); lim > 0 && lim < slots {
			slots = lim
		}
		if slots <= 0 {
			continue
		}
		ratio := float64(s.TotalRunMs) / float64(wall*slots)
		if hot == nil || ratio < hotRatio {
			hot = s
			hotRatio = ratio
			hotWallMs = wall
			hotSlots = slots
		}
	}
	if hot == nil || hotRatio >= 0.2 {
		return okFinding(IdleStageRule{}.ID(), IdleStageRule{}.Title())
	}
	sev := "warn"
	if hotWallMs >= 60_000 && hotRatio < 0.05 {
		sev = "critical"
	}
	evidence := map[string]any{
		"stage_id":        hot.ID,
		"wall_ms":         hotWallMs,
		"total_run_ms":    hot.TotalRunMs,
		"effective_slots": hotSlots,
		"busy_ratio":      round3(hotRatio),
		"num_tasks":       hot.NumTasks,
	}
	if ws := wallShare(hot, app); ws > 0 {
		evidence["wall_share"] = round3(ws)
	}
	driverMem := confValue(app, "spark.driver.memory")
	driverOverhead := confValue(app, "spark.driver.memoryOverhead")
	autoBroadcast := confValue(app, "spark.sql.autoBroadcastJoinThreshold")
	broadcastTimeout := confValue(app, "spark.sql.broadcastTimeout")
	if driverMem != "" {
		evidence["spark_driver_memory"] = driverMem
	}
	if driverOverhead != "" {
		evidence["spark_driver_memory_overhead"] = driverOverhead
	}
	if autoBroadcast != "" {
		evidence["spark_sql_auto_broadcast_join_threshold"] = autoBroadcast
	}
	if broadcastTimeout != "" {
		evidence["spark_sql_broadcast_timeout"] = broadcastTimeout
	}
	return Finding{
		RuleID:     IdleStageRule{}.ID(),
		Severity:   sev,
		Title:      IdleStageRule{}.Title(),
		Evidence:   evidence,
		Suggestion: idleStageSuggestion(hot.ID, hotWallMs, hotRatio, hotSlots, driverMem, autoBroadcast, broadcastTimeout),
	}
}

func idleStageSuggestion(stageID int, wallMs int64, busyRatio float64, slots int64, driverMem, autoBroadcast, broadcastTimeout string) string {
	parts := []string{
		fmt.Sprintf("stage %d wall-clock %.1fs 但 executor 实际工作占比仅 %.1f%%(%d 个有效 slot)",
			stageID, float64(wallMs)/1000.0, busyRatio*100, slots),
		"通常是 driver 端 broadcast/串行计算或调度等待,检查执行计划是否触发大表 broadcast 或同步 collect",
	}
	var conf []string
	if driverMem != "" {
		conf = append(conf, "spark.driver.memory="+driverMem)
	}
	if autoBroadcast != "" {
		conf = append(conf, "spark.sql.autoBroadcastJoinThreshold="+autoBroadcast)
	}
	if broadcastTimeout != "" {
		conf = append(conf, "spark.sql.broadcastTimeout="+broadcastTimeout)
	}
	if len(conf) > 0 {
		parts = append(parts, "当前关键配置: "+strings.Join(conf, ", "))
	}
	if broadcastTimeout == "-1" {
		parts = append(parts, "`spark.sql.broadcastTimeout=-1` 会让 broadcast 问题表现为长时间等待,建议改成有限值并用 `spark-conf` 一次查看完整配置")
	} else {
		parts = append(parts, "可用 `spark-conf` 一次查看完整 Spark 配置后再给参数建议")
	}
	return strings.Join(parts, "; ") + "。"
}
