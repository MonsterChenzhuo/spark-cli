package rules

import "github.com/opay-bigdata/spark-cli/internal/model"

type Finding struct {
	RuleID     string         `json:"rule_id"`
	Severity   string         `json:"severity"`
	Title      string         `json:"title"`
	Evidence   map[string]any `json:"evidence"`
	Suggestion string         `json:"suggestion"`
}

type Rule interface {
	ID() string
	Title() string
	Eval(app *model.Application) Finding
}

func All() []Rule {
	return []Rule{
		SkewRule{}, GCRule{}, SpillRule{}, FailedTasksRule{}, TinyTasksRule{}, IdleStageRule{},
	}
}

func okFinding(id, title string) Finding {
	return Finding{RuleID: id, Severity: "ok", Title: title}
}

// confValue returns the SparkConf value for key, empty string if absent.
// SparkConf is populated from SparkListenerEnvironmentUpdate's Spark Properties.
func confValue(app *model.Application, key string) string {
	if app.SparkConf == nil {
		return ""
	}
	return app.SparkConf[key]
}

// wallShareNegligible: stage 占应用 wall 不到此阈值时,即使指标显示问题严重,
// 优化它的整体收益也接近 0(短 stage 的尾抖动几乎没有 ROI)。当前用于 SkewRule
// 在 wall_share 已知(>0)时降级 critical → warn。
const wallShareNegligible = 0.01

// wallShare = (stage 的 wall-clock) / (app 的 wall-clock)。
// app.DurationMs 由 SparkListenerApplicationEnd 触发的 aggregator 写入,缺事件
// 时为 0;此处把 0 视为"未知,不应用闸门",避免没 ApplicationEnd 的日志全线降级。
func wallShare(s *model.Stage, app *model.Application) float64 {
	if app.DurationMs <= 0 {
		return 0
	}
	wall := s.CompleteMs - s.SubmitMs
	if wall <= 0 {
		return 0
	}
	return float64(wall) / float64(app.DurationMs)
}
