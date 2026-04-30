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
		SkewRule{}, GCRule{}, SpillRule{}, FailedTasksRule{}, TinyTasksRule{},
	}
}

func okFinding(id, title string) Finding {
	return Finding{RuleID: id, Severity: "ok", Title: title}
}
