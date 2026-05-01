# spark-cli 诊断精度与可读性修复 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修四处影响 agent 决策质量的诊断输出问题:`sql_description` 对 DataFrame 作业失效、`data_skew` 在均匀输入 + idle stage 上误报 critical、`top_stages_by_duration` 不区分 idle/busy stage、`slow-stages` 行没有口径一致的 `gc_ratio`。

**Architecture:** 全部改动收敛在 `internal/scenario/*` 与 `internal/rules/*`,**不动** `internal/model` 字段类型/名称,因此 cache schema 不需要 bump。每条修复独立提交,独立带单元测试。最后一次性同步 SKILL.md / README / CHANGELOG / CLAUDE.md 并 push。

**Tech Stack:** Go 1.22 stdlib;现有 `github.com/opay-bigdata/spark-cli/internal/{model,rules,scenario,stats}` 包。

**Reference spec:** `docs/superpowers/specs/2026-05-02-spark-cli-diagnostic-fixes-design.md`

---

## File Structure

**Modified:**
- `internal/scenario/slow_stages.go` — `SlowStageRow` 加 `GCRatio`,`SlowStagesColumns()` 加列,计算 ratio
- `internal/scenario/slow_stages_test.go` — `TestSlowStagesGCRatio` + 零除边界
- `internal/scenario/result.go:32-41` — `stageSQL` 描述回退 details 首行
- `internal/scenario/result_test.go` — 4 例 fallback 行为(callsite + 真 SQL + 空 + details 多行)
- `internal/scenario/app_summary.go` — `TopStage` 加 `BusyRatio`,计算 + clamp [0,1]
- `internal/scenario/app_summary_test.go` — `TestAppSummaryTopStagesIncludeBusyRatio`
- `internal/rules/skew_rule.go` — 双闸门:输入均匀降级 + idle stage 跳过
- `internal/rules/rule_test.go` — 3 例:均匀降级、极端长尾仍 critical、idle 跳过
- `internal/scenario/data_skew.go` — verdict 闸门 A(同 SkewRule 口径)
- `internal/scenario/data_skew_test.go` — `TestDataSkewVerdictDowngradesOnUniformInput`
- `.claude/skills/spark/SKILL.md` — envelope 字段更新 + 三层 GC ratio 口径表
- `README.md` / `README.zh.md` — 输出示例字段更新
- `CHANGELOG.md` / `CHANGELOG.zh.md` — Unreleased 条目
- `CLAUDE.md` — 已知踩坑加 sql_description fallback 说明

---

## Task 1: `slow-stages` 行加 `gc_ratio`(口径 sum(task_gc) / sum(task_run))

**Files:**
- Modify: `internal/scenario/slow_stages.go`
- Modify: `internal/scenario/slow_stages_test.go`

- [ ] **Step 1: Write the failing test**

追加到 `internal/scenario/slow_stages_test.go`:

```go
func TestSlowStagesComputesGCRatio(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "stage", 10, 0)
	s.SubmitMs = 0
	s.CompleteMs = 1000
	s.Status = "succeeded"
	s.TotalRunMs = 1000
	s.TotalGCMs = 300
	for i := 0; i < 10; i++ {
		s.TaskDurations.Add(100)
	}
	app.Stages[model.StageKey{ID: 1}] = s

	rows := SlowStages(app, 0)
	if len(rows) != 1 {
		t.Fatalf("rows=%d", len(rows))
	}
	if rows[0].GCRatio < 0.299 || rows[0].GCRatio > 0.301 {
		t.Errorf("gc_ratio=%v want ~0.3", rows[0].GCRatio)
	}
}

func TestSlowStagesGCRatioZeroDivision(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "stage", 10, 0)
	s.SubmitMs = 0
	s.CompleteMs = 1000
	s.Status = "succeeded"
	s.TotalRunMs = 0 // pathological: stage produced no task metrics
	s.TotalGCMs = 50
	app.Stages[model.StageKey{ID: 1}] = s

	rows := SlowStages(app, 0)
	if len(rows) != 1 || rows[0].GCRatio != 0 {
		t.Fatalf("expect gc_ratio=0 on zero run_ms, got %+v", rows)
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

```bash
cd /Users/opay-20240095/IdeaProjects/createcli/spark-cli
go test ./internal/scenario/ -run TestSlowStagesComputesGCRatio -count=1
```

Expected: FAIL with `rows[0].GCRatio undefined`(struct 字段还没加)

- [ ] **Step 3: Add `GCRatio` field + columns + computation**

修改 `internal/scenario/slow_stages.go`:

把 `SlowStageRow` 的 `GCMs int64 \`json:"gc_ms"\`` 行下方加一行:
```go
	GCRatio        float64 `json:"gc_ratio"`
```

`SlowStagesColumns()` 列表里 `"gc_ms",` 后面紧跟 `"gc_ratio",`(在 `"sql_execution_id",` 之前)。

`SlowStages` 内 `GCMs: s.TotalGCMs,` 之后加:
```go
			GCRatio:        gcRatio(s.TotalGCMs, s.TotalRunMs),
```

文件末尾追加辅助:
```go
func gcRatio(gcMs, runMs int64) float64 {
	if runMs <= 0 {
		return 0
	}
	return round3(float64(gcMs) / float64(runMs))
}
```

- [ ] **Step 4: Run tests and verify pass**

```bash
go test ./internal/scenario/ -count=1
```

Expected: PASS(包括既有的 `TestSlowStagesAttachesSQLExecution`、`TestSlowStagesSortsByWallTimeDesc`)

- [ ] **Step 5: Commit**

```bash
git add internal/scenario/slow_stages.go internal/scenario/slow_stages_test.go
git commit -m "$(cat <<'EOF'
feat(scenario): slow-stages 行加 gc_ratio 字段

口径 = sum(task_gc) / sum(task_run),与 envelope 顶层 / gc-pressure 行一致。
此前用户拿 gc_ms / duration_ms 会得到 >100% 误读,因为 duration 是 wall。

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `stageSQL` 描述回退 details 首行

**Files:**
- Modify: `internal/scenario/result.go`
- Modify: `internal/scenario/result_test.go`

- [ ] **Step 1: Write the failing tests**

追加到 `internal/scenario/result_test.go`(新增 import `"github.com/opay-bigdata/spark-cli/internal/model"`):

```go
func TestStageSQLPrefersRealDescription(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[5] = &model.SQLExecution{
		ID:          5,
		Description: "select count(*) from orders where dt = '2026-05-01'",
		Details:     "== Physical Plan ==\nHashAggregate ...\n",
	}
	app.StageToSQL[1] = 5

	id, desc := stageSQL(app, 1)
	if id != 5 || desc != "select count(*) from orders where dt = '2026-05-01'" {
		t.Errorf("want real SQL, got id=%d desc=%q", id, desc)
	}
}

func TestStageSQLFallsBackOnGetCallSiteDescription(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[7] = &model.SQLExecution{
		ID:          7,
		Description: "getCallSite at SQLExecution.scala:74",
		Details:     "Execution: collect at MyJob.scala:42\n== Parsed Logical Plan ==\n...",
	}
	app.StageToSQL[2] = 7

	id, desc := stageSQL(app, 2)
	if id != 7 {
		t.Errorf("id=%d want 7", id)
	}
	if desc != "Execution: collect at MyJob.scala:42" {
		t.Errorf("want details first line, got %q", desc)
	}
}

func TestStageSQLFallsBackOnEmptyDescription(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[3] = &model.SQLExecution{
		ID:          3,
		Description: "",
		Details:     "Execution: save at FileSink.scala:99",
	}
	app.StageToSQL[4] = 3

	id, desc := stageSQL(app, 4)
	if id != 3 || desc != "Execution: save at FileSink.scala:99" {
		t.Errorf("want details fallback, got id=%d desc=%q", id, desc)
	}
}

func TestStageSQLKeepsCallSiteWhenNoDetails(t *testing.T) {
	app := model.NewApplication()
	app.SQLExecutions[1] = &model.SQLExecution{
		ID:          1,
		Description: "getCallSite at SQLExecution.scala:74",
		Details:     "",
	}
	app.StageToSQL[9] = 1

	id, desc := stageSQL(app, 9)
	if id != 1 || desc != "getCallSite at SQLExecution.scala:74" {
		t.Errorf("want callsite preserved when no details, got id=%d desc=%q", id, desc)
	}
}

func TestStageSQLReturnsNegOneWhenNoLink(t *testing.T) {
	app := model.NewApplication()
	id, desc := stageSQL(app, 99)
	if id != -1 || desc != "" {
		t.Errorf("want (-1, \"\"), got id=%d desc=%q", id, desc)
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

```bash
go test ./internal/scenario/ -run TestStageSQL -count=1 -v
```

Expected: 4 个新测试 FAIL(callsite 类用例返回 callsite 不返回 details)

- [ ] **Step 3: Implement fallback in `stageSQL`**

把 `internal/scenario/result.go` 全部替换为:

```go
package scenario

import (
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

// Envelope is the canonical JSON shape returned by every scenario.
// Data is `any` because gc-pressure returns an object, others return arrays.
// Columns mirrors data: []string for arrays, map[string][]string for gc-pressure.
type Envelope struct {
	Scenario     string `json:"scenario"`
	AppID        string `json:"app_id"`
	AppName      string `json:"app_name"`
	LogPath      string `json:"log_path"`
	LogFormat    string `json:"log_format"`
	Compression  string `json:"compression"`
	Incomplete   bool   `json:"incomplete"`
	ParsedEvents int64  `json:"parsed_events"`
	ElapsedMs    int64  `json:"elapsed_ms"`
	Columns      any    `json:"columns"`
	Data         any    `json:"data"`
	Summary      any    `json:"summary,omitempty"`
}

type DiagnoseSummary struct {
	Critical int `json:"critical"`
	Warn     int `json:"warn"`
	OK       int `json:"ok"`
}

// stageSQL looks up the Spark SQL execution that owns the given stage.
// Returns (-1, "") when the stage is not part of any tracked SQL execution
// (jobs without `spark.sql.execution.id` in their JobStart properties).
//
// When SparkListenerSQLExecutionStart.description carries Spark's default
// callsite ("getCallSite at SQLExecution.scala:74") or is empty — the
// typical case for DataFrame API jobs — fall back to the first non-empty
// line of details, which usually points at the user's call site.
func stageSQL(app *model.Application, stageID int) (int64, string) {
	id, ok := app.StageToSQL[stageID]
	if !ok {
		return -1, ""
	}
	e, ok := app.SQLExecutions[id]
	if !ok {
		return id, ""
	}
	if isCallSiteDescription(e.Description) {
		if first := firstNonEmptyLine(e.Details); first != "" {
			return id, first
		}
	}
	return id, e.Description
}

func isCallSiteDescription(desc string) bool {
	return desc == "" || strings.HasPrefix(desc, "getCallSite ")
}

func firstNonEmptyLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		if t := strings.TrimSpace(line); t != "" {
			return t
		}
	}
	return ""
}
```

- [ ] **Step 4: Run tests and verify pass**

```bash
go test ./internal/scenario/ -count=1
```

Expected: PASS(全部 result / slow_stages / data_skew / app_summary 测试)

- [ ] **Step 5: Commit**

```bash
git add internal/scenario/result.go internal/scenario/result_test.go
git commit -m "$(cat <<'EOF'
feat(scenario): stageSQL 在 callsite description 时回退 details 首行

Spark DataFrame API 提交的 SQLExecutionStart.description 默认是
"getCallSite at SQLExecution.scala:74",对 agent 没有信息量。当
description 是该 callsite 或为空时,改取 details 的首行(通常是
用户实际调用位置),让 data-skew / slow-stages 行的 sql_description
对 DataFrame 作业也有可读输出。

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `app-summary` `top_stages_by_duration` 加 `busy_ratio`

**Files:**
- Modify: `internal/scenario/app_summary.go`
- Modify: `internal/scenario/app_summary_test.go`

- [ ] **Step 1: Write the failing test**

追加到 `internal/scenario/app_summary_test.go`:

```go
func TestAppSummaryTopStagesIncludeBusyRatio(t *testing.T) {
	app := model.NewApplication()
	app.MaxConcurrentExecutors = 10

	// idle stage: wall=100s, run=2s, slots=10 → busy_ratio = 2000 / (100000*10) = 0.002
	idle := model.NewStage(1, 0, "idle", 1, 0)
	idle.SubmitMs = 0
	idle.CompleteMs = 100_000
	idle.TotalRunMs = 2_000
	idle.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = idle

	// busy stage: wall=10s, run=80s across 10 slots → busy_ratio = 80000 / (10000*10) = 0.8
	busy := model.NewStage(2, 0, "busy", 100, 0)
	busy.SubmitMs = 0
	busy.CompleteMs = 10_000
	busy.TotalRunMs = 80_000
	busy.Status = "succeeded"
	app.Stages[model.StageKey{ID: 2}] = busy

	row := AppSummary(app)
	tops := map[int]TopStage{}
	for _, t := range row.TopStagesByDuration {
		tops[t.StageID] = t
	}
	if got := tops[1].BusyRatio; got < 0.001 || got > 0.003 {
		t.Errorf("idle stage busy_ratio=%v want ~0.002", got)
	}
	if got := tops[2].BusyRatio; got < 0.79 || got > 0.81 {
		t.Errorf("busy stage busy_ratio=%v want ~0.8", got)
	}
}

func TestAppSummaryBusyRatioClampsToOne(t *testing.T) {
	app := model.NewApplication()
	app.MaxConcurrentExecutors = 1
	s := model.NewStage(1, 0, "weird", 1, 0)
	s.SubmitMs = 0
	s.CompleteMs = 1_000
	s.TotalRunMs = 5_000 // > wall*slots due to fetch wait accounting
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = s

	row := AppSummary(app)
	if got := row.TopStagesByDuration[0].BusyRatio; got != 1.0 {
		t.Errorf("busy_ratio=%v want clamped to 1.0", got)
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

```bash
go test ./internal/scenario/ -run TestAppSummaryTopStagesIncludeBusyRatio -count=1
```

Expected: FAIL with `tops[1].BusyRatio undefined`(字段还没加)

- [ ] **Step 3: Add `BusyRatio` field + computation**

修改 `internal/scenario/app_summary.go`:

`TopStage` struct 加字段:
```go
type TopStage struct {
	StageID    int     `json:"stage_id"`
	Name       string  `json:"name"`
	DurationMs int64   `json:"duration_ms"`
	BusyRatio  float64 `json:"busy_ratio"`
}
```

`AppSummary` 函数内的循环替换为(保留 `dur` 计算,新增 `busy`):

```go
	type sd struct {
		id   int
		name string
		dur  int64
		busy float64
	}
	var all []sd
	for _, s := range app.Stages {
		switch s.Status {
		case "failed":
			row.StagesFailed++
		case "skipped":
			row.StagesSkipped++
		}
		dur := s.CompleteMs - s.SubmitMs
		if dur < 0 {
			dur = 0
		}
		all = append(all, sd{id: s.ID, name: s.Name, dur: dur, busy: stageBusyRatio(s, app.MaxConcurrentExecutors)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].dur > all[j].dur })
	n := 3
	if len(all) < n {
		n = len(all)
	}
	for i := 0; i < n; i++ {
		row.TopStagesByDuration = append(row.TopStagesByDuration, TopStage{
			StageID: all[i].id, Name: all[i].name, DurationMs: all[i].dur, BusyRatio: all[i].busy,
		})
	}
```

文件末尾追加:
```go
// stageBusyRatio = TotalRunMs / (wall * effective_slots), clamped to [0,1].
// effective_slots = min(NumTasks, MaxConcurrentExecutors), matching IdleStageRule.
// Returns 0 when wall <= 0, slots <= 0, or TotalRunMs <= 0.
func stageBusyRatio(s *model.Stage, maxExec int) float64 {
	wall := s.CompleteMs - s.SubmitMs
	if wall <= 0 || s.TotalRunMs <= 0 {
		return 0
	}
	slots := int64(s.NumTasks)
	if lim := int64(maxExec); lim > 0 && lim < slots {
		slots = lim
	}
	if slots <= 0 {
		return 0
	}
	r := float64(s.TotalRunMs) / float64(wall*slots)
	if r > 1.0 {
		r = 1.0
	}
	return round3(r)
}
```

- [ ] **Step 4: Run tests and verify pass**

```bash
go test ./internal/scenario/ -count=1
```

Expected: PASS(包括既有 `TestAppSummaryColumnsMatchRowFields` —— `top_stages_by_duration` 仍是单字段,内部嵌套对象不影响顶层 columns 比对)

- [ ] **Step 5: Commit**

```bash
git add internal/scenario/app_summary.go internal/scenario/app_summary_test.go
git commit -m "$(cat <<'EOF'
feat(scenario): app-summary top stages 加 busy_ratio

不加这个字段时,driver 端 idle 等待的 stage 会按 wall duration 排进
top,误导用户去优化 executor。busy_ratio = TotalRunMs / (wall *
effective_slots),与 IdleStageRule 同口径,clamp 到 [0,1]。

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `data_skew` 双闸门(输入均匀降级 + idle 抑制)

**Files:**
- Modify: `internal/rules/skew_rule.go`
- Modify: `internal/rules/rule_test.go`
- Modify: `internal/scenario/data_skew.go`
- Modify: `internal/scenario/data_skew_test.go`

- [ ] **Step 1: Write failing tests for SkewRule**

追加到 `internal/rules/rule_test.go`:

```go
func TestSkewRuleDowngradesOnUniformInput(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "uniform-but-jittery", 100, 0)
	for i := 0; i < 99; i++ {
		s.TaskDurations.Add(100)
		s.TaskInputBytes.Add(1024 * 1024) // 1 MiB
	}
	s.TaskDurations.Add(1700) // p99/p50 = 17, below 20 cutoff
	s.TaskInputBytes.Add(1024 * 1024)
	s.MaxInputBytes = 1024 * 1024 // input_skew_factor = 1.0
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = s

	f := SkewRule{}.Eval(app)
	if f.Severity != "warn" {
		t.Fatalf("severity=%s want warn (uniform input + moderate ratio should downgrade)", f.Severity)
	}
}

func TestSkewRuleStaysCriticalOnExtremeRatio(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "extreme", 100, 0)
	for i := 0; i < 99; i++ {
		s.TaskDurations.Add(100)
		s.TaskInputBytes.Add(1024 * 1024)
	}
	s.TaskDurations.Add(2500) // p99/p50 = 25, above 20 cutoff
	s.TaskInputBytes.Add(1024 * 1024)
	s.MaxInputBytes = 1024 * 1024
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = s

	f := SkewRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical (ratio≥20 should bypass uniform-input gate)", f.Severity)
	}
}

func TestSkewRuleSkipsIdleStageCandidate(t *testing.T) {
	app := model.NewApplication()
	app.MaxConcurrentExecutors = 10

	// idle stage with large skew_factor: wall=120s, total_run_ms=300ms → busy_ratio≈0.0003
	idle := model.NewStage(1, 0, "idle-skewed", 100, 0)
	idle.SubmitMs = 0
	idle.CompleteMs = 120_000
	idle.TotalRunMs = 300
	idle.Status = "succeeded"
	for i := 0; i < 99; i++ {
		idle.TaskDurations.Add(2)
		idle.TaskInputBytes.Add(100)
	}
	idle.TaskDurations.Add(60) // p99/p50 = 30 — would normally be critical
	idle.TaskInputBytes.Add(100)
	idle.MaxInputBytes = 100
	app.Stages[model.StageKey{ID: 1}] = idle

	// busy stage with smaller but real skew: wall=60s, total_run_ms=540s (10 slots*54s)
	busy := model.NewStage(2, 0, "busy-skewed", 100, 0)
	busy.SubmitMs = 0
	busy.CompleteMs = 60_000
	busy.TotalRunMs = 540_000
	busy.Status = "succeeded"
	for i := 0; i < 99; i++ {
		busy.TaskDurations.Add(5_000)
		busy.TaskInputBytes.Add(10 * 1024 * 1024)
	}
	busy.TaskDurations.Add(60_000)              // p99/p50 = 12
	busy.TaskInputBytes.Add(120 * 1024 * 1024) // input_skew_factor = 12
	busy.MaxInputBytes = 120 * 1024 * 1024
	app.Stages[model.StageKey{ID: 2}] = busy

	f := SkewRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical", f.Severity)
	}
	if got, _ := f.Evidence["stage_id"].(int); got != 2 {
		t.Errorf("reported stage_id=%v want 2 (idle stage 1 should be suppressed)", got)
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

```bash
go test ./internal/rules/ -run "TestSkewRule" -count=1 -v
```

Expected: 3 个新测试 FAIL(`TestSkewRuleDowngradesOnUniformInput` 拿到 critical 想要 warn;`TestSkewRuleSkipsIdleStageCandidate` 拿到 stage 1 想要 stage 2)

- [ ] **Step 3: Implement gates in SkewRule**

把 `internal/rules/skew_rule.go` 全部替换为:

```go
package rules

import (
	"fmt"
	"math"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type SkewRule struct{}

func (SkewRule) ID() string    { return "data_skew" }
func (SkewRule) Title() string { return "Data skew detected" }

// uniformInputThreshold: input_skew_factor below this is considered uniform
// distribution; combined with moderate p99/p50 ratio, the long tail is more
// likely jitter than real skew, so we downgrade critical to warn.
const uniformInputThreshold = 1.2

// extremeRatioBypass: p99/p50 ratios at or above this stay critical even on
// uniform input — extreme task-time spread is anomalous regardless of data.
const extremeRatioBypass = 20.0

func (SkewRule) Eval(app *model.Application) Finding {
	var bestF float64
	var bestStage *model.Stage
	var bestP50, bestP99, bestInputSkew float64
	for _, s := range app.Stages {
		if s.Status != "succeeded" || int64(s.TaskDurations.Count()) < 50 {
			continue
		}
		p99 := s.TaskDurations.Quantile(0.99)
		if p99 < 1000 {
			continue
		}
		// Suppress candidates that match idle_stage criteria — task-time spread
		// on idle stages is jitter, not skew.
		if isIdleStage(s, app) {
			continue
		}
		median := s.TaskDurations.Quantile(0.5)
		if median < 1 {
			median = 1
		}
		medianB := s.TaskInputBytes.Quantile(0.5)
		if medianB < 1 {
			medianB = 1
		}
		inputSkew := float64(s.MaxInputBytes) / medianB
		f := math.Max(p99/median, inputSkew)
		if f > bestF {
			bestF = f
			bestStage = s
			bestP50 = median
			bestP99 = p99
			bestInputSkew = inputSkew
		}
	}
	if bestStage == nil || bestF < 4 {
		return okFinding(SkewRule{}.ID(), SkewRule{}.Title())
	}
	sev := skewSeverity(bestF, bestInputSkew, bestP50, bestP99)
	evidence := map[string]any{
		"stage_id":          bestStage.ID,
		"skew_factor":       round3(bestF),
		"p50_task_ms":       int64(bestP50),
		"p99_task_ms":       int64(bestP99),
		"input_skew_factor": round3(bestInputSkew),
	}
	aqe := confValue(app, "spark.sql.adaptive.enabled")
	skewJoin := confValue(app, "spark.sql.adaptive.skewJoin.enabled")
	if aqe != "" {
		evidence["spark_sql_adaptive_enabled"] = aqe
	}
	if skewJoin != "" {
		evidence["spark_sql_adaptive_skewjoin_enabled"] = skewJoin
	}
	return Finding{
		RuleID:     SkewRule{}.ID(),
		Severity:   sev,
		Title:      SkewRule{}.Title(),
		Evidence:   evidence,
		Suggestion: skewSuggestion(bestStage.ID, int64(bestP50), int64(bestP99), aqe, skewJoin),
	}
}

// skewSeverity downgrades critical to warn when input is uniform AND the
// p99/p50 spread is moderate. Extreme spread (>= extremeRatioBypass) keeps
// critical regardless of input distribution.
func skewSeverity(f, inputSkew, p50, p99 float64) string {
	if f < 10 {
		return "warn"
	}
	if inputSkew < uniformInputThreshold && (p99/p50) < extremeRatioBypass {
		return "warn"
	}
	return "critical"
}

// isIdleStage mirrors IdleStageRule's wall+busy_ratio thresholds. Kept here
// rather than exported so the two rules can evolve independently if the
// idle definition changes.
func isIdleStage(s *model.Stage, app *model.Application) bool {
	wall := s.CompleteMs - s.SubmitMs
	if wall < 30_000 || s.TotalRunMs <= 0 {
		return false
	}
	slots := int64(s.NumTasks)
	if lim := int64(app.MaxConcurrentExecutors); lim > 0 && lim < slots {
		slots = lim
	}
	if slots <= 0 {
		return false
	}
	return float64(s.TotalRunMs)/float64(wall*slots) < 0.2
}

func skewSuggestion(stageID int, p50, p99 int64, aqe, skewJoin string) string {
	hint := "检查 join key 分布或开启 AQE skew join"
	if skewJoin == "true" {
		hint = "AQE skewJoin 已开启仍长尾，检查 join key 分布或调整 spark.sql.adaptive.skewJoin.skewedPartitionFactor"
	} else if skewJoin == "false" || (aqe != "" && aqe != "true") {
		hint = "建议启用 spark.sql.adaptive.enabled=true 与 spark.sql.adaptive.skewJoin.enabled=true，并复查 join key 分布"
	}
	return fmt.Sprintf("stage %d 任务长尾严重，median %dms / P99 %dms。%s。",
		stageID, p50, p99, hint)
}

func round3(f float64) float64 {
	x := f * 1000
	if x < 0 {
		x -= 0.5
	} else {
		x += 0.5
	}
	return float64(int64(x)) / 1000
}
```

- [ ] **Step 4: Run rules tests, verify pass**

```bash
go test ./internal/rules/ -count=1
```

Expected: PASS(包括既有 `TestSkewRuleTriggers`)

- [ ] **Step 5: Add data-skew verdict gate test (failing first)**

追加到 `internal/scenario/data_skew_test.go`:

```go
func TestDataSkewVerdictDowngradesOnUniformInput(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "uniform", 100, 0)
	for i := 0; i < 99; i++ {
		s.TaskDurations.Add(100)
		s.TaskInputBytes.Add(1024 * 1024)
	}
	s.TaskDurations.Add(1700) // p99/p50=17 (< 20)
	s.TaskInputBytes.Add(1024 * 1024)
	s.MaxInputBytes = 1024 * 1024 // input_skew_factor=1.0
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = s

	rows := DataSkew(app, 10)
	if len(rows) != 1 || rows[0].Verdict != "warn" {
		t.Fatalf("expect single row with verdict=warn, got %+v", rows)
	}
}
```

- [ ] **Step 6: Run, verify failing**

```bash
go test ./internal/scenario/ -run TestDataSkewVerdictDowngradesOnUniformInput -count=1 -v
```

Expected: FAIL — current code reports `severe` (skew_factor=17 ≥ 10 hits old switch).

- [ ] **Step 7: Apply same gate to DataSkew verdict**

修改 `internal/scenario/data_skew.go` 内的 verdict 计算块,把:

```go
		v := "mild"
		switch {
		case f >= 10:
			v = "severe"
		case f >= 4:
			v = "warn"
		}
```

替换为:

```go
		v := skewVerdict(f, inputSkew, p99, median)
```

文件末尾追加(放在 `bytesToMB` 之后):

```go
const (
	dataSkewUniformInputThreshold = 1.2
	dataSkewExtremeRatioBypass    = 20.0
)

// skewVerdict mirrors rules.skewSeverity but emits the verdict ladder used
// by DataSkew rows. Uniform input + moderate ratio downgrades severe to warn.
func skewVerdict(f, inputSkew, p99, median float64) string {
	if f < 4 {
		return "mild"
	}
	if f < 10 {
		return "warn"
	}
	if inputSkew < dataSkewUniformInputThreshold && (p99/median) < dataSkewExtremeRatioBypass {
		return "warn"
	}
	return "severe"
}
```

- [ ] **Step 8: Run all scenario + rules + e2e tests**

```bash
go test ./internal/scenario/ ./internal/rules/ -count=1
go test -tags=e2e ./tests/e2e/... -count=1
```

Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add internal/rules/skew_rule.go internal/rules/rule_test.go \
        internal/scenario/data_skew.go internal/scenario/data_skew_test.go
git commit -m "$(cat <<'EOF'
feat(rules): data_skew 加双闸门防误报

闸门 A:输入均匀(input_skew_factor < 1.2)且 p99/p50 < 20 时,把
critical 降为 warn —— 数据均匀的长尾通常是抖动而非真倾斜。
闸门 B:候选 stage 命中 idle_stage 条件(wall>=30s 且 busy_ratio<0.2)
时整个跳过 —— idle stage 上的 task 长尾本质是调度噪音。
极端 ratio (>=20) 仍保留 critical,不被闸门遮蔽。

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: 同步 SKILL.md / README / CHANGELOG / CLAUDE.md

**Files:**
- Modify: `.claude/skills/spark/SKILL.md`
- Modify: `README.md`
- Modify: `README.zh.md`
- Modify: `CHANGELOG.md`
- Modify: `CHANGELOG.zh.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: 更新 SKILL.md**

在 `.claude/skills/spark/SKILL.md` 的 "Drill down based on findings" 段:

- `data_skew` 行末追加:"**注意**:`evidence.input_skew_factor < 1.2` 时(数据均匀),即使 `skew_factor` 很高也常被降级为 warn —— 这种长尾多半是 task 抖动而非真倾斜。"
- `gc_pressure` 行下方加新段:"**GC ratio 三层口径**:`envelope.gc_ratio` = total_gc_ms / total_run_ms(全应用),`slow-stages[].gc_ratio` = stage 内 sum(task_gc) / sum(task_run),`gc-pressure.by_executor[].gc_ratio` = executor 累加值;**三层分母都是 task 累加**,不要用 wall(`duration_ms`)做分母。"
- "For overview: `spark-cli app-summary <appId>`" 行下方加:"top_stages_by_duration 每行带 `busy_ratio`,接近 0 的 stage 是 driver 端 idle 等待,不是优化目标。"

- [ ] **Step 2: 更新 README.md / README.zh.md**

把英文 README 中 slow-stages 输出示例的 columns 列表里 `"gc_ms"` 后面加 `"gc_ratio"`(若无示例则跳过);data-skew 段提到 verdict 时补一句 "with input_skew_factor < 1.2 the verdict is downgraded to warn"。中文同义改 `README.zh.md`。

- [ ] **Step 3: 更新 CHANGELOG**

`CHANGELOG.md` 顶部 `## [Unreleased]` 段加:

```markdown
### Added
- `slow-stages` rows now expose `gc_ratio` (sum(task_gc) / sum(task_run)).
- `app-summary.top_stages_by_duration[]` rows now expose `busy_ratio` so idle stages are visible at a glance.

### Changed
- `data_skew` rule downgrades critical → warn when input is uniform (`input_skew_factor < 1.2`) and `p99/p50 < 20`. Extreme ratios still report critical.
- `data_skew` rule skips stages that match `idle_stage` criteria — task-time long tails on idle stages are jitter, not skew.
- `stageSQL` falls back to `details` first line when `description` is Spark's default `getCallSite at SQLExecution.scala:74` callsite or empty (typical DataFrame API submission).
```

`CHANGELOG.zh.md` 写中文等价条目。

- [ ] **Step 4: 更新 CLAUDE.md**

在 `## 已知踩坑` 段末尾追加:

```markdown
- **sql_description 对 DataFrame 作业默认是 callsite**:`SparkListenerSQLExecutionStart.description` 在 DataFrame API 提交时是 `getCallSite at SQLExecution.scala:74`,几乎没有信息量。`stageSQL` 在 `internal/scenario/result.go` 已实现 fallback:description 是该 callsite 或为空时,改取 `SQLExecution.Details` 的首行。**不要**回退到只读 description,否则 data-skew / slow-stages 的 sql_description 列对绝大多数生产作业完全无用。
- **data_skew 闸门**:`SkewRule` 与 `DataSkew` 在 `input_skew_factor < 1.2` 且 `p99/p50 < 20` 时,把 critical 降为 warn(数据均匀的长尾几乎都是抖动);候选 stage 同时命中 `IdleStageRule`(wall>=30s + busy_ratio<0.2)时直接跳过。两道闸门的阈值与 `IdleStageRule` 同口径,**改 idle 阈值时记得回头改 SkewRule.isIdleStage**(目前两边各保一份,故意不抽 helper)。
- **三层 gc_ratio 口径**:`Envelope.gc_ratio`(顶层)= `app.TotalGCMs / app.TotalRunMs`;`SlowStageRow.gc_ratio` = `stage.TotalGCMs / stage.TotalRunMs`;`GCExecRow.gc_ratio` = `executor.TotalGCMs / executor.TotalRunMs`。**三层分母都是 task 累加**,**不要**用 wall(`duration_ms`)做分母,否则多 executor 并发场景下会出现 >100% 的诡异值。
- **app-summary top stages 的 busy_ratio**:`TopStage.BusyRatio` = `TotalRunMs / (wall * effective_slots)`,clamp 到 [0,1]。接近 0 的 stage 在 top 列表里是 driver-side idle stage(broadcast/planning/listing 等),按 wall 排序会把它们推到前面但优化方向完全不同 —— agent / 用户读 top 时**必须**先看 busy_ratio 再决定是否值得动 stage 本身。
```

- [ ] **Step 5: 跑完整 gate**

```bash
make tidy && make lint && make unit-test && make e2e
```

Expected: 全绿,`go.mod` / `go.sum` 无 diff,vet/gofmt/golangci 无报警,所有单测/E2E pass。

- [ ] **Step 6: Commit docs**

```bash
git add .claude/skills/spark/SKILL.md README.md README.zh.md CHANGELOG.md CHANGELOG.zh.md CLAUDE.md \
        docs/superpowers/specs/2026-05-02-spark-cli-diagnostic-fixes-design.md \
        docs/superpowers/plans/2026-05-02-spark-cli-diagnostic-fixes.md
git commit -m "$(cat <<'EOF'
docs: 同步诊断精度修复(SKILL/README/CHANGELOG/CLAUDE)

记录 data_skew 双闸门、stageSQL fallback、busy_ratio、三层 gc_ratio
口径,并附设计 spec + 实施 plan。

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Push

- [ ] **Step 1: 检查本地 main 状态**

```bash
git log --oneline origin/main..HEAD
git status
```

Expected: 看到 5 条新 commit(Task 1/2/3/4 各 1 条 + Task 5 docs)、worktree 干净。

- [ ] **Step 2: Push**

```bash
git push origin main
```

Expected: `release.yml` workflow 自动触发 patch bump tag(见 CLAUDE.md 发版段)。

- [ ] **Step 3: 报告完成**

向用户报告:5 条 commit 已 push,触发了自动 tag bump;关键改动一句话总结。
