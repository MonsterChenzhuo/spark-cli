# spark-cli 诊断精度与可读性修复设计文档

> **状态**：Draft(待用户复审)
> **日期**:2026-05-02
> **作者**:Claude Code(brainstorming session)
> **目标读者**:实现者 + 后续维护者
> **关联**:`docs/superpowers/specs/2026-04-29-spark-cli-design.md`(MVP 设计)

---

## 0. 背景

在真实生产 EventLog(`application_1771556836054_951861`)上跑 `spark-cli diagnose` 时观察到 4 处影响 agent 决策质量的问题:

1. **`sql_description` 字段对 DataFrame 作业全部退化为 callsite**(`getCallSite at SQLExecution.scala:74`),SKILL.md 承诺的"quote the SQL that owns the skewed stage"无法兑现。
2. **`data_skew` 规则在数据均匀的 idle stage 上误报 critical**(stage 0:input_skew_factor=1.011 但 skew_factor=16.99 + busy_ratio=1%),让 agent 把注意力放到错误的优化方向上。
3. **`top_stages_by_duration` 不区分"真在跑的 stage"与"driver 在等的 idle stage"**,duration 排序对前者有意义、对后者完全误导。
4. **GC 比值在不同层级口径不一致**:envelope 顶层 `gc_ratio = total_gc_ms / total_run_ms`,但 `slow-stages` 行只暴露 `gc_ms` 与 `duration_ms`,直接相除得到 >100% 的诡异数字(因 12 并发 executor 累加 vs wall)。SKILL.md 没说清两层口径,易踩坑。

## 0.1 目标

- 修这 4 处,**不破坏 Envelope 形状**(只新增字段,不删/不改类型)
- 不动 `internal/model` 字段类型/名称,**不需要** bump cache schema
- 每条修复带单元测试 + E2E 守门
- 同步更新 SKILL.md / README / CHANGELOG / CLAUDE.md

## 0.2 非目标

- **不**重构现有规则评估流程(规则间共享上下文留作后续)
- **不**改 `internal/output` 表格/Markdown formatter 列宽逻辑(JSON 契约更新即可)
- **不**新增 CLI flag / 配置项
- **不**改其他规则(GC / spill / failed_tasks / tiny / idle_stage)的判定阈值

---

## 1. 修复 #1:`sql_description` 回退到 details

### 1.1 当前行为

`internal/scenario/result.go:38` 的 `stageSQL`:

```go
if e, ok := app.SQLExecutions[id]; ok {
    return id, e.Description
}
```

`SQLExecution.Description` 来自 Spark 的 `SparkListenerSQLExecutionStart.description` 字段。该字段对 SQL 文本提交是真 SQL,对 DataFrame API 提交则是 callsite(`getCallSite at SQLExecution.scala:74` —— Spark 内部默认行为)。

`SQLExecution.Details` 字段已经存在并被 aggregator 填充(`internal/model/aggregator.go:88-99`),内容是 Spark 物理计划 / 代码栈 + (有时) SQL,信息量远大于 callsite,但**当前完全没被任何 scenario 使用**。

### 1.2 设计决策

`stageSQL` 增加 fallback:**当 description 看起来像 callsite 时**,改取 details 的首行(避免把整个物理计划塞进 envelope)。

判定标准(简单直接,不引入正则):
- description 以 `getCallSite ` 前缀开头 → fallback
- description 为空 → fallback
- 其他情况 → 维持原 description

为什么取 details 首行而不是全文:
- details 在生产 Spark 经常是几十 KB 的物理计划,塞进 envelope 会让 JSON 体积爆炸
- 首行通常是 `Execution: <op> at <call>`,信息密度最高
- agent 想看完整 plan 可以单独命令(本期不做)

返回值仍为 `(int64, string)`,签名不变。

### 1.3 测试

`internal/scenario/result_test.go`(已存在,扩):
- callsite description + 有意义 details → 返回 details 首行
- callsite description + 空 details → 返回 callsite(保留原行为,有总比无好)
- 真 SQL description → 维持 description
- 空 description + 有 details → 返回 details 首行

---

## 2. 修复 #2:`data_skew` 误报闸门

### 2.1 当前行为

`internal/rules/skew_rule.go` 的 `f = max(p99/p50, max_input/median_input)`,`f >= 10` 即 critical。问题:
- 输入数据均匀时(`max_input/median_input ≈ 1`),`p99/p50` 任何抖动都会撞 10x 阈值
- 同一 stage 已被 idle_stage 命中时,task 时长长尾纯粹是抖动,谈倾斜没意义

### 2.2 设计决策

引入两道闸门,**仅对 critical 升级**生效(warn / mild 不动,避免静默小问题):

**闸门 A — 输入均匀降级**:
- 若 `input_skew_factor < 1.2`(数据均匀)且 `p99/p50 < 20` → 不报 critical(降到 warn)
- 阈值理由:input_skew_factor 1.2 即数据最大与中位数差不超过 20%,基本认定均匀;p99/p50 ≥ 20x 时即使数据均匀也异常严重,保留 critical

**闸门 B — idle stage 抑制**:
- 若候选 hot stage 满足 `idle_stage` 触发条件(wall ≥ 30s + busy_ratio < 0.2) → 跳过该 stage,选下一个候选
- 实现:`SkewRule` 内部抽出 `isIdleStage(s, app) bool` 辅助函数(与 `IdleStageRule` 共享公式,不抽 package level helper —— YAGNI,两条规则在同 package)
- 注:闸门 B 只影响 SkewRule 选择哪个 stage 上报,不影响 IdleStageRule 自身

`internal/scenario/data_skew.go` 的 verdict 同步加闸门 A:
- `input_skew_factor < 1.2` 且 `p99/p50 < 20` 时,把 verdict 从 `severe` 降到 `warn`(`mild` 不动)
- 输出行**不过滤**,只调 verdict —— 用户列表里能看到所有候选

### 2.3 测试

`internal/rules/rule_test.go` 扩:
- `TestSkewRuleQuietsOnUniformInput`:p99/p50 = 17、input_skew_factor = 1.05 → severity = "warn"
- `TestSkewRuleStillCriticalOnExtremeRatio`:p99/p50 = 25、input_skew_factor = 1.05 → severity = "critical"(闸门 A 不挡 ≥20x)
- `TestSkewRuleSkipsIdleStage`:stage A wall=120s/busy=0.01,stage B wall=30s/busy=0.5 都倾斜 → 报告 stage B(stage A 被 idle 抑制)

`internal/scenario/data_skew_test.go` 扩:
- `TestDataSkewVerdictDowngradesOnUniformInput`:p99/p50=17、input_skew=1.05 → verdict="warn"

---

## 3. 修复 #3:`app-summary` top stages 加 `busy_ratio`

### 3.1 当前行为

`internal/scenario/app_summary.go` 的 `TopStage`:

```go
type TopStage struct {
    StageID    int    `json:"stage_id"`
    Name       string `json:"name"`
    DurationMs int64  `json:"duration_ms"`
}
```

按 `dur = CompleteMs - SubmitMs` 排序。idle stage(driver 等)和真在跑的 stage 在 top 列表里没法区分。

### 3.2 设计决策

`TopStage` 加 `BusyRatio float64 json:"busy_ratio"`:

```go
busy_ratio = TotalRunMs / (wall * effective_slots)
effective_slots = min(NumTasks, MaxConcurrentExecutors)  // 与 IdleStageRule 同口径
```

边界:
- `wall <= 0` 或 `effective_slots <= 0` → busy_ratio = 0
- `TotalRunMs <= 0` → busy_ratio = 0(stage 没跑过任务)
- 计算结果 > 1.0(罕见,Spark task 时间含 fetch wait 可能轻微超过)→ clamp 到 1.0

`AppSummaryColumns()` 不变(`top_stages_by_duration` 是单字段名;嵌套对象的内部字段不在 columns 列表里)。

### 3.3 影响

- Envelope 顶层契约不变,`columns` 不变
- `top_stages_by_duration[].busy_ratio` 是新字段
- 不需要 cache schema bump(只动 scenario 输出,不动 `model.Application`)
- e2e 现有断言只看顶层 keys,自动通过;新增断言查 busy_ratio 存在

### 3.4 测试

`internal/scenario/app_summary_test.go` 扩:
- `TestAppSummaryTopStagesIncludeBusyRatio`:构造 idle stage(wall=100s, run=2s, slots=10) + busy stage(wall=10s, run=80s, slots=10) → 验证两个的 busy_ratio 计算正确
- 现有 `TestAppSummaryColumnsMatchRowFields` 反射断言不受影响(嵌套对象内部字段不在 columns 里)

---

## 4. 修复 #4:`slow-stages` 行加 `gc_ratio`,口径明确

### 4.1 当前行为

`SlowStageRow` 只有 `GCMs int64 json:"gc_ms"` 和 `DurationMs int64 json:"duration_ms"`。前者是 task GC 累加,后者是 wall;直接相除会得到 100%+ 的误导值。

### 4.2 设计决策

`SlowStageRow` 加 `GCRatio float64 json:"gc_ratio"`:

```go
gc_ratio = TotalGCMs / TotalRunMs   // 两者均为 task 累加,口径一致
```

边界:
- `TotalRunMs <= 0` → gc_ratio = 0

`SlowStagesColumns()` 在 `gc_ms` 后面追加 `gc_ratio`(顺序遵循"原始量在前、派生量在后")。

SKILL.md 同步说明:
- envelope 顶层 `gc_ratio` = `total_gc_ms / total_run_ms`(应用全局口径)
- `slow-stages` 行 `gc_ratio` = `sum(task_gc) / sum(task_run)`(stage 内 task 累加口径)
- `gc-pressure` executor 行 `gc_ratio` = `executor.TotalGCMs / executor.TotalRunMs`(executor 累加口径)
- 三层口径分母都是"task 实际工作时间累加",可比性强;**不要**用 `gc_ms / duration_ms`,duration 是 wall。

### 4.3 影响

- envelope 数组每行新增字段,columns 同步加一列 → 现有反射守门测试(若有)需要看 `slow_stages_test.go`
- 不需要 cache schema bump

### 4.4 测试

`internal/scenario/slow_stages_test.go` 扩:
- `TestSlowStagesGCRatio`:stage TotalGCMs=300, TotalRunMs=1000 → gc_ratio=0.3
- `TestSlowStagesGCRatioZeroDivision`:TotalRunMs=0 → gc_ratio=0

---

## 5. 文档与契约同步

按 CLAUDE.md "Envelope 改动需同步" 规则:

| 文件 | 改动 |
|---|---|
| `.claude/skills/spark/SKILL.md` | (a) data_skew 段加"输入均匀时降级 + idle 抑制"说明;(b) app-summary 段提到 `top_stages_by_duration[].busy_ratio`;(c) slow-stages 段说明 `gc_ratio` 口径;(d) 三层 gc_ratio 口径表 |
| `README.md` / `README.zh.md` | 输出示例字段更新(top_stages_by_duration 加 busy_ratio、slow-stages 行加 gc_ratio) |
| `CHANGELOG.md` / `CHANGELOG.zh.md` | 一条 Unreleased 项:"诊断精度与可读性修复" |
| `CLAUDE.md` | "已知踩坑"加一条"sql_description 对 DataFrame 作业回退 details 首行";"添加新场景的标准步骤"末尾保留 cache schema bump 提醒(本次不 bump,但规则没变) |

---

## 6. 实施顺序与回归

按互独立程度从小到大:

1. **修复 #4(slow-stages gc_ratio)**:scenario 层独立加字段,最小风险
2. **修复 #1(stageSQL fallback)**:跨 scenario 取数,影响 data-skew / slow-stages 两个输出
3. **修复 #3(app-summary busy_ratio)**:嵌套对象加字段
4. **修复 #2(skew 闸门)**:逻辑判定改动最大,放最后

每步完成后跑:

```bash
make tidy && make lint && make unit-test && make e2e
```

最终一次性同步文档 + commit。

---

## 7. 风险与缓解

| 风险 | 缓解 |
|---|---|
| #1 fallback 的"callsite 判定"过度宽松,真 SQL 被误判成 callsite | 仅匹配 `getCallSite ` 前缀(Spark 内部固定字符串),不做模糊判断 |
| #2 闸门 A 把真倾斜误降级 | 双条件 `input_skew < 1.2` 且 `p99/p50 < 20`,极端长尾仍 critical |
| #2 闸门 B 跳过 idle stage 后没有候选 | `SkewRule` 自然返回 ok finding,行为与"无倾斜"一致 |
| #3 busy_ratio > 1 显示异常 | clamp 到 1.0 |
| #4 columns 加列破坏 e2e | 现有 e2e 只断言顶层 keys,不检查 columns 数量;手工跑 `make e2e` 确认 |
| 引入逻辑回归被遗漏 | 4 项各自带单元测试,覆盖正/反/边界 |

---

## 8. 不在本期范围

- SkewRule 的多 stage 排序(当前只报最 hot 一个)
- gc-pressure 引入 by_stage 视图(SKILL 提到但非本期目标)
- output formatter 的表格列对齐(只动 JSON 契约)
- cache schema bump(model 字段未变)
