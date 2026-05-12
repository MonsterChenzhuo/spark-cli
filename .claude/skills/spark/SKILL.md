---
name: spark-performance-diagnostics
description: Use when investigating Spark application performance, slow stages, GC pressure, data skew, or task failures. Run via the spark-cli binary on local or HDFS EventLog directories.
---

# Spark Performance Diagnostics

You have access to `spark-cli`, a single-binary CLI that parses Spark EventLogs and emits JSON envelopes. Always start with `diagnose` — it runs all rules at once.

## Required input

The user must provide a Spark `applicationId` (e.g. `application_1735000000_0001`). Accept short forms (`1735000000_0001`) — the CLI normalizes them.

## Workflow

1. **Always run diagnose first**:
   ```
   spark-cli diagnose <appId>
   ```
   Read `summary.critical` and `summary.warn`. Even `severity: "ok"` rows are meaningful — they confirm a check ran. **优先看 `summary.top_findings_by_impact`**(按 `wall_share` 倒序的 `[{rule_id, severity, wall_share}]`)—— 这个数组直接告诉你哪个 finding 占整作业耗时最多,不要再自己心算。`app.DurationMs == 0`(没 ApplicationEnd 事件)或 finding 没 `stage_id` 关联时这段会缺失(omitempty)。**先看 `summary.findings_wall_coverage`**(所有非 ok finding 涉及 stage 占应用 wall 的总比例,按 stage_id 去重 + cap 1.0)—— **当它 < 0.05** 时,瓶颈基本不在 finding 列表里(常见为作业结构碎片化 / driver-side 等待 / 太多 action),应当**直接跳到 `app-summary` 看 `top_busy_stages` / `top_io_bound_stages` / `stages_total` / `jobs_total`**,而不是继续逐条下钻 finding。
   **severity 是诊断置信度,不是 ROI 优先级**:`disk_spill warn (wall_share 0.5)` 实际比 `data_skew critical (wall_share 0.05)` 更值得修;按 severity 字符串排优先级会错位,永远以 `top_findings_by_impact` 为准。

2. **Drill down based on findings**:
   - `data_skew` critical → 优先**直接读 finding evidence 的 `similar_stages: [{stage_id, wall_share, skew_factor}]`**(SkewRule 在多 stage 命中时输出;primary stage_id 选 wall_share 最大那个,其余进 similar_stages 按 wall_share 倒序最多 4 条)。需要更详细排序时再 `spark-cli data-skew <appId> --top 10`。每行带 `sql_execution_id` + `wall_share`;**SQL description 文本在 envelope 顶层 `sql_executions: {<id>: <description>}` 一份共享,不再嵌入每行**;**默认 truncate 到前 500 个 rune**(`--sql-detail=full` 还原完整 SQL,`--sql-detail=none` 整段 omit)。对 DataFrame 作业自动回退到 `details` 首行;若 description / details 都是 callsite 形态则整行被过滤,所有条目都是噪音时整段 map 走 `omitempty` 缺失。**四道闸门**降级 critical:**(0) 任务时长紧致**:`p99/p50 < 1.5` → 规则直接 `ok`、行 verdict `mild`(任务时长本身就均匀,不存在真倾斜);(1) `input_skew_factor < 1.2` 且 `p99/p50 < 20`(均匀输入抖动)→ warn;(2) stage `wall_share < 1%` 且 `p99/p50 < 20`(短 stage 长尾收益太低)→ DataSkew row verdict 直接降为 mild;(3) 命中 `idle_stage` 条件(wall ≥ 30s + busy_ratio < 0.2 且 `NumTasks <= 2*MaxConcurrentExecutors`)整个跳过 —— 任务多但 IO 阻塞导致 busy_ratio 低的 stage 不再被误当 idle。极端 ratio (≥ 20) 不被任何闸门遮蔽。
   - `gc_pressure` critical → `spark-cli gc-pressure <appId>`(`data` 是 executor 行扁平数组,直接读 `data[].gc_ratio` 找压力高的 executor)。The `gc_pressure` finding now embeds `spark_executor_memory` in evidence — quote it before suggesting tuning.
   - `disk_spill` triggered → `spark-cli slow-stages <appId>` and read `spill_disk_gb`. Evidence 现在还带 `partitions`(stage 实际 task 数)和 `est_partition_size_mb`(`shuffle_read / num_tasks` 转 MB)—— 建议时把 `est_partition_size_mb` 跟 `spark_executor_memory` 比较,直接给出"partition size 已经超 executor 内存,把 shuffle.partitions 翻到 X"这种带数字的建议,不要套模板。
   - `failed_tasks` triggered → if `evidence.blacklisted_hosts` is non-empty, those hosts have been excluded ≥2 times; report them by name and tell the user the failure looks node-level (hardware/network/disk), not random task flakiness. Otherwise ask for driver logs.
   - `tiny_tasks` triggered → 分区过细,evidence 现在带 `wall_share` / `wall_ms` + 多 stage 命中时含 `similar_stages`;suggestion 直接给"降到 ~N partition"的具体目标(基于"目标 task ~500ms"经验式),不必自己再算
   - `idle_stage` triggered → stage wall-clock 远大于 executor 实际工作时间(driver 端 broadcast/串行计算/调度等待),用 `spark-cli slow-stages <appId>` 看具体 stage,然后排查执行计划
   - `executor_supply` triggered → 静态配置的 `spark.executor.instances` 大于 EventLog 实际注册/并发 executor。**只能说明 executor 供给不足,不能从 EventLog 直接断言 YARN 为什么没分配**;引用 evidence 里的 `configured_executor_instances` / `max_concurrent_executors` / `executors_added`,然后让用户查 YARN RM/AM 的 pending containers、user limit、队列容量、节点标签、reserved containers、container 资源碎片。
   - All `ok` but user reports slowness → `spark-cli slow-stages <appId> --top 5`

3. **For overview**: `spark-cli app-summary <appId>`。app-summary 同时输出三个互补切面,**永远要三个一起看**才能不漏瓶颈:
   - 先看 `dynamic_allocation_enabled` / `configured_executor_instances` / `executors_added` / `max_concurrent_executors`。静态申请数远大于实际 executor 时,瓶颈先按 YARN executor 供给不足处理;但原因必须去 RM/AM 查,EventLog 不含调度器拒绝原因。
   - `top_stages_by_duration[]` 按 wall 倒序(包含 driver-side 等待 stage),**先看 busy_ratio** 再决定是否下钻。
   - **`top_busy_stages[]`** —— `busy_ratio > 0.8` 的 stage 按 `busy_ratio * duration_ms` 倒序。这是真正吃 CPU 的 executor 热点;driver-side 等待 stage 自动过滤掉。
   - **`top_io_bound_stages[]`** —— `busy_ratio < 0.8` 但 `spill_disk_gb >= 0.5` 或 `shuffle_read_gb >= 1` 的 stage,按 wall_share 倒序。这是 spill / shuffle 主导的"大 wall + 低 busy"瓶颈,**只看 top_busy_stages 会全错过**(典型场景:stage spill 9 GB,executor 都在等盘,busy_ratio 仅 0.05;这种 stage 才是真值得修的)。
   - `slow-stages` 行附 `wall_share`(stage.duration / app.duration)+ `busy_ratio` + 三档 `*_mb_per_task`(`input_mb_per_task` / `shuffle_read_mb_per_task` / `shuffle_write_mb_per_task`,任一 NumTasks=0 时为 0)。看 stage 偏写侧 / 读侧 / source-scan 时分别看对应字段判 partition 粒度;`wall_share` 直接给 ROI 信号,不必再 dur / app_duration_ms 自己除。

### GC ratio 三层口径

三层 `gc_ratio` 分母都是 task 累加,不是 wall —— 不要用 `gc_ms / duration_ms`,多 executor 并发场景下会得到 >100% 的诡异值。

| 层级 | 字段 | 公式 |
|---|---|---|
| 应用全局 | `envelope.gc_ratio`(`app-summary` 顶层) | `total_gc_ms / total_run_ms` |
| stage 内 | `slow-stages[].gc_ratio` | `sum(task_gc) / sum(task_run)` |
| executor 累加 | `gc-pressure.data[].gc_ratio`(executor 行) | `executor.TotalGCMs / executor.TotalRunMs` |

## Envelope contract

Every command emits one JSON object on stdout:

```json
{
  "scenario": "...",
  "app_id": "...",
  "log_path": "...",
  "log_format": "v1|v2",
  "compression": "none|zstd|lz4|snappy",
  "incomplete": false,
  "parsed_events": 482113,
  "elapsed_ms": 1842,
  "app_duration_ms": 4230802,
  "columns": [...],
  "data": [...]
}
```

`app_duration_ms` 是应用 wall 时长(`SparkListenerApplicationEnd - SparkListenerApplicationStart`),omitempty 在 `app.DurationMs == 0`(没 ApplicationEnd 事件)时缺失。所有 5 场景一致输出 —— 看 `wall_share` 时直接换算绝对秒数,不必再额外跑 `app-summary`。

Exceptions:
- `gc-pressure` returns `data: [{executor_id, host, tasks, run_ms, gc_ms, gc_ratio, verdict}, ...]` —— 跟其他场景一致是数组(每行一个 executor),不再像早期 spec 那样有 by_stage / by_executor 双段
- `diagnose` adds `summary: {critical, warn, ok, top_findings_by_impact?, findings_wall_coverage?}`. `top_findings_by_impact` is an array of `{rule_id, severity, wall_share}` sorted desc by `wall_share` — `wall_share` 取**该 finding 命中所有 stage(primary + similar_stages)的 max** 而非 sum,直观对应"本规则击中的最严重 stage"。`findings_wall_coverage` is the deduped (max-per-stage) sum of those wall shares **capped at 1.0**(stage 在 wall 上并行,naive sum 可能 > 1.0,语义上不应超 100%);**< 0.05 means the bottleneck is structural — read `app-summary.top_busy_stages` / `top_io_bound_stages` instead of drilling findings**. Both fields missing (omitempty) when `app.DurationMs == 0` (no ApplicationEnd event).
- `data_skew` finding evidence 在多 stage 命中时含 `similar_stages: [{stage_id, wall_share, skew_factor}]`(按 wall_share 倒序最多 4 条),`top_findings_by_impact` 与 `findings_wall_coverage` 都会聚合 primary + similar_stages 的覆盖度 —— **看到 similar_stages 时直接读它,不必再回头跑 data-skew 找补**。
- `slow-stages` and `data-skew` add `sql_executions: {<int64 id>: <description>}` at the top level. **map 只包含当前 row 实际引用的 sql_id**(`--top 5` 截断后,没出现在 row 里的 SQL 不会泄露到 envelope —— 例如 `SHOW DATABASES` 这种 setup SQL)。Rows reference the id via `sql_execution_id`; **rows no longer carry `sql_description`** — look it up in the top-level map. **Description 默认 truncate 到前 500 个 rune**,过长追加 `...(truncated, total <N> chars)`;`--sql-detail=full` 还原完整 SQL,`--sql-detail=none` 整段 omit。Empty/absent (omitempty) when no stage links to a SQL execution **or every linked execution carries only callsite noise** (DataFrame jobs whose description and details are both `org.apache.spark.SparkContext.getCallSite(...)`-style placeholders).

`incomplete: true` means an `.inprogress` log was read — treat data as preliminary.

## Errors

Errors go to **stderr** as `{"error": {"code": "...", "message": "...", "hint": "..."}}`. Exit codes:
- `0` success
- `1` internal error (file a bug)
- `2` user error (bad flag, app not found, ambiguous)
- `3` IO/HDFS unreachable

`diagnose` returns `0` even when findings are critical — read `summary.critical` to decide.

## Useful flags

- `--log-dirs <uri,uri>` — comma-separated `file://`, `hdfs://`, and/or `shs://host:port` URIs (Spark History Server REST endpoint) to search
- `--format json|table|markdown` — default `json`; use `markdown` when embedding in chat
- `--top N` — for `slow-stages` / `data-skew` / `gc-pressure`
- `--dry-run` — locate the log without parsing (fast sanity check)
- `--cache-dir <path>` — persistent cache dir (default `~/.cache/spark-cli`); cached runs report `parsed_events: 0`
- `--no-cache` — bypass the parsed-application cache for this invocation (no read, no write)
- `--shs-timeout <duration>` — HTTP timeout for `shs://` log-dirs (default `5m`;生产 zip 几个 GB 是常态,失败时 hint 直接告知此参数)
- `--sql-detail truncate|full|none` — `sql_executions` 中 description 的呈现:**默认 truncate**(前 500 个 rune,过长加 `...(truncated, total <N> chars)` 标记),`full` 还原原始 SQL,`none` 整段 omit。也可用 `SPARK_CLI_SQL_DETAIL` 环境变量 / yaml `sql.detail` 覆盖。
- `--no-progress` — 不打 SHS zip 下载进度提示(优先级高于 SPARK_CLI_QUIET 与 TTY 检测)。
- SHS zip 持久化:同一 appID 的 zip 在 `<cache_dir>/shs/<host>/<appID>_<lastUpdated>.zip` 复用,attempt 更新时旧文件自动 sweep;`--no-cache` 旁路。
- `SPARK_CLI_QUIET` — `1`/`true` 强制静默,`0`/`false` 强制保留进度,**未设时按 stdout 是否 TTY 自动决定**(管道 / 重定向 / agent 调用默认静默,交互终端默认显示)。

## Utility commands

- `spark-cli config show [--format json]` — print effective configuration with source labels (`file` / `env` / `default` / `flag`). JSON 形态适合 agent 一次拿到完整状态,而不必分别读 yaml + env 比对生效值。
- `spark-cli cache list [--format json]` — 列所有 cached parsed application + SHS zip(按 size 降序),应用 cache hit 慢于预期时先看这个。
- `spark-cli cache clear [--app <id>] [--dry-run]` — 删全部 cache 或只删指定 app 的 entry;`--dry-run` 先看会删什么再确认。

## Setup if missing

If `spark-cli` is not installed, fetch the appropriate release:

```
curl -fsSL https://raw.githubusercontent.com/opay-bigdata/spark-cli/main/scripts/install.sh | bash
```

Or build from source: `go install github.com/opay-bigdata/spark-cli@latest`.

If config is missing, run `spark-cli config init` to write `~/.config/spark-cli/config.yaml` with default `log-dirs` placeholders.

## Don't

- Don't parse the JSON manually — use the documented field names from `columns` to know what's in `data`.
- Don't compare `gc_ratio` across runs without checking `total_run_ms` — short stages have noisy GC ratios.
- Don't claim a problem isn't present without running `diagnose` first.
