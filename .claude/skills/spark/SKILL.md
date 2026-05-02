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
   Read `summary.critical` and `summary.warn`. Even `severity: "ok"` rows are meaningful — they confirm a check ran. **优先看 `summary.top_findings_by_impact`**(按 `wall_share` 倒序的 `[{rule_id, severity, wall_share}]`)—— 这个数组直接告诉你哪个 finding 占整作业耗时最多,不要再自己心算。`app.DurationMs == 0`(没 ApplicationEnd 事件)或 finding 没 `stage_id` 关联时这段会缺失(omitempty)。**先看 `summary.findings_wall_coverage`**(所有非 ok finding 涉及 stage 占应用 wall 的总比例,按 stage_id 去重)—— **当它 < 0.05** 时,瓶颈基本不在 finding 列表里(常见为作业结构碎片化 / driver-side 等待 / 太多 action),应当**直接跳到 `app-summary` 看 `top_busy_stages` + `stages_total` + `jobs_total`**,而不是继续逐条下钻 finding。

2. **Drill down based on findings**:
   - `data_skew` critical → `spark-cli data-skew <appId> --top 10`。每行带 `sql_execution_id` + `wall_share`;**SQL description 文本在 envelope 顶层 `sql_executions: {<id>: <description>}` 一份共享,不再嵌入每行**(对 DataFrame 作业自动回退到 `details` 首行;若 description / details 都是 callsite 形态则整行被过滤,所有条目都是噪音时整段 map 走 `omitempty` 缺失)。**四道闸门**降级 critical:**(0) 任务时长紧致**:`p99/p50 < 1.5` → 规则直接 `ok`、行 verdict `mild`(任务时长本身就均匀,不存在真倾斜);(1) `input_skew_factor < 1.2` 且 `p99/p50 < 20`(均匀输入抖动)→ warn;(2) stage `wall_share < 1%` 且 `p99/p50 < 20`(短 stage 长尾收益太低)→ DataSkew row verdict 直接降为 mild;(3) 命中 `idle_stage` 条件(wall ≥ 30s + busy_ratio < 0.2)整个跳过。极端 ratio (≥ 20) 不被任何闸门遮蔽。
   - `gc_pressure` critical → `spark-cli gc-pressure <appId>` (look at `by_executor`). The `gc_pressure` finding now embeds `spark_executor_memory` in evidence — quote it before suggesting tuning.
   - `disk_spill` triggered → `spark-cli slow-stages <appId>` and read `spill_disk_gb`. Evidence 现在还带 `partitions`(stage 实际 task 数)和 `est_partition_size_mb`(`shuffle_read / num_tasks` 转 MB)—— 建议时把 `est_partition_size_mb` 跟 `spark_executor_memory` 比较,直接给出"partition size 已经超 executor 内存,把 shuffle.partitions 翻到 X"这种带数字的建议,不要套模板。
   - `failed_tasks` triggered → if `evidence.blacklisted_hosts` is non-empty, those hosts have been excluded ≥2 times; report them by name and tell the user the failure looks node-level (hardware/network/disk), not random task flakiness. Otherwise ask for driver logs.
   - `tiny_tasks` triggered → 分区过细,建议 `coalesce` / 调低 `spark.sql.shuffle.partitions`
   - `idle_stage` triggered → stage wall-clock 远大于 executor 实际工作时间(driver 端 broadcast/串行计算/调度等待),用 `spark-cli slow-stages <appId>` 看具体 stage,然后排查执行计划
   - All `ok` but user reports slowness → `spark-cli slow-stages <appId> --top 5`

3. **For overview**: `spark-cli app-summary <appId>`。
   - `top_stages_by_duration[]` 每行带 `busy_ratio`,接近 0 的 stage 是 driver 端 idle 等待(broadcast/planning/listing),不是 executor 优化目标 —— 读 top 时**先看 busy_ratio** 再决定是否下钻。
   - **`top_busy_stages[]`** 是与 `top_stages_by_duration` 并列的另一切面:只收 `busy_ratio > 0.8` 的 stage、按 `busy_ratio * duration_ms` 倒序。**这才是真正吃 CPU 值得调优的热点**(driver-side 等待 stage 自动被过滤掉)。`findings_wall_coverage` 低 + 用户报慢时,应当从这个数组里挑 stage 看执行计划。
   - `slow-stages` 行附 `busy_ratio` + 三档 `*_mb_per_task`(`input_mb_per_task` / `shuffle_read_mb_per_task` / `shuffle_write_mb_per_task`,任一 NumTasks=0 时为 0)。看 stage 偏写侧 / 读侧 / source-scan 时分别看对应字段判 partition 粒度。

### GC ratio 三层口径

三层 `gc_ratio` 分母都是 task 累加,不是 wall —— 不要用 `gc_ms / duration_ms`,多 executor 并发场景下会得到 >100% 的诡异值。

| 层级 | 字段 | 公式 |
|---|---|---|
| 应用全局 | `envelope.gc_ratio`(`app-summary` 顶层) | `total_gc_ms / total_run_ms` |
| stage 内 | `slow-stages[].gc_ratio` | `sum(task_gc) / sum(task_run)` |
| executor 累加 | `gc-pressure.by_executor[].gc_ratio` | `executor.TotalGCMs / executor.TotalRunMs` |

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
  "columns": [...],
  "data": [...]
}
```

Exceptions:
- `gc-pressure` returns `data: {by_stage: [...], by_executor: [...]}` (the only object-shaped data field)
- `diagnose` adds `summary: {critical, warn, ok, top_findings_by_impact?, findings_wall_coverage?}`. `top_findings_by_impact` is an array of `{rule_id, severity, wall_share}` sorted desc by `wall_share` — only findings with a `stage_id` evidence link and `wall_share > 0` appear here. `findings_wall_coverage` is the deduped (max-per-stage) sum of those wall shares; **< 0.05 means the bottleneck is structural — read `app-summary.top_busy_stages` instead of drilling findings**. Both fields missing (omitempty) when `app.DurationMs == 0` (no ApplicationEnd event).
- `slow-stages` and `data-skew` add `sql_executions: {<int64 id>: <description>}` at the top level. Rows reference the id via `sql_execution_id`; **rows no longer carry `sql_description`** — look it up in the top-level map. Empty/absent (omitempty) when no stage links to a SQL execution **or every linked execution carries only callsite noise** (DataFrame jobs whose description and details are both `org.apache.spark.SparkContext.getCallSite(...)`-style placeholders).

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
- `SPARK_CLI_QUIET=1` — 静默 SHS zip 下载的 stderr 进度提示(默认会打 "downloading EventLog zip from SHS for ..." 一行,免得用户以为 CLI 挂了)

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
