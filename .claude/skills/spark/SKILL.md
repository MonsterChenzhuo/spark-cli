---
name: spark-performance-diagnostics
description: Use when investigating Spark application performance, slow stages, GC pressure, data skew, or task failures. Run via the spark-cli binary on local, HDFS, or Spark History Server EventLog sources.
---

# Spark Performance Diagnostics

You have access to `spark-cli`, a single-binary CLI that parses Spark EventLogs and emits JSON envelopes. Always confirm the cluster first, then start with guided `diagnose` — it runs all rules at once while keeping stdout as one JSON envelope.

## Required input

The user must provide a Spark `applicationId` (e.g. `application_1735000000_0001`). Accept short forms (`1735000000_0001`) — the CLI normalizes them.

## Workflow

0. **Confirm cluster before reading EventLogs**:
   ```
   spark-cli config cluster list
   spark-cli config show
   ```
   Check `active_cluster`, `selected_cluster`, `clusters`, `log_dirs`, and `yarn.base_urls`.
   - If no cluster is configured and `log_dirs` is empty, ask the user for the Spark History Server / HDFS EventLog source and YARN gateway, then record it with:
     ```
     spark-cli config cluster add <name> --log-dirs <shs://...|hdfs://...|file://...> --yarn-base-urls <url> --activate
     ```
   - If multiple clusters exist and none is selected, choose with `--cluster <name>` after confirming the intended physical cluster.
   - `spark-cli diagnose <appId> --guided` enforces this SOP: it auto-selects the only configured cluster, fails when multiple clusters exist without a selection, and writes preflight notes to stderr as `{"event":...}` JSON lines. stdout remains the normal `diagnose` envelope.

1. **Always run guided diagnose first**:
   ```
   spark-cli diagnose <appId> --guided
   ```
   Read `summary.critical` and `summary.warn`. Even `severity: "ok"` rows are meaningful — they confirm a check ran. **优先看 `summary.top_findings_by_impact`**(按 `wall_share` 倒序的 `[{rule_id, severity, wall_share}]`)—— 这个数组直接告诉你哪个 finding 占整作业耗时最多,不要再自己心算。`app.DurationMs == 0`(没 ApplicationEnd 事件)或 finding 没 `stage_id` 关联时这段会缺失(omitempty)。**先看 `summary.findings_wall_coverage`**(所有非 ok finding 涉及 stage 占应用 wall 的总比例,按 stage_id 去重 + cap 1.0)—— **当它 < 0.05** 时,瓶颈基本不在 finding 列表里(常见为作业结构碎片化 / driver-side 等待 / 太多 action),应当**直接跳到 `app-summary` 看 `top_busy_stages` / `top_io_bound_stages` / `stages_total` / `jobs_total`**,而不是继续逐条下钻 finding。
   **severity 是诊断置信度,不是 ROI 优先级**:`disk_spill warn (wall_share 0.5)` 实际比 `data_skew critical (wall_share 0.05)` 更值得修;按 severity 字符串排优先级会错位,永远以 `top_findings_by_impact` 为准。

   如果用户明确说当前 Spark UI jobs/stages 没进展、EventLog 还没有有效信号,且作业启用了 Paimon diagnostics/profiler 插件,直接补一条 live 观测:
   ```
   spark-cli paimon-diagnostics <appId> --executor-id <driver|executorId>
   ```
   该命令走 YARN tracking/proxy URL 读取 `/paimon-diagnostics/*/json`,不解析 EventLog。优先读 `data[0].thread_dump.facts.state_counts`、`top_stacks`、`flamegraph` 和 `data[0].profiler.facts.artifacts`;端点未安装或暂无 profiler 文件时看 `data[0].warnings` 与 `profiler.diagnosis.category`。

2. **Drill down based on findings**:
   - `data_skew` critical → 优先**直接读 finding evidence 的 `similar_stages: [{stage_id, wall_share, skew_factor}]`**(SkewRule 在多 stage 命中时输出;primary stage_id 选 wall_share 最大那个,其余进 similar_stages 按 wall_share 倒序最多 4 条)。需要更详细排序时再 `spark-cli data-skew <appId> --top 10`。每行带 `sql_execution_id` + `wall_share`;**SQL description 文本在 envelope 顶层 `sql_executions: {<id>: <description>}` 一份共享,不再嵌入每行**;**默认 truncate 到前 500 个 rune**(`--sql-detail=full` 还原完整 SQL,`--sql-detail=none` 整段 omit)。对 DataFrame 作业自动回退到 `details` 首行;若 description / details 都是 callsite 形态则整行被过滤,所有条目都是噪音时整段 map 走 `omitempty` 缺失。**四道闸门**降级 critical:**(0) 任务时长紧致**:`p99/p50 < 1.5` → 规则直接 `ok`、行 verdict `mild`(任务时长本身就均匀,不存在真倾斜);(1) `input_skew_factor < 1.2` 且 `p99/p50 < 20`(均匀输入抖动)→ warn;(2) stage `wall_share < 1%` 且 `p99/p50 < 20`(短 stage 长尾收益太低)→ DataSkew row verdict 直接降为 mild;(3) 命中 `idle_stage` 条件(wall ≥ 30s + busy_ratio < 0.2 且 `NumTasks <= 2*MaxConcurrentExecutors`)整个跳过 —— 任务多但 IO 阻塞导致 busy_ratio 低的 stage 不再被误当 idle。极端 ratio (≥ 20) 不被任何闸门遮蔽。
   - `gc_pressure` critical → `spark-cli gc-pressure <appId>`(`data` 是 executor 行扁平数组,直接读 `data[].gc_ratio` 找压力高的 executor)。The `gc_pressure` finding now embeds `spark_executor_memory` in evidence — quote it before suggesting tuning.
   - `disk_spill` triggered → `spark-cli slow-stages <appId>` and read `spill_disk_gb`. Evidence 现在还带 `partitions`(stage 实际 task 数)和 `est_partition_size_mb`(`shuffle_read / num_tasks` 转 MB)—— 建议时把 `est_partition_size_mb` 跟 `spark_executor_memory` 比较,直接给出"partition size 已经超 executor 内存,把 shuffle.partitions 翻到 X"这种带数字的建议,不要套模板。
   - `scheduler_delay` triggered → `spark-cli slow-stages <appId>` and read `scheduler_delay_ratio` / `scheduler_delay_ms` with `wall_share`;先查 executor 供给、task locality、资源队列等待和过多小 task,不要把它当成 executor CPU 瓶颈。
   - `remote_shuffle` triggered → `spark-cli slow-stages <appId>` and read `remote_shuffle_read_ratio` / `shuffle_remote_blocks`;先查数据本地性、executor 丢失/重启、shuffle 分区布局和跨机架网络。
   - `speculative_tasks` triggered → `spark-cli slow-stages <appId>` and read `speculative_tasks` with `wall_share`;先查慢节点、数据倾斜和 GC 抖动。speculative 是 straggler 信号,不是优化建议本身。
   - `failed_tasks` triggered → if `evidence.blacklisted_hosts` is non-empty, those hosts have been excluded ≥2 times; report them by name and tell the user the failure looks node-level (hardware/network/disk), not random task flakiness. Otherwise ask for driver logs.
   - `tiny_tasks` triggered → 分区过细,evidence 现在带 `wall_share` / `wall_ms` + 多 stage 命中时含 `similar_stages`;suggestion 直接给"降到 ~N partition"的具体目标(基于"目标 task ~500ms"经验式),不必自己再算
   - `idle_stage` triggered → stage wall-clock 远大于 executor 实际工作时间(driver 端 broadcast/串行计算/调度等待),先引用 finding evidence 里的 `spark_driver_memory` / `spark_driver_memory_overhead` / `spark_sql_auto_broadcast_join_threshold` / `spark_sql_broadcast_timeout`。若这些字段缺失或需要完整配置,跑 `spark-cli spark-conf <appId>`。然后用 `spark-cli slow-stages <appId>` 看具体 stage,排查执行计划是否触发 broadcast / collect / driver-side 等待。
   - `executor_supply` triggered → 静态配置的 `spark.executor.instances` 大于 EventLog 实际注册/并发 executor。**只能说明 executor 供给不足,不能从 EventLog 直接断言 YARN 为什么没分配**;引用 evidence 里的 `configured_executor_instances` / `max_concurrent_executors` / `executors_added`。如果 `diagnose` 顶层有 `yarn`,优先读 `yarn.app.diagnostics` 与 `yarn.containers[].diagnostics/log_url`;否则在配置了 `yarn.base_urls` / `--yarn-base-urls` 后运行 `spark-cli yarn-logs <appId> --top 5` 查 RM/AM 的 pending containers、user limit、队列容量、节点标签、reserved containers、container 资源碎片。
   - Paimon native IO enabled or user asks about native IO收益 / DV / columnar reader → `spark-cli native-io <appId> --top 20`。优先读 `summary.top_phases` / `top_operations` 找 READ_BATCH、DV_FILTER、EXPORT 等阶段耗时,再看 `data[].metrics`、`throughput_mb_per_s`、`verdict`、`file_path`、`stage_id`、`task_attempt_id`。该命令读 EventLog,不是 live 页面;若 EventLog 还没写完或应用卡在 job 前,继续用 `paimon-diagnostics` / `driver-thread-dump`。
   - All `ok` but user reports slowness → `spark-cli slow-stages <appId> --top 5`

3. **For overview**: `spark-cli app-summary <appId>`。app-summary 同时输出三个互补切面,**永远要三个一起看**才能不漏瓶颈:
   - 先看 `dynamic_allocation_enabled` / `configured_executor_instances` / `executors_added` / `max_concurrent_executors`。静态申请数远大于实际 executor 时,瓶颈先按 YARN executor 供给不足处理;但原因必须去 RM/AM 查,EventLog 不含调度器拒绝原因。
   - `top_stages_by_duration[]` 按 wall 倒序(包含 driver-side 等待 stage),**先看 busy_ratio** 再决定是否下钻。
   - **`top_busy_stages[]`** —— `busy_ratio > 0.8` 的 stage 按 `busy_ratio * duration_ms` 倒序。这是真正吃 CPU 的 executor 热点;driver-side 等待 stage 自动过滤掉。
   - **`top_io_bound_stages[]`** —— `busy_ratio < 0.8` 但 `spill_disk_gb >= 0.5` 或 `shuffle_read_gb >= 1` 的 stage,按 wall_share 倒序。这是 spill / shuffle 主导的"大 wall + 低 busy"瓶颈,**只看 top_busy_stages 会全错过**(典型场景:stage spill 9 GB,executor 都在等盘,busy_ratio 仅 0.05;这种 stage 才是真值得修的)。
   - app-summary 全局 AI 字段:`avg_active_tasks`、`executor_cpu_ratio`、`scheduler_delay_ratio`、`remote_shuffle_read_ratio`、`speculative_tasks`、`peak_execution_memory_gb`。用它们先分流 CPU / scheduler / remote shuffle / straggler / memory,不要只看 `total_run_ms`。
   - `slow-stages` 行附 `wall_share`(stage.duration / app.duration)+ `busy_ratio` + 三档 `*_mb_per_task`(`input_mb_per_task` / `shuffle_read_mb_per_task` / `shuffle_write_mb_per_task`,任一 NumTasks=0 时为 0)。看 stage 偏写侧 / 读侧 / source-scan 时分别看对应字段判 partition 粒度;`wall_share` 直接给 ROI 信号,不必再 dur / app_duration_ms 自己除。新增 AI 归因字段包括 `executor_cpu_ratio`、`scheduler_delay_ratio`、`remote_shuffle_read_ratio`、`shuffle_remote_blocks`、records、`peak_execution_memory_gb`、`speculative_tasks`。

4. **For Spark config**: `spark-cli spark-conf <appId>` 直接读取 EventLog 中 `SparkListenerEnvironmentUpdate.Spark Properties`,输出 `{key,value,category,importance,tuning_hint}` 行和 `summary.parameter_hints`。当需要基于实际配置推荐参数时,先用这个命令拿当前 `spark.driver.memory`、`spark.executor.memory`、`spark.sql.shuffle.partitions`、`spark.sql.adaptive.*`、`spark.sql.autoBroadcastJoinThreshold`、`spark.sql.broadcastTimeout` 等值,再结合 diagnose finding 给建议。

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
  "contract_version": 1,
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

`contract_version` 当前固定为 `1`;不认识版本时不要猜字段含义。`app_duration_ms` 是应用 wall 时长(`SparkListenerApplicationEnd - SparkListenerApplicationStart`),omitempty 在 `app.DurationMs == 0`(没 ApplicationEnd 事件)时缺失。所有 EventLog 场景一致输出 —— 看 `wall_share` / native IO 耗时时直接换算绝对秒数,不必再额外跑 `app-summary`。

Exceptions:
- `gc-pressure` returns `data: [{executor_id, host, tasks, run_ms, gc_ms, gc_ratio, verdict}, ...]` —— 跟其他场景一致是数组(每行一个 executor),不再像早期 spec 那样有 by_stage / by_executor 双段
- `native-io` parses Paimon `SparkListenerNativeIOEvent` records. It supports both top-level `native_io_*` fields and legacy embedded `eventJson`. `summary` contains `{events_total, operations_total, reader_events, export_events, error_events, total_duration_ms, total_rows, total_bytes, throughput_mb_per_s, throughput_rows_per_s, top_phases, top_operations}`; `data[]` is ranked by `duration_ms` and includes direct Spark context, file/object fields, raw numeric `metrics`, throughput, memory, and `verdict`.
- `diagnose` adds `summary: {critical, warn, ok, top_findings_by_impact?, findings_wall_coverage?}`. `top_findings_by_impact` is an array of `{rule_id, severity, wall_share}` sorted desc by `wall_share` — `wall_share` 取**该 finding 命中所有 stage(primary + similar_stages)的 max** 而非 sum,直观对应"本规则击中的最严重 stage"。`findings_wall_coverage` is the deduped (max-per-stage) sum of those wall shares **capped at 1.0**(stage 在 wall 上并行,naive sum 可能 > 1.0,语义上不应超 100%);**< 0.05 means the bottleneck is structural — read `app-summary.top_busy_stages` / `top_io_bound_stages` instead of drilling findings**. Both fields missing (omitempty) when `app.DurationMs == 0` (no ApplicationEnd event).
- `diagnose` rules include `scheduler_delay`, `remote_shuffle`, and `speculative_tasks`. Their evidence always carries `stage_id` and, when app duration is known, `wall_share`; treat them as routing signals for the next probe rather than standalone human prose.
- `spark-conf` returns Spark Properties rows from EventLog with columns `key,value,category,importance,tuning_hint` and summary `{total, important, missing_important?, parameter_hints?}`. `parameter_hints` maps problem classes like `driver_broadcast_wait`, `shuffle_spill_or_large_partition`, `data_skew`, and `gc_pressure` to the config keys that should ground parameter recommendations.
- When YARN is configured (`yarn.base_urls` / `--yarn-base-urls`), `diagnose` may add top-level `yarn: {app, containers, warnings}`. This is RM/NM context for the same appId: read `app.state`, `app.final_status`, `app.diagnostics`, and container `log_url` / `diagnostics` before asking the user for driver logs. `diagnose` only includes log URLs; use `spark-cli yarn-logs <appId> --yarn-log-bytes 65536` when you need stderr/stdout/syslog snippets.
- `paimon-diagnostics` returns one live Spark UI row: `{overview, thread_dump, profiler, warnings}` from the Paimon tab JSON endpoints. It is for currently running Paimon read/write stalls where EventLog/Spark stages are not enough. `thread_dump.facts.top_stacks` is the aggregate flamegraph source for thread dumps; `thread_dump.diagnosis.category` summarizes the likely stack shape; `profiler.facts.artifacts[]` lists async-profiler CPU/wall/html outputs and their `open_url`.
- `data_skew` finding evidence 在多 stage 命中时含 `similar_stages: [{stage_id, wall_share, skew_factor}]`(按 wall_share 倒序最多 4 条),`top_findings_by_impact` 与 `findings_wall_coverage` 都会聚合 primary + similar_stages 的覆盖度 —— **看到 similar_stages 时直接读它,不必再回头跑 data-skew 找补**。
- `slow-stages` and `data-skew` add `sql_executions: {<int64 id>: <description>}` at the top level. **map 只包含当前 row 实际引用的 sql_id**(`--top 5` 截断后,没出现在 row 里的 SQL 不会泄露到 envelope —— 例如 `SHOW DATABASES` 这种 setup SQL)。Rows reference the id via `sql_execution_id`; **rows no longer carry `sql_description`** — look it up in the top-level map. **Description 默认 truncate 到前 500 个 rune**,过长追加 `...(truncated, total <N> chars)`;`--sql-detail=full` 还原完整 SQL,`--sql-detail=none` 整段 omit。Empty/absent (omitempty) when no stage links to a SQL execution **or every linked execution carries only callsite noise** (DataFrame jobs whose description and details are both `org.apache.spark.SparkContext.getCallSite(...)`-style placeholders).

`incomplete: true` means an `.inprogress` log was read — treat data as preliminary.

## Errors

Errors go to **stderr** as `{"error": {"code": "...", "message": "...", "hint": "..."}}`. Non-error progress/warnings use `{"event": {"code": "...", "level": "...", "message": "...", "fields": {...}}}`. Exit codes:
- `0` success
- `1` internal error (file a bug)
- `2` user error (bad flag, app not found, ambiguous)
- `3` IO/HDFS unreachable

`diagnose` returns `0` even when findings are critical — read `summary.critical` to decide.

## Useful flags

- `--log-dirs <uri,uri>` — comma-separated `file://`, `hdfs://`, `shs://host:port`, and/or `shs+https://host:port[/gateway/path]` URIs (Spark History Server REST endpoint) to search
- `--cluster <name>` — select a local named cluster profile from `config.yaml` (`active_cluster` is used by default). Profiles bind `log_dirs` and `yarn.base_urls` for the same physical cluster; explicit `--log-dirs` / `--yarn-base-urls` still override after selection.
- `--format json` — default `json`; non-json formats return FLAG_INVALID
- `--top N` — for `slow-stages` / `data-skew` / `gc-pressure` / `native-io`
- `--dry-run` — locate the log without parsing (fast sanity check)
- `--guided` — **diagnose-only**; confirm/select a named cluster before reading EventLogs. Preflight notes go to stderr as `{"event":...}` JSON lines and stdout remains the diagnose envelope. Other commands reject `--guided` with `FLAG_INVALID` instead of silently ignoring it.
- `--cache-dir <path>` — persistent cache dir (default `~/.cache/spark-cli`); cached runs report `parsed_events: 0`
- `--no-cache` — bypass the parsed-application cache for this invocation (no read, no write)
- `--shs-timeout <duration>` — HTTP timeout for `shs://` / `shs+https://` log-dirs (default `5m`;生产 zip 几个 GB 是常态,失败时 hint 直接告知此参数)
- `--yarn-base-urls <url,url>` — YARN RM/gateway base URLs, e.g. `http://host/gateway/prod/yarn`; also available as `SPARK_CLI_YARN_BASE_URLS` / yaml `yarn.base_urls`.
- `--yarn-log-bytes <N>` — for `yarn-logs`, fetch at most N bytes per log type (`stderr`, `stdout`, `syslog` by default). `diagnose` keeps logs out of the envelope and emits URLs only.
- `--yarn-log-types <type,type>` — for `yarn-logs`, select log files such as `stderr,gc.log.0.current`; `gc` expands common GC log names. Use with `--executor-id <id>` when heartbeat timeout may actually be executor Full GC.
- `--executor-id <id>` — for `driver-thread-dump`, targets Spark executor threads (empty defaults to driver); for `paimon-diagnostics`, chooses which executor's Paimon thread-dump JSON to fetch (empty defaults to driver); for `yarn-logs`, filters through Spark UI executors API and fetches that executor's container logs when available.
- `--sql-detail truncate|full|none` — `sql_executions` 中 description 的呈现:**默认 truncate**(前 500 个 rune,过长加 `...(truncated, total <N> chars)` 标记),`full` 还原原始 SQL,`none` 整段 omit。也可用 `SPARK_CLI_SQL_DETAIL` 环境变量 / yaml `sql.detail` 覆盖。
- `--no-progress` — 不输出 SHS zip `SHS_DOWNLOAD_START` / `SHS_DOWNLOAD_READY` JSON 进度事件(优先级高于 SPARK_CLI_QUIET 与 TTY 检测)。
- `--tls-insecure-skip-verify` — HTTPS SHS/YARN gateway 使用自签证书时跳过证书校验;也可用 `SPARK_CLI_TLS_INSECURE_SKIP_VERIFY=true` 或 yaml `tls.insecure_skip_verify: true`。只在确认是内网可信 gateway 时使用。
- SHS zip 持久化:同一 appID 的 zip 在 `<cache_dir>/shs/<host>/<appID>_<lastUpdated>.zip` 复用,attempt 更新时旧文件自动 sweep;`--no-cache` 旁路。
- `SPARK_CLI_QUIET` — `1`/`true` 强制静默,`0`/`false` 强制保留进度,**未设时按 stdout 是否 TTY 自动决定**(管道 / 重定向 / agent 调用默认静默,交互终端默认显示)。

## Utility commands

- `spark-cli config show` — print effective configuration with source labels (`file` / `env` / `default` / `flag`). JSON 形态适合 agent 一次拿到完整状态,而不必分别读 yaml + env 比对生效值。
- `spark-cli config cluster add <name> --log-dirs shs://... --yarn-base-urls http://... [--shs-timeout 5m] [--tls-insecure-skip-verify] [--activate]` — 把 Spark History Server 与 YARN gateway 作为同一个集群 profile 写入本地配置;排查多集群问题时优先使用,避免 SHS / YARN URL 串集群。
- `spark-cli config cluster list` — 查看本地已沉淀的集群和当前 `active_cluster`。
- `spark-cli spark-conf <appId>` — 查看应用实际 Spark 配置,包括诊断相关参数分类与调参提示。需要基于参数推荐优化方案时先跑它。
- `spark-cli yarn-logs <appId> --top 5` — fetch YARN application diagnostics, attempt/container log URLs, and bounded stderr/stdout/syslog snippets. It normalizes numeric appAttempt ids before calling the containers API and falls back to appAttempt metadata / YARN HTML log links when that API returns 400 or `{}`.
- `spark-cli yarn-logs <appId> --executor-id <id> --yarn-log-types stderr,gc --yarn-log-bytes 131072` — fetch a specific executor's stderr and common GC logs. Read `containers[].log_findings`; `type=full_gc` is strong evidence that heartbeat timeout / executor lost symptoms were caused by JVM Full GC stalls.
- `spark-cli native-io <appId> --top 20` — read Paimon native IO metrics from EventLog, including phase/operation summaries and top slow native IO segments.
- `spark-cli paimon-diagnostics <appId> --executor-id <id>` — fetch Paimon diagnostics tab JSON (`overview`, `thread_dump`, `profiler`) through Spark UI. Use this before requesting screenshots of Paimon-diagnostics pages.
- `spark-cli self-update` — update the installed binary from the latest GitHub release after checksum verification. Use `--dry-run` to inspect the target asset first or `--version vX.Y.Z` to pin a release.
- `spark-cli cache list` — 列所有 cached parsed application + SHS zip(按 size 降序),应用 cache hit 慢于预期时先看这个。
- `spark-cli cache clear [--app <id>] [--dry-run]` — 删全部 cache 或只删指定 app 的 entry;`--dry-run` 先看会删什么再确认。

## Setup if missing

If `spark-cli` is not installed, fetch the appropriate release:

```
curl -fsSL https://raw.githubusercontent.com/opay-bigdata/spark-cli/main/scripts/install.sh | bash
```

Or build from source: `go install github.com/opay-bigdata/spark-cli@latest`.

If config is missing, run `spark-cli config init` to write `~/.config/spark-cli/config.yaml` non-interactively and return JSON metadata. For production clusters, prefer `spark-cli config cluster add prod --log-dirs shs://history:18081 --yarn-base-urls http://gateway/prod/yarn --activate`. For HTTPS gateways with self-signed certificates, use `shs+https://...` plus `--tls-insecure-skip-verify`.

## Don't

- Don't parse the JSON manually — use the documented field names from `columns` to know what's in `data`.
- Don't compare `gc_ratio` across runs without checking `total_run_ms` — short stages have noisy GC ratios.
- Don't claim a problem isn't present without running `diagnose` first.
