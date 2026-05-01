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
   Read `summary.critical` and `summary.warn`. Even `severity: "ok"` rows are meaningful — they confirm a check ran.

2. **Drill down based on findings**:
   - `data_skew` critical → `spark-cli data-skew <appId> --top 10`. Each row carries `sql_execution_id` + `sql_description` so you can quote the SQL that owns the skewed stage.
   - `gc_pressure` critical → `spark-cli gc-pressure <appId>` (look at `by_executor`). The `gc_pressure` finding now embeds `spark_executor_memory` in evidence — quote it before suggesting tuning.
   - `disk_spill` triggered → `spark-cli slow-stages <appId>` and read `spill_disk_gb`. The finding's evidence already includes `spark_sql_shuffle_partitions` and `spark_executor_memory` — anchor your suggestion to the actual configured values, not generic advice.
   - `failed_tasks` triggered → if `evidence.blacklisted_hosts` is non-empty, those hosts have been excluded ≥2 times; report them by name and tell the user the failure looks node-level (hardware/network/disk), not random task flakiness. Otherwise ask for driver logs.
   - `tiny_tasks` triggered → 分区过细,建议 `coalesce` / 调低 `spark.sql.shuffle.partitions`
   - `idle_stage` triggered → stage wall-clock 远大于 executor 实际工作时间(driver 端 broadcast/串行计算/调度等待),用 `spark-cli slow-stages <appId>` 看具体 stage,然后排查执行计划
   - All `ok` but user reports slowness → `spark-cli slow-stages <appId> --top 5`

3. **For overview**: `spark-cli app-summary <appId>`

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
- `diagnose` adds `summary: {critical, warn, ok}`

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
- `--shs-timeout <duration>` — HTTP timeout for `shs://` log-dirs (default `60s`)

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
