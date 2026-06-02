# SparkMeasure AI Metrics Design

## Context

`spark-cli` is an AI-facing Go CLI that parses Spark EventLog files and emits one JSON envelope per scenario. The adjacent `sparkMeasure` project is a Scala/Python live Spark listener library. Its listener runtime, notebook APIs, and monitoring sinks do not fit this repository, but its metric vocabulary is useful: it captures task/stage metrics that Spark already writes into EventLog and turns them into resource-consumption summaries.

This design absorbs the useful metric semantics from `sparkMeasure` while preserving the current contract:

- stdout remains a single JSON object per command.
- scenario envelopes keep `contract_version: 1`.
- no table, markdown, or prose output is added.
- new fields are shaped for AI agents, not humans.

## Goal

Expose additional EventLog-derived performance signals that help an AI agent decide whether Spark wall time is dominated by executor CPU, scheduler delay, remote shuffle, spill, GC, speculative retries, or under-provisioned executors.

## Non-Goals

- Do not port `sparkMeasure` Scala/Python listeners.
- Do not add notebook APIs, Prometheus/Kafka/Influx sinks, or online instrumentation.
- Do not add broad task-level dumps by default.
- Do not optimize for human-readable reports.

## AI-Only Output Principles

New fields must be compact and decision-oriented:

- Prefer ratios and top offenders over raw exhaustive lists.
- Include `wall_share` or stage identity whenever a signal affects prioritization.
- Keep field names stable and explicit.
- Use numbers instead of formatted strings.
- Avoid duplicating the same signal at many levels unless it changes the agent decision.

The agent should be able to answer:

- Is this stage executor-busy, scheduler-delayed, shuffle-waiting, spill-bound, or GC-bound?
- Which stage has the highest ROI?
- Which command should be run next, if any?

## Metrics To Absorb

From `sparkMeasure` task/stage metric vocabulary, the useful EventLog-backed additions are:

- `executor_cpu_ms`, converted from Spark nanoseconds to milliseconds.
- `executor_deserialize_ms`.
- `result_serialization_ms`.
- `scheduler_delay_ms`, computed as task duration minus executor run, deserialize, result serialization, and getting-result time.
- `getting_result_ms`.
- `result_size_bytes`, aggregated as max per stage and app.
- `peak_execution_memory_bytes`, aggregated as max per stage and app.
- input/output records.
- shuffle records read/written.
- shuffle total/local/remote blocks fetched.
- local/remote shuffle read bytes.
- shuffle remote read ratio.
- speculative task count.

These fields are already present in Spark task end events and do not require live listeners.

## Model Changes

Extend `model.TaskMetrics` with the additional raw metrics. Extend `model.Stage`, `model.Executor`, and `model.Application` only where aggregation is needed by AI-facing scenarios or rules.

Aggregation rules:

- CPU and time metrics are summed.
- scheduler delay is clamped at zero.
- `result_size_bytes` and `peak_execution_memory_bytes` use max for stage/app summaries.
- shuffle read bytes continue to represent local plus remote bytes.
- remote shuffle ratio is computed as `remote_shuffle_read_bytes / shuffle_read_bytes`, returning `0` when the denominator is zero.
- speculative tasks are counted from `TaskInfo.Speculative`.

Changing model field names or types requires bumping the cache schema version. Adding fields does not.

## Scenario Changes

### app-summary

Add compact global signals:

- `avg_active_tasks`: `total_run_ms / duration_ms` when duration is known.
- `executor_cpu_ratio`: `total_executor_cpu_ms / total_run_ms`.
- `scheduler_delay_ratio`: `total_scheduler_delay_ms / total_task_duration_ms`.
- `remote_shuffle_read_ratio`.
- `speculative_tasks`.
- `peak_execution_memory_gb`.

These are top-level row fields because they help the agent decide the first branch of diagnosis without another command.

### slow-stages

Add per-stage signals:

- `executor_cpu_ratio`.
- `scheduler_delay_ms`.
- `scheduler_delay_ratio`.
- `remote_shuffle_read_ratio`.
- `shuffle_remote_blocks`.
- `shuffle_total_blocks`.
- `records_read`.
- `records_written`.
- `shuffle_records_read`.
- `shuffle_records_written`.
- `peak_execution_memory_gb`.
- `speculative_tasks`.

Do not add per-task rows. The existing `top` behavior keeps output bounded.

### diagnose

Add findings only when they produce clear AI action:

- `scheduler_delay`: flags stages where scheduler delay is a meaningful wall contributor.
- `remote_shuffle`: flags stages dominated by remote shuffle reads.
- `speculative_tasks`: flags meaningful speculative execution, especially when concentrated in high-wall-share stages.

Each finding must include `stage_id`, `wall_share` when known, direct ratios, and a concrete next step. `top_findings_by_impact` should continue to order by wall impact rather than severity.

## Tests

Use TDD:

- Add model aggregator tests for each new metric.
- Add decoder tests proving JSON EventLog task fields are parsed with correct unit conversion.
- Add scenario column reflection tests where row fields change.
- Add scenario tests for app-level and stage-level derived ratios.
- Add rule tests for scheduler delay, remote shuffle, and speculative task findings.
- Keep e2e envelope contract tests green.

## Documentation

Because output contract and AI workflow change, update:

- `AGENTS.md`
- `CLAUDE.md`
- `.agents/skills/spark/SKILL.md`
- `.claude/skills/spark/SKILL.md`
- `README.md`
- `README.zh.md`
- `CHANGELOG.md`
- `CHANGELOG.zh.md`

Docs must describe how an AI agent should interpret the new ratios and why `severity` is not the same as ROI.

## Risks

- JSON envelope size can grow if too many raw fields are added. Keep task-level detail out of default output.
- Scheduler delay is a derived metric and can be noisy if Spark event timestamps are odd. Clamp negative values and present it as a ratio, not a standalone verdict.
- Some EventLogs may omit fields. Missing values should naturally decode as zero and not create false positives.
