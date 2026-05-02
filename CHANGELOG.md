# Changelog

## Unreleased

### Rounds 4-8 polish (same 2026-05-02 dogfooding session)

Output / UX:
- **`--format table` single-row scenarios go vertical** (mirrors round-3's markdown change). `app-summary` no longer prints a 1200+ char horizontal row with nested-array cells stuffed inline.
- **markdown / table header now shows app wall** (`· app: 70.5min` / `· app: 30.0s`), so humans don't have to read `app_duration_ms` and convert.
- **markdown cells escape `|` and newlines.** SQL text containing pipes (`select a | b from t`) no longer breaks the table layout into mis-aligned columns.
- **markdown / table render `envelope.sql_executions` as a section** — `### sql_executions` + per-id fenced code block (markdown) / `=== sql_executions ===` + per-id line (table). Humans using non-JSON formats now see SQL alongside stage rows.
- **Single shared `formatAppDuration` helper** between markdown / table headers; auto-picks "X.Xs" under 60s and "X.Xmin" otherwise.
- **`config init` writes optional sections as YAML comments** (`# shs:`, `# cache:`, `# sql:`) so users discover `sql.detail` etc. on first init instead of having to grep docs.

Errors / hints:
- **SHS non-200 status codes go through structured `cerrors.Error{LogUnreadable}`** with category-specific hints: 5xx → "联系运维或稍后重试", 401/403 → "v1 不支持鉴权,检查后端", other 4xx → "检查 --log-dirs 或 curl 同 URL".
- **`spark-cli unknown-foo` returns USER_ERR (rc=2)** with a `FLAG_INVALID` envelope and "see spark-cli --help" hint, instead of falling through to `INTERNAL` (rc=1).
- **`--version` flag now works** (alongside the existing `version` subcommand) with identical "spark-cli <ver>" output.

CLI / surface:
- **`--log-dirs` / `--no-progress` / `--shs-timeout` help text rewritten** to match current behavior — `--log-dirs` now lists all three schemes (`file://` / `hdfs://` / `shs://`); `--no-progress` mentions TTY auto-detect; `--shs-timeout` calls out the 5-minute default.
- **SHS HTTP requests now send `User-Agent: spark-cli/<version>`** so SHS operators can identify spark-cli traffic in their access logs.

Code-quality:
- **dispatch.go uses a `rowsToAny[T]` generic helper** instead of repeating the typed-row-to-`[]any` loop in 4 case branches.
- **`GCPressureColumns()` gains the reflection-based field-match test** — making all 4 scenarios (AppSummary / SlowStages / DataSkew / GCPressure) consistently guarded; CLAUDE.md updated from "三处反射守门" to "四处".
- **e2e covers `--format table` + `--format markdown` for all 5 scenarios** plus the `app_duration_ms` field in every envelope; new `TestE2E_VersionFlagAndCommandMatch` and `TestE2E_FormatTableAndMarkdownSmoke`.

### Round-3 polish (same 2026-05-02 dogfooding session)

- **`slow-stages` row gains `wall_share`** (mirrors `data-skew` row), saving the agent a `dur / app_duration_ms` mental division when ranking stages by ROI.
- **`sql_executions` map filtered to row-used IDs.** `BuildSQLExecutionMap` now takes an `onlyIDs map[int64]struct{}` arg; dispatch passes the SQL ids actually referenced by the current scenario rows. `slow-stages --top 5` no longer leaks unrelated SQL like `SHOW DATABASES` into the envelope.
- **Cobra "unknown command / unknown flag / missing arg" → `USER_ERR` (rc=2).** Previously fell through to `INTERNAL` (rc=1) with the user thinking they'd hit a bug; now wrapped as `cerrors.New(FLAG_INVALID, msg, "see spark-cli --help")` in root.go's Execute / RunWith. Existing structured errors pass through unchanged.
- **`spark-cli config show` lists `sql.detail`.** Was missing from the output table since the `--sql-detail` config landed; show.go now detects yaml / `SPARK_CLI_SQL_DETAIL` env / default sources.
- **`app-summary --format markdown` renders single-row scenarios as vertical key/value table** instead of a 25-column horizontal monster with nested-array cells running hundreds of chars wide. Multi-row scenarios (slow-stages / data-skew / diagnose) keep the horizontal layout — horizontal still wins for "scan many rows pick one" browsing.
- **Lower-layer `cerrors.Error` hints preserved through Locator.** `internal/eventlog/locate.go` was wrapping every `fsys.List/Stat` error as `cerrors.New(LogUnreadable, err.Error(), "")` — but when the underlying err was already a `*cerrors.Error` (SHS network errors with proper hints), `.Error()` flattened "CODE: msg (hint: ...)" into the new message and wiped the hint field. New `preserveCerror` helper passes `*cerrors.Error` through unchanged; only plain errors get re-wrapped.
- **SHS network errors all structured** with actionable hints: timeouts → `--shs-timeout` guidance; DNS / connect-refused / no-such-host → "check shs:// host spelling; curl `<endpoint>/api/v1/applications` to verify reachability"; everything else → generic SHS hint. Previously only timeouts had hints.
- **`cfg.Validate()` errors now carry hints** via runner.go's `validateHint`. `log_dirs is empty` → "run `spark-cli config init` 写默认 config,或加 --log-dirs file:///path / hdfs://nn/path / shs://host:port"; `timeout must be positive` → example value.

### Round-2 dogfooding refinements (2026-05-02 same session)

- **Envelope `app_duration_ms`**: top-level field across all five scenarios, populated from `model.Application.DurationMs` (omitted when `app.DurationMs == 0` / no ApplicationEnd event). Lets agents convert `wall_share` to absolute seconds without an extra `app-summary` round-trip.
- **`TinyTasksRule` aligned with the `similar_stages` pattern.** Was previously single-stage / map-iteration-order primary; now collects all `tasks >= 200 && p50 < 100ms` candidates, picks primary by `wall_share` desc (ties resolved by `tasks` desc), surfaces the rest as `evidence.similar_stages`. Evidence gains `wall_ms` / `wall_share`; suggestion now computes a concrete target partition count using the "~500 ms/task" heuristic (e.g. 1828 tasks @ p50=23ms → "降到 ~84 partition") instead of the generic "consider coalesce".
- **`IdleStageRule.evidence` adds `wall_share`**, so agents stop dividing `wall_ms` by `app.DurationMs` themselves.
- **Output table / markdown render struct rows** (long-standing latent bug found via `--format markdown` on the dogfooding run): `toRowSlice` now JSON-round-trips struct elements into maps, so `--format table` / `--format markdown` no longer silently produce header-only output for any scenario.
- **`gc-pressure` data-shape doc fix** — replaced the stale `{by_stage, by_executor}` description in SKILL.md / README / README.zh.md with the actual flat-array shape.
- **`SpillRule` follows the `similar_stages` pattern**, mirroring SkewRule. Suggestion now computes `advisoryPartitionSizeInBytes=64m` + the target partition count from `est_partition_size_mb`, instead of the generic "raise shuffle.partitions".
- **`SkewRule` suggestion** adds `wall_share` percentage and similar-stages count without nested brackets; AQE-skewJoin-on path now proposes concrete `skewedPartitionFactor=2 + skewedPartitionThresholdInBytes=64m`.
- **`data-skew` row default ordering** switched from `max(skew_factor, input_skew_factor)` desc to `wall_share` desc (ties resolved by skew_factor), aligning with `SkewRule.primary` selection. Wall_share-large stages no longer get pushed to the bottom by extreme-but-low-impact `input_skew_factor`.
- **`top_busy_stages` adds a `wall_share >= 1%` floor** so few-second stages with high `busy_ratio` no longer pose as "real CPU hotspots". Apps where every CPU-busy stage is sub-1% wall return an empty `top_busy_stages`, signalling the agent to read `top_io_bound_stages` instead.

### Agent-friendly UX overhaul (driven by 2026-05-02 dogfooding session)

- **BREAKING — `sql_executions` map descriptions truncated by default.** Each entry is cut to the first 500 runes (UTF-8-safe) with a `...(truncated, total <N> chars)` marker. Production ETL SQL is routinely 5K+ tokens per execution; long agent sessions used to lose context to a single envelope. `--sql-detail=full` (or `SPARK_CLI_SQL_DETAIL=full`, or `sql.detail: full` in YAML) restores the original; `--sql-detail=none` omits the entire map.
- **`data_skew` finding now selects primary stage by `wall_share`** (ties resolved by `skew_factor`), with the remaining gate-passing stages emitted as `evidence.similar_stages: [{stage_id, wall_share, skew_factor}]` (max 4, wall_share-desc, only `wall_share > 0`). Historic implementation ranked by `skew_factor` and reported a single stage — real apps where stage 14 had `wall_share 0.92` were drowned out by stage 7's extreme ratio at `wall_share 0.26`. `summary.top_findings_by_impact` and `findings_wall_coverage` now aggregate primary + similar_stages.
- **`SkewRule.isIdleStage` adds a `NumTasks > 2*MaxConcurrentExecutors` guard.** Many-task stages whose `busy_ratio` is low because executors are stalled on disk spill / shuffle wait are no longer mistakenly classified as driver-side idle (a real br_loan_em_phone_sale stage with 1000 tasks / 6 executors / `busy_ratio 0.07` was previously dropped by `SkewRule`). `IdleStageRule` itself keeps its original threshold — driver-side stages typically have very few tasks.
- **`summary.findings_wall_coverage` is now capped at 1.0.** Stages run in parallel on the wall, so naive sum of per-stage `wall_share` could exceed 1.0 (one real run hit 4.337). Cap keeps the field interpretable as "share of app wall covered by findings".
- **`top_findings_by_impact[].wall_share` uses max across primary + similar_stages**, not sum. Avoids parallel-stage inflation while still surfacing the rule's largest hit.
- **`app-summary.top_io_bound_stages`** — new orthogonal slot to `top_busy_stages`. Filters `busy_ratio < 0.8` AND (`spill_disk_gb >= 0.5` OR `shuffle_read_gb >= 1.0`), ranked by `wall_share` (limit 3). Real bottleneck stages where executors stall on disk IO (low `busy_ratio` but huge wall) used to be invisible — `top_busy_stages` filtered them out. With this view, `app-summary` exposes three orthogonal slices: by-duration (raw wall), top-busy (CPU hotspots), top-io-bound (IO stall). All three should be read together.
- **SHS zip persistent disk cache.** Downloaded zips land in `<cache_dir>/shs/<host_safe>/<appID>_<lastUpdated>.zip` (atomic tmp+rename) and are reused across CLI invocations: subsequent runs only fetch the cheap metadata JSON to compare `lastUpdated`. Stale attempts are swept on each successful download. Corrupt cache files auto-recover (delete + re-download). `--no-cache` falls back to a one-shot system tempfile. Practical effect: warm `app-summary` / `slow-stages` returns in <1s instead of waiting 4-7s for several-GB zip re-download.
- **`SPARK_CLI_QUIET` is now TTY-aware.** Unset means quiet on non-TTY stdout (pipes, agent invocations) and verbose on interactive terminals; `1`/`true` forces quiet, `0`/`false` forces verbose. The previously-dead `--no-progress` flag is now wired and overrides everything else. `NewSHS` no longer reads the env directly — `cmd/scenarios.resolveQuiet` decides once and passes `SHSOptions{Quiet: bool}`.
- **`severity` is now explicitly documented as diagnostic confidence, not ROI**. Always sort findings by `top_findings_by_impact[].wall_share` for prioritization; `disk_spill warn (wall_share 0.5)` outranks `data_skew critical (wall_share 0.05)` and the docs spell this out so agents stop sorting by severity strings.

### UX & diagnostic-coverage fixes (driven by real-app feedback)

- **SHS default timeout 60s → 5m**, plus structured `LOG_UNREADABLE` error with a `hint` that names `--shs-timeout` / `SPARK_CLI_SHS_TIMEOUT` when a fetch times out. Production EventLog zips routinely take minutes; the old 60s default forced first-time users to crash and re-read docs.
- **SHS first-fetch progress on stderr**: `bundleFor` now prints `spark-cli: downloading EventLog zip from SHS for <app> (timeout 5m0s; set SPARK_CLI_QUIET=1 to silence) ...` before the HTTP body and a `ready in <duration>` line afterwards. Set `SPARK_CLI_QUIET=1` to silence (intended for scripted callers / tests). Cache hits still need this fetch to detect V1/V2 layout — surfacing the wait removes the "is the CLI hung?" UX bug.
- **`sql_executions` map drops callsite-only noise**. `isCallSiteDescription` now also matches the `org.apache.spark.SparkContext.getCallSite(SparkContext.scala:2205)` form Spark stamps onto DataFrame submissions. `BuildSQLExecutionMap` filters entries whose final (post-fallback) description is still callsite-only — when every entry is noise the entire map is omitted from the envelope. Today's reproduction had 81 entries collapse to 0; the slow-stages payload shrank from ~30 KB to a few KB.
- **`data_skew` adds a tight-task-time gate**: when `p99/p50 < 1.5` the rule reports `ok` (and `data-skew` row verdict drops to `mild`), regardless of `input_skew_factor`. Common false positive was a single near-zero task pulling `input_skew_factor` into the thousands while task durations were uniform — the resulting "warn" wasted user attention. Extreme `p99/p50 ≥ 20` still bypasses any gate.
- **`diagnose.summary.findings_wall_coverage`** — sum of `wall_share` across all non-ok findings (deduped by `stage_id`, max per stage). Tells callers immediately how much of app wall time is explained by the rule set; values < 0.05 mean the bottleneck is structural (too many stages / driver-side waits) and the agent should jump to `app-summary.top_busy_stages` instead of drilling into findings. Omitted when `app.DurationMs == 0`.
- **`app-summary.top_busy_stages`** — parallel slot to `top_stages_by_duration`, filtered to `busy_ratio > 0.8` and ordered by `busy_ratio * duration_ms`. Driver-side idle stages no longer mask the real CPU hotspots when the agent looks at the summary.
- **`slow-stages` rows add `input_mb_per_task` and `shuffle_write_mb_per_task`** alongside the existing `shuffle_read_mb_per_task`. Write-side and source-scan stages can now expose partition granularity at a glance — previously the read-side metric was 0 for every shuffle-write stage and gave no signal.

### LLM-friendly envelope + impact-aware diagnostics

- **Breaking:** `slow-stages` and `data-skew` envelopes now expose a single top-level `sql_executions: {<int64 id>: <description>}` map; per-row `sql_description` columns were removed. Rows reference the id via `sql_execution_id`. Production logs with multi-line SQL drop from 40+ KB (per-row repetition) to a few KB (one shared map). `omitempty` keeps the field absent for non-SQL apps and for scenarios that don't need it.
- `data_skew` rule and `data-skew` rows gain a `wall_share` gate: when a stage's wall-clock is < 1% of app duration and `p99/p50 < 20`, severity downgrades to `warn` (rule) / `mild` (row verdict). Tail latency on a sub-1% stage isn't worth flagging as critical. Threshold is suppressed when `app.DurationMs <= 0` (no ApplicationEnd event) so logs without an end marker aren't blanket-downgraded. New `wall_share` field appears on rule evidence (when known) and `data-skew` rows.
- `slow-stages` rows now expose `busy_ratio` (same `TotalRunMs / (wall * effective_slots)` formula as `app-summary.top_stages_by_duration[]`) and `shuffle_read_mb_per_task` (`TotalShuffleReadBytes / NumTasks`, MiB). Lets agents distinguish driver-idle stages and identify under-partitioned shuffles without manual arithmetic.
- `disk_spill` rule evidence now includes `partitions` (stage `NumTasks`) and `est_partition_size_mb` (`shuffle_read / num_tasks` in MiB). Suggestions can now be anchored to the actual partition size and compared against `spark.executor.memory`, rather than emitting generic "raise shuffle.partitions" advice.
- `diagnose` envelope's `summary` adds `top_findings_by_impact: [{rule_id, severity, wall_share}]` ranked desc by `wall_share`. Only findings with a `stage_id` evidence link and `wall_share > 0` appear; the array is `omitempty` when `app.DurationMs == 0`. Agents and humans read priority directly without re-drilling per finding.
- Reflection-based column contract tests added for `SlowStageRow` ↔ `SlowStagesColumns()` and `DataSkewRow` ↔ `DataSkewColumns()`, mirroring the existing `app_summary` guard.

### Diagnostic accuracy & readability fixes

- `slow-stages` rows now expose `gc_ratio` (`sum(task_gc) / sum(task_run)`) so callers stop computing `gc_ms / duration_ms` and getting >100% values from multi-executor concurrency.
- `app-summary.top_stages_by_duration[]` rows now expose `busy_ratio` so driver-side idle stages (broadcast / planning / file listing) are visible at a glance instead of masquerading as the slowest "real" stages.
- `data_skew` rule downgrades critical → warn when input is uniform (`input_skew_factor < 1.2`) and `p99/p50 < 20`. Extreme ratios (≥ 20) still report critical regardless of input distribution. Same gate applies to `data-skew` row verdicts.
- `data_skew` rule skips stages that match `idle_stage` criteria (wall ≥ 30s and `busy_ratio < 0.2`) — task-time long tails on idle stages are scheduling jitter, not skew.
- `data_skew` finding evidence now includes `input_skew_factor`.
- `stageSQL` falls back to `details` first line when `description` is Spark's default `getCallSite at SQLExecution.scala:74` callsite or empty (typical DataFrame API submission). `data-skew` and `slow-stages` row `sql_description` becomes useful for DataFrame jobs.

### Spark History Server EventLog source

- New `shs://host:port` scheme for `--log-dirs`. spark-cli pulls `GET /api/v1/applications/<id>/<attempt>/logs` (a zip body) and exposes its entries through the existing `fs.FS` abstraction, so the locator, decoder, rules, and parsed-application cache all work transparently against a Spark History Server.
- The largest numeric `attemptId` reported by `/api/v1/applications/<id>` is auto-selected. When SHS reports an attempt with no `attemptId` field (Spark 3.4+ single-attempt default), spark-cli now drops the attempt segment and fetches `/api/v1/applications/<id>/logs` directly — fixes APP_NOT_FOUND against attempt-less apps.
- New flag `--shs-timeout`, env var `SPARK_CLI_SHS_TIMEOUT`, and YAML key `shs.timeout` (default `5m` since the unreleased 60s→5m bump above; original release shipped `60s`). `spark-cli config show` reports the resolved value and its source.
- HTTP only — TLS, Basic Auth, Bearer Token, and Kerberos are out of scope for v1.
- Zip bodies up to 256 MiB are decoded in memory; larger or unknown-length responses spill to a `os.CreateTemp` file that is removed when the process exits.
- **Known caveat**: even when the parsed-application cache hits, every invocation still downloads the zip — `Locator.Resolve` must read zip contents to decide V1 vs V2 layout. A persistent on-disk zip cache is on the roadmap.

### Application cache layer

- `internal/cache` persists the parsed `*model.Application` as `gob+zstd` blobs under `$XDG_CACHE_HOME/spark-cli/` (or `~/.cache/spark-cli/`). The first command on a given `appId` parses normally; subsequent commands skip Open + Decode + Aggregate and return in <300 ms (envelope `parsed_events=0` on hit).
- New flags `--cache-dir <path>` and `--no-cache` (bypass read+write). New env var `SPARK_CLI_CACHE_DIR`. New YAML key `cache.dir`.
- Cache invalidates on V1 source mtime/size change or any V2 part change. `.inprogress` logs are never cached. `spark-cli config show` reports the resolved `cache.dir` and its source (flag/env/file/default).
- Cache failures (corrupt files, schema mismatch, write errors) degrade silently to "miss + reparse" — never surfaced as CLI errors.
- `internal/stats.Digest` now implements `gob.GobEncoder` / `GobDecoder` so the cached `*Application` round-trips quantile state.
- `internal/fs.FileInfo` exposes `ModTime` (UnixNano) for cache-key invalidation.

### Diagnostic rules — SparkConf-aware suggestions
- `internal/eventlog` now decodes `SparkListenerEnvironmentUpdate`, populating `Application.SparkConf` with the runtime Spark Properties.
- `disk_spill` / `gc_pressure` / `data_skew` rules surface the relevant configs (`spark.sql.shuffle.partitions`, `spark.executor.memory`, `spark.sql.adaptive.skewJoin.enabled`, etc.) in both `evidence` and `suggestion`. The skew rule's hint flips when AQE skewJoin is already enabled — instead of "enable AQE", it suggests tuning `skewedPartitionFactor`.

### SQL execution attached to slow / skewed stages
- `slow-stages` and `data-skew` rows now include `sql_execution_id` and `sql_description` columns. The link is built from `SparkListenerJobStart` properties (`spark.sql.execution.id`) plus `SparkListenerSQLExecutionStart`. Stages outside any SQL execution emit `sql_execution_id: -1` and an empty description.
- `Application` exposes new fields `SparkConf`, `SQLExecutions`, `StageToSQL`.

### Failed-tasks rule — node-level pattern detection
- `internal/eventlog` now decodes `Node{Blacklisted,Excluded}ForStage` and `Executor{Blacklisted,Excluded}ForStage` events into `Application.Blacklists`.
- `failed_tasks` rule escalates to `critical` when the same host appears in ≥2 blacklist events even at low overall failure ratio, and surfaces `blacklisted_hosts` / `blacklist_node_events` / `blacklist_executor_events` in evidence with a node-targeted suggestion. Random task flakiness still reports the original message.

### EventLog locator
- `internal/eventlog/locate.go` now matches V2 directories with an optional `_<attempt>` suffix (`eventlog_v2_<appId>` or `eventlog_v2_<appId>_<n>`). When multiple attempts coexist the highest is selected, mirroring Spark History Server. Previously the locator required strict equality and could not find Spark-written rolling logs whose directory name carried the attempt counter.
- `resolveV1` now applies the same attempt-suffix tolerance to V1 single-file logs (`application_<id>_<attempt>`), produced when `spark.eventLog.rolling.enabled=false`. Bare-name files still beat attempt-suffixed siblings; otherwise the highest attempt wins. Codec / `.inprogress` suffixes continue to work on top.

### HDFS
- `internal/fs/hdfs.go` + new `internal/fs/hdfs_conf.go` — load Hadoop XML config (`core-site.xml` / `hdfs-site.xml`) and resolve HA NameService addresses via `hadoopconf.Load` + `hdfs.ClientOptionsFromConf`.
- New flag `--hadoop-conf-dir <path>`, env var `SPARK_CLI_HADOOP_CONF_DIR`, and YAML key `hdfs.conf_dir`. Auto-discovers from `HADOOP_CONF_DIR`, then `HADOOP_HOME/etc/hadoop`, then `HADOOP_HOME/conf`. Falls back to URI literal `host:port` when no conf is found.
- `spark-cli config show` now reports `hdfs.conf_dir` and its source (flag/env/file/default); `spark-cli config init` accepts an optional Hadoop conf dir.
- Kerberos / SASL / TLS remain unsupported — simple auth + HA only.

### CI / Release
- Single-job `ci.yml` now enforces `go.mod` Go version, `go mod tidy` cleanliness, gofmt, golangci-lint v2 (via `go run`), race-tested unit suite, ldflag-versioned build, smoke checks, e2e dry-run with `-tags=e2e`, and SKILL.md frontmatter lint.
- `release.yml` triggers on `push` to `main` (auto-bumps the patch tag) and on `v*` tag pushes; serialised by a `release` concurrency group.
- `.goreleaser.yml` archive now bundles `README.zh.md` and `CHANGELOG.zh.md` alongside the existing English copies and the bundled skill.
- `.golangci.yml` adds the `formatters` block (gofmt + goimports), errcheck function exclusions, and excludes `dist/` from issue scanning.

### Installer
- `scripts/install.sh` rewritten to match the hbase-metrics-cli installer: SHA-256 checksum verification, redirect-then-API latest-tag resolution, sudo fallback, skill-tree mirroring, and env overrides `VERSION` / `PREFIX` / `SKILL_DIR` / `NO_SUDO` / `NO_SKILL` / `REPO`.
- **Breaking:** previous env vars `SPARK_CLI_BIN_DIR`, `SPARK_CLI_VERSION`, and `SPARK_CLI_SKILL_DIR` were renamed to `PREFIX`, `VERSION`, and `SKILL_DIR` respectively. Default install dir moved from `~/.local/bin` to `/usr/local/bin` (override with `PREFIX=...`).
- Default repo slug fixed to `MonsterChenzhuo/spark-cli`; README install snippets updated.

## v0.1.0 — 2026-04-29

Initial MVP release.

### Scenarios
- `app-summary` — application-level overview (executors, stages, tasks, GC ratio, top stages by duration)
- `slow-stages` — stages ranked by wall-clock duration with task percentiles
- `data-skew` — skewed stages with `skew_factor`, `input_skew_factor`, and `severe`/`warn`/`mild` verdict
- `gc-pressure` — per-executor GC ratio classification
- `diagnose` — runs 5 rules (data_skew, gc_pressure, disk_spill, failed_tasks, tiny_tasks) and emits a single envelope

### Output formats
- JSON (default, canonical contract for AI agents)
- Table (aligned text)
- Markdown (pipe tables for chat embedding)

### EventLog support
- V1 single-file logs (with `.inprogress`, `.zstd`, `.lz4`, `.snappy` suffix detection)
- V2 rolling directories (`eventlog_v2_<id>/events_<n>_*`) with concatenated multi-part decoding
- File system abstraction over `file://` and `hdfs://` URIs

### Agent integration
- Bundled `.claude/skills/spark/SKILL.md` teaches Claude Code the diagnose-first workflow
- Structured stderr errors with codes `APP_NOT_FOUND`, `APP_AMBIGUOUS`, `LOG_UNREADABLE`, `LOG_PARSE_FAILED`, `LOG_INCOMPLETE`, `FLAG_INVALID`, `INTERNAL`

### Distribution
- `scripts/install.sh` one-liner installer
- Multi-platform release artifacts (linux/darwin × amd64/arm64) via goreleaser
