# Changelog

## Unreleased

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
