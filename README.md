# spark-cli

A single-binary CLI for diagnosing Apache Spark application performance from EventLogs. Designed for autonomous AI agents: successful stdout is JSON-only, stderr is newline-delimited JSON (`{"error":...}` or `{"event":...}`) for recovery and progress.

## Quick start

```bash
spark-cli diagnose application_1735000000_0001 --guided
```

If `diagnose` flags `data_skew`:

```bash
spark-cli data-skew application_1735000000_0001 --top 10
```

Each row carries `input_skew_factor` alongside `skew_factor`. `data_skew` ladder downgrades severity through four gates: tight `p99/p50 < 1.5` ⇒ `ok` (uniform task time isn't skew); uniform `input_skew_factor < 1.2` + moderate ratio; stage `wall_share < 1%`; idle stage candidates (busy < 0.2). Extreme `p99/p50 ≥ 20` bypasses every gate. `slow-stages` rows expose `gc_ratio`, `busy_ratio`, three `*_mb_per_task` fields, and sparkMeasure-derived AI signals such as `scheduler_delay_ratio`, `remote_shuffle_read_ratio`, `executor_cpu_ratio`, `speculative_tasks`, and `peak_execution_memory_gb`. `app-summary` ships `top_stages_by_duration[]`, `top_busy_stages[]`, `top_io_bound_stages[]`, plus global ratios (`avg_active_tasks`, `executor_cpu_ratio`, `scheduler_delay_ratio`, `remote_shuffle_read_ratio`) for first-branch diagnosis. `diagnose.summary` adds `top_findings_by_impact: [{rule_id, severity, wall_share}]` ranked desc plus `findings_wall_coverage`; new `scheduler_delay`, `remote_shuffle`, and `speculative_tasks` findings use the same wall-impact ordering. SQL description text moved out of every row into a single `envelope.sql_executions: {<id>: <description>}` map (slow-stages / data-skew envelopes only); callsite-only DataFrame placeholders are filtered out and the entire map is omitted when every entry would be noise.

## Install

### One-liner (recommended)

Installs the latest release binary into `/usr/local/bin` and the bundled agent
skills into `~/.claude/skills/spark/` and `~/.agents/skills/spark/`. Re-run the
same command to upgrade.

```bash
curl -fsSL https://raw.githubusercontent.com/MonsterChenzhuo/spark-cli/main/scripts/install.sh | bash
```

Common overrides:

```bash
# pin a version
curl -fsSL https://raw.githubusercontent.com/MonsterChenzhuo/spark-cli/main/scripts/install.sh | VERSION=v0.1.0 bash

# install to a non-sudo path, skip the skill
curl -fsSL https://raw.githubusercontent.com/MonsterChenzhuo/spark-cli/main/scripts/install.sh | PREFIX="$HOME/.local/bin" NO_SKILL=1 bash
```

Supported envs: `VERSION`, `PREFIX`, `CLAUDE_SKILL_DIR`, `AGENTS_SKILL_DIR`,
`SKILL_DIR` (legacy alias for `CLAUDE_SKILL_DIR`), `NO_SUDO`, `NO_SKILL`,
`REPO`. See `scripts/install.sh` header for details.

After `spark-cli` is installed, update the local binary in place with:

```bash
spark-cli self-update
```

Use `spark-cli self-update --dry-run` to see the release asset first, or
`spark-cli self-update --version vX.Y.Z` to pin a release. If the installed
binary lives in a protected directory such as `/usr/local/bin`, run the command
with permission to write there or pass `--install-dir "$HOME/.local/bin"`.

### From source

```bash
go install github.com/opay-bigdata/spark-cli@latest
```

### From release (manual)

Download the archive for your OS from GitHub Releases, then `tar -xzf … && mv spark-cli /usr/local/bin/`.

## Configure

```bash
spark-cli config init       # non-interactive; writes ~/.config/spark-cli/config.yaml and returns JSON
```

```yaml
log_dirs:
  - file:///var/log/spark-history
  - hdfs://mycluster/spark-history     # HA NameService logical name (recommended)
  # - hdfs://nn:8020/spark-history     # or an explicit host:port
  # - shs://history.example.com:18081  # Spark History Server REST API
  # - shs+https://gateway.example.com/sparkhistory  # HTTPS SHS gateway
hdfs:
  user: hadoop
  conf_dir: /etc/hadoop/conf           # optional; auto-discovered via HADOOP_CONF_DIR / HADOOP_HOME if empty
shs:
  timeout: 5m  # default; production EventLog zips often need minutes — first fetch emits SHS_DOWNLOAD_START / SHS_DOWNLOAD_READY JSON events on stderr (silence with SPARK_CLI_QUIET=1)
tls:
  insecure_skip_verify: false  # set true only for HTTPS gateways with self-signed certificates
yarn:
  base_urls:
    - http://203.123.81.20:7765/gateway/hadoop-prod/yarn  # optional RM/gateway prefix
timeout: 30s
```

Override per-invocation via `--log-dirs` / `--yarn-base-urls` /
`--tls-insecure-skip-verify`, or env vars `SPARK_CLI_LOG_DIRS`,
`SPARK_CLI_YARN_BASE_URLS`, and `SPARK_CLI_TLS_INSECURE_SKIP_VERIFY`.

For production use, prefer named cluster profiles so the Spark History Server
and YARN gateway for the same cluster are selected together:

```yaml
active_cluster: prod
clusters:
  prod:
    log_dirs:
      - shs://history.example.com:18081
    yarn:
      base_urls:
        - http://203.123.81.20:7765/gateway/hadoop-prod/yarn
    shs:
      timeout: 5m
    tls:
      insecure_skip_verify: false
```

```bash
spark-cli config cluster add prod \
  --log-dirs shs://history.example.com:18081 \
  --yarn-base-urls http://203.123.81.20:7765/gateway/hadoop-prod/yarn \
  --activate
# For a self-signed HTTPS gateway:
spark-cli config cluster add id \
  --log-dirs shs+https://gateway.example.com/component/Spark2x/JobHistory2x/89 \
  --yarn-base-urls https://gateway.example.com/component/Yarn/ResourceManager/25 \
  --tls-insecure-skip-verify
spark-cli config cluster list
spark-cli --cluster prod diagnose application_1772605260987_35693 --guided
```

`active_cluster` is used by default. `--cluster <name>` selects another local
profile for one invocation. Explicit `--log-dirs` / `--yarn-base-urls` flags
still win after cluster selection for ad-hoc debugging.

### Diagnostic SOP

For production triage, use the guided path so cluster selection is confirmed
before the EventLog is read:

```bash
spark-cli config cluster list
spark-cli config show
spark-cli diagnose application_1772605260987_35693 --guided
```

If no cluster is configured, record one first:

```bash
spark-cli config cluster add prod \
  --log-dirs shs://history.example.com:18081 \
  --yarn-base-urls http://203.123.81.20:7765/gateway/hadoop-prod/yarn \
  --activate
```

`--guided` keeps stdout as the normal `diagnose` JSON envelope. Preflight events
go to stderr as `{"event":...}` JSON lines: it selects the only configured cluster automatically, fails when
multiple clusters exist and none is selected, and warns when `yarn.base_urls` is
missing so live YARN/thread probes will need a URL later. See
[`docs/examples/diagnostic-sop.md`](docs/examples/diagnostic-sop.md) for the
full agent workflow.

### HDFS configuration

- Pure-Go client (`github.com/colinmarc/hdfs/v2`); **reads** `core-site.xml` / `hdfs-site.xml` and honors HA NameService entries.
- NameNode address resolution (highest → lowest priority):
  1. `--hadoop-conf-dir <path>` flag
  2. `SPARK_CLI_HADOOP_CONF_DIR` env
  3. `hdfs.conf_dir` in `config.yaml`
  4. `HADOOP_CONF_DIR` env
  5. `HADOOP_HOME/etc/hadoop` or `HADOOP_HOME/conf`
  6. Falls back to the literal `host:port` from `--log-dirs` (no HA logical-name support in this mode)
- HDFS user resolution: `--hdfs-user` → `SPARK_CLI_HDFS_USER` → `hdfs.user` → `$USER`. Note: it reads `$USER`, **not** Hadoop's `$HADOOP_USER_NAME`.
- Kerberos / SASL / TLS are **not supported for HDFS**; this targets simple-auth HDFS clusters only.

### Spark History Server

Point `--log-dirs` at a Spark History Server REST endpoint and spark-cli will
fetch the EventLog over `GET /api/v1/applications/<id>/<attempt>/logs` (a zip
body) and treat it like any other source — locator, decoder, rules, and the
parsed-application cache all work transparently.

```bash
spark-cli diagnose application_1771556836054_861265 \
  --log-dirs shs://history.example.com:18081
```

- The latest numeric `attemptId` is auto-selected.
- Use `shs://` for HTTP endpoints and `shs+https://` for HTTPS gateways. Basic Auth, Bearer token, and Kerberos are still unsupported.
- For private HTTPS gateways with self-signed certificates, explicitly set `tls.insecure_skip_verify: true`, `SPARK_CLI_TLS_INSECURE_SKIP_VERIFY=true`, or pass `--tls-insecure-skip-verify`. This setting applies to SHS and YARN gateway clients.
- Timeout precedence (highest → lowest): `--shs-timeout` flag → `SPARK_CLI_SHS_TIMEOUT` env → `shs.timeout` in YAML → default `5m`. Timeout errors return `LOG_UNREADABLE` with a `hint` naming the flag — no need to grep docs after the first failure.
- Progress events (`{"event":{"code":"SHS_DOWNLOAD_START",...}}` / `SHS_DOWNLOAD_READY`) follow `--no-progress` flag → `SPARK_CLI_QUIET` env (`1/true` 静默, `0/false` 强制显示) → stdout TTY 检测;**agent 重定向 stdout 时默认静默,交互终端默认显示**。
- **Persistent on-disk zip cache** (since v0.x): the downloaded zip is written to `<cache_dir>/shs/<host>/<appID>_<lastUpdated>.zip` (atomic tmp+rename) and reused across CLI invocations. Subsequent commands on the same appID only fetch the cheap metadata JSON to compare `lastUpdated`; on a hit the zip is read from disk. Stale attempts (older `lastUpdated` for the same appID) are swept on each successful download. `--no-cache` falls back to a one-shot system tempfile.
- Zip bodies up to 256 MiB without disk caching are decoded in memory; with disk caching enabled the response always lands on disk via tmp+rename.
- `shs://` / `shs+https://` support gateway path prefixes, for example
  `shs+https://host:7765/gateway/prod/sparkhistory`; the CLI preserves the prefix
  when calling `/api/v1/applications/...`.

### YARN logs

When `yarn.base_urls` is configured, `diagnose` appends a top-level `yarn`
payload to the Spark EventLog diagnosis. It includes RM application state,
diagnostics, and latest-attempt container log URLs. It does not embed log bodies
by default, keeping `diagnose` small.

To inspect NodeManager logs directly:

```bash
spark-cli yarn-logs application_1772605260987_20682 \
  --yarn-base-urls http://203.123.81.20:7765/gateway/hadoop-prod/yarn \
  --top 5 --yarn-log-bytes 65536
```

`yarn-logs` uses RM REST to find the application user, attempts, and containers.
When a gateway rejects `/appattempts/<id>/containers` with 400 or returns an
empty container payload, it falls back to appAttempt metadata and YARN HTML log
links. It then builds gateway URLs like
`/nodemanager/node/containerlogs/<container>/<user>?scheme=http&host=<nm>&port=<port>`
and fetches the first N bytes of `stderr` / `stdout` / `syslog`.

To pull a specific Spark executor and GC log files:

```bash
spark-cli yarn-logs application_1772605260987_20682 \
  --yarn-base-urls http://203.123.81.20:7765/gateway/hadoop-prod/yarn \
  --executor-id 7 \
  --yarn-log-types stderr,gc \
  --yarn-log-bytes 131072
```

For `yarn-logs`, `--executor-id` uses Spark UI's executors REST endpoint when
available and returns `spark_executor_id` plus the container log URL. `gc` in
`--yarn-log-types` expands to common GC log names such as `gc.log.0.current`;
detected Full GC evidence is surfaced as `log_findings`.

To inspect driver-side stalls before jobs are submitted, fetch Spark UI thread
dumps through the YARN tracking/proxy URL:

```bash
spark-cli driver-thread-dump application_1772605260987_20765 \
  --yarn-base-urls http://203.123.81.20:7765/gateway/hadoop-prod/yarn \
  --executor-id driver \
  --thread-summary-only
```

The output starts with `diagnosis`, `main_thread`, and `interesting_threads`,
highlighting whether the driver is waiting in `runJob/collect`, stuck in Spark SQL
planning / `CollapseProject`, inside Paimon schema validation, or whether an executor
task is spending time in projection/codegen/shuffle write. Raw Spark UI thread stacks are still
emitted by default; add `--thread-summary-only` to return just the compact summary.
`--executor-id` can also target a specific executor when the driver is already
waiting for a submitted job.

If the Spark application was started with Paimon's diagnostics/profiler plugin,
fetch the tab's AI-readable JSON directly:

```bash
spark-cli paimon-diagnostics application_1772605260987_20765 \
  --yarn-base-urls http://203.123.81.20:7765/gateway/hadoop-prod/yarn \
  --executor-id 7
```

The command reads Spark UI endpoints `/paimon-diagnostics/json`,
`/paimon-diagnostics/threadDump/json?executorId=<id>`, and
`/paimon-diagnostics/profiler/json`. Output includes `overview`, `thread_dump`,
and `profiler` objects as produced by the Paimon tab. Agents should read
`thread_dump.facts.state_counts`, `thread_dump.facts.top_stacks`,
`thread_dump.facts.flamegraph`, and `profiler.facts.artifacts` before parsing
HTML. Missing endpoints are returned as `warnings` instead of failing the whole
command.

### Cache

The first invocation on a new EventLog persists the parsed `*model.Application`
to `<cache_dir>/<appId>.gob.zst`. Subsequent commands on the same `appId` skip
parsing and return in <300 ms regardless of source size (the envelope's
`parsed_events` is `0` on a hit).

| Source | Cache dir |
|---|---|
| `--cache-dir` flag | (highest priority) |
| `SPARK_CLI_CACHE_DIR` env var | |
| `config.yaml: cache.dir` | |
| `$XDG_CACHE_HOME/spark-cli` | |
| `~/.cache/spark-cli` | (default) |

`--no-cache` bypasses both the read and the write for the current invocation.
`.inprogress` logs are never cached. The cache invalidates automatically when
the source file's mtime or size changes (V1) or when any part's mtime / total
size / part count changes (V2). All cache failures (corrupt files, schema
mismatch, write errors) degrade silently to "miss + reparse".

## Commands

| Command | Purpose |
|---|---|
| `spark-cli diagnose <appId>` | Run all rules; agent's first stop |
| `spark-cli app-summary <appId>` | Application-level overview |
| `spark-cli spark-conf <appId>` | Spark configuration from the EventLog, categorized with tuning hints |
| `spark-cli slow-stages <appId>` | Stages by wall time |
| `spark-cli data-skew <appId>` | Skewed stages |
| `spark-cli gc-pressure <appId>` | GC ratio per stage / executor |
| `spark-cli native-io <appId>` | Paimon native IO EventLog metrics and phase/operation summary |
| `spark-cli yarn-logs <appId>` | Fetch YARN diagnostics and container log snippets |
| `spark-cli driver-thread-dump <appId>` | Fetch Spark UI driver/executor thread dump through YARN tracking/proxy URL |
| `spark-cli paimon-diagnostics <appId>` | Fetch Paimon diagnostics thread-dump/profiler JSON through Spark UI |
| `spark-cli config show` | Print effective configuration JSON (yaml / env / default sources) |
| `spark-cli config cluster add <name>` / `config cluster list` | Persist and inspect named cluster profiles |
| `spark-cli cache list` / `cache clear [--app <id>] [--dry-run]` | Inspect / prune the parsed-application + SHS zip caches |
| `spark-cli self-update` (aliases `update`, `upgrade`) | Download the latest release binary, verify checksum, and replace the local executable |
| `spark-cli version` (also `--version`) | Print spark-cli version JSON |
| `spark-cli --help` / `spark-cli help <command>` | Print command metadata JSON |

Scenario and live diagnostic commands accept `--format json` only. Non-JSON
formats are rejected with `FLAG_INVALID`; utility commands also return JSON by
default. Common diagnostic flags include `--top N`, `--dry-run`, `--guided`, `--log-dirs`,
`--cluster`, `--cache-dir`, `--no-cache`, `--shs-timeout`, `--no-progress`,
`--tls-insecure-skip-verify`, `--yarn-base-urls`, `--yarn-log-bytes`,
`--yarn-log-types`, `--executor-id`, and `--sql-detail truncate|full|none`
(default `truncate` — first 500 runes of the SQL description with a
`...(truncated, total <N> chars)` marker; `full` keeps the original; `none`
omits the entire `sql_executions` map). `--guided` is diagnose-only:
`spark-cli diagnose <appId> --guided`. Other commands reject it with
`FLAG_INVALID` instead of silently ignoring the flag. Shell completion output
is disabled so successful stdout stays JSON-only.

## For AI agents

The repo ships `.agents/skills/spark/SKILL.md` and `.claude/skills/spark/SKILL.md`.
Agent clients load the matching skill to follow the cluster-confirmation SOP and guided diagnose-first workflow.

## Output contract

```json
{
  "scenario": "data-skew",
  "contract_version": 1,
  "app_id": "...",
  "log_path": "hdfs://...",
  "log_format": "v1",
  "compression": "zstd",
  "incomplete": false,
  "parsed_events": 482113,
  "elapsed_ms": 1842,
  "columns": [...],
  "data": [...],
  "sql_executions": {
    "0": "select count(*) from orders where dt = '2026-05-01'"
  }
}
```

Per-scenario extras:
- `gc-pressure` returns `data` as a flat array of executor rows (`[{executor_id, host, tasks, run_ms, gc_ms, gc_ratio, verdict}, ...]`); the early spec's `{by_stage, by_executor}` two-section shape was collapsed long ago.
- `native-io` parses Paimon `SparkListenerNativeIOEvent` records from EventLog. It supports both top-level `native_io_*` fields and legacy embedded `eventJson`. The envelope `summary` reports event/operation counts, reader/export/error counts, total rows/bytes/duration, `top_phases`, and `top_operations`; `data` is the top `--top` native IO events ranked by `duration_ms`, with direct Spark context (`sql_execution_id`, `stage_id`, `task_attempt_id`, executor/host), file/object fields, throughput, memory, raw numeric `metrics`, and a simple `verdict`. See [`docs/examples/native-io-eventlog.md`](docs/examples/native-io-eventlog.md).
- `diagnose` returns `summary: {critical, warn, ok, top_findings_by_impact?, findings_wall_coverage?}`. `top_findings_by_impact` ranks findings by `wall_share` (max across primary + `similar_stages`); `findings_wall_coverage` is the deduped (max-per-stage) sum **capped at 1.0** (parallel stages can naively sum > 1.0). Both omit when `app.DurationMs == 0`. Coverage `< 0.05` ⇒ bottleneck is structural — read `app-summary.top_busy_stages` / `top_io_bound_stages` instead of drilling further. **`severity` is diagnostic confidence, not ROI** — always sort by `top_findings_by_impact` first.
- `diagnose` includes an `executor_supply` rule. For static allocation, it compares `spark.executor.instances` with EventLog-observed `executors_added` / `max_concurrent_executors`. A trigger proves executor under-supply in the EventLog, but does **not** prove the YARN ResourceManager reason; inspect RM/AM diagnostics for pending containers, user limit, queue capacity, node labels, reserved containers, or resource fragmentation.
- `spark-conf` reads `SparkListenerEnvironmentUpdate.Spark Properties` and returns `{key,value,category,importance,tuning_hint}` rows. `summary.parameter_hints` maps common problems (driver/broadcast wait, shuffle spill, data skew, GC pressure) to the Spark parameters worth checking first, so agents can ground recommendations in the application's actual configuration.
- `idle_stage` findings attach available driver/broadcast config in evidence (`spark_driver_memory`, `spark_driver_memory_overhead`, `spark_sql_auto_broadcast_join_threshold`, `spark_sql_broadcast_timeout`), avoiding a manual SHS environment lookup when broadcast/collect waits dominate.
- `paimon-diagnostics` is a live Spark UI command, not an EventLog parser. It returns one row with `overview`, `thread_dump`, and `profiler` objects from the Paimon tab JSON endpoints. Use it when Spark UI jobs/stages are not enough and Paimon read/write appears stuck: `thread_dump.facts.top_stacks` is the thread-dump aggregate flamegraph source; `profiler.facts.artifacts` lists async-profiler CPU/wall-clock outputs and open URLs.
- `data_skew` finding evidence carries `similar_stages: [{stage_id, wall_share, skew_factor}]` when more than one stage qualifies (primary picks the one with the largest `wall_share`, ties resolved by `skew_factor`). `top_findings_by_impact` and `findings_wall_coverage` aggregate across primary + similar_stages — agents can read evidence directly instead of re-running `data-skew`.
- `app-summary` surfaces executor request config (`dynamic_allocation_enabled`, `configured_executor_instances`, `executor_cores`, `executor_memory`, `executor_memory_overhead`) next to observed executor counts, then returns three orthogonal stage views: `top_stages_by_duration` (raw wall, includes driver-side waits), `top_busy_stages` (`busy_ratio > 0.8` only — true CPU hotspots), and `top_io_bound_stages` (`busy_ratio < 0.8` but `spill >= 0.5 GB` or `shuffle_read >= 1 GB` — IO-stalled stages that `top_busy_stages` filters out). The row also includes `avg_active_tasks`, `executor_cpu_ratio`, `scheduler_delay_ratio`, `remote_shuffle_read_ratio`, `speculative_tasks`, and `peak_execution_memory_gb`.
- `slow-stages` rows include per-stage AI signals: `executor_cpu_ratio`, `scheduler_delay_ms`, `scheduler_delay_ratio`, `remote_shuffle_read_ratio`, shuffle block/record counts, `peak_execution_memory_gb`, and `speculative_tasks`.
- `slow-stages` and `data-skew` return `sql_executions: {<id>: <description>}` at the top level — rows reference it via `sql_execution_id`. Descriptions are **truncated to the first 500 runes by default** (use `--sql-detail=full` to restore the original). Callsite-only entries (DataFrame jobs whose description and details are both `org.apache.spark.SparkContext.getCallSite(...)` placeholders) are filtered; if every entry would be noise the entire map is omitted.

Errors → stderr as `{"error":{"code":..., "message":..., "hint":...}}`; non-error progress/warnings use `{"event":{"code":..., "level":..., "message":..., "fields":...}}`. Exit codes: `0` success · `1` internal · `2` user · `3` IO.

## Supported EventLog formats

- V1 single file (`application_<id>` with optional `.inprogress`, `.zstd`, `.lz4`, `.snappy`)
- V2 rolling directory (`eventlog_v2_<id>/events_<n>_<id>`)

Compression: `zstd` and uncompressed are first-class; `lz4` and `snappy` are experimental (Hadoop block framing — open an issue if your logs fail to parse).

## License

MIT
