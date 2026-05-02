# spark-cli

A single-binary CLI for diagnosing Apache Spark application performance from EventLogs. Designed for AI agents (Claude Code) and humans alike — every command emits a structured JSON envelope on stdout.

## Quick start

```bash
spark-cli diagnose application_1735000000_0001
```

If `diagnose` flags `data_skew`:

```bash
spark-cli data-skew application_1735000000_0001 --top 10
```

Each row carries `input_skew_factor` alongside `skew_factor`. `data_skew` ladder downgrades severity through four gates: tight `p99/p50 < 1.5` ⇒ `ok` (uniform task time isn't skew); uniform `input_skew_factor < 1.2` + moderate ratio; stage `wall_share < 1%`; idle stage candidates (busy < 0.2). Extreme `p99/p50 ≥ 20` bypasses every gate. `slow-stages` rows expose `gc_ratio`, `busy_ratio`, and three `*_mb_per_task` (input / shuffle_read / shuffle_write) so partition granularity is visible on read-side, write-side, and source-scan stages alike. `app-summary` ships both `top_stages_by_duration[]` and `top_busy_stages[]` (the latter filtered to `busy_ratio > 0.8` and ranked by `busy_ratio * duration` — the real CPU hotspots). `diagnose.summary` adds `top_findings_by_impact: [{rule_id, severity, wall_share}]` ranked desc plus `findings_wall_coverage` (deduped sum of those wall shares); coverage `< 0.05` means the bottleneck is structural, jump to `top_busy_stages`. SQL description text moved out of every row into a single `envelope.sql_executions: {<id>: <description>}` map (slow-stages / data-skew envelopes only); callsite-only DataFrame placeholders are filtered out and the entire map is omitted when every entry would be noise.

## Install

### One-liner (recommended)

Installs the latest release binary into `/usr/local/bin` and the bundled Claude Code skill into `~/.claude/skills/spark/`. Re-run the same command to upgrade.

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

Supported envs: `VERSION`, `PREFIX`, `SKILL_DIR`, `NO_SUDO`, `NO_SKILL`, `REPO`. See `scripts/install.sh` header for details.

### From source

```bash
go install github.com/opay-bigdata/spark-cli@latest
```

### From release (manual)

Download the archive for your OS from GitHub Releases, then `tar -xzf … && mv spark-cli /usr/local/bin/`.

## Configure

```bash
spark-cli config init       # writes ~/.config/spark-cli/config.yaml
$EDITOR ~/.config/spark-cli/config.yaml
```

```yaml
log_dirs:
  - file:///var/log/spark-history
  - hdfs://mycluster/spark-history     # HA NameService logical name (recommended)
  # - hdfs://nn:8020/spark-history     # or an explicit host:port
  # - shs://history.example.com:18081  # Spark History Server REST API
hdfs:
  user: hadoop
  conf_dir: /etc/hadoop/conf           # optional; auto-discovered via HADOOP_CONF_DIR / HADOOP_HOME if empty
shs:
  timeout: 5m  # default; production EventLog zips often need minutes — first-fetch progress is printed to stderr (silence with SPARK_CLI_QUIET=1)
timeout: 30s
```

Override per-invocation via `--log-dirs`, env var `SPARK_CLI_LOG_DIRS`.

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
- Kerberos / SASL / TLS are **not supported**; this targets simple-auth clusters only.

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
- HTTP only; **no** TLS, Basic Auth, Bearer token, or Kerberos in v1.
- Timeout precedence (highest → lowest): `--shs-timeout` flag → `SPARK_CLI_SHS_TIMEOUT` env → `shs.timeout` in YAML → default `5m`. Timeout errors return `LOG_UNREADABLE` with a `hint` naming the flag — no need to grep docs after the first failure.
- Progress lines (`spark-cli: downloading EventLog zip from SHS ...` / `ready in <duration>`) follow `--no-progress` flag → `SPARK_CLI_QUIET` env (`1/true` 静默, `0/false` 强制显示) → stdout TTY 检测;**agent 重定向 stdout 时默认静默,交互终端默认显示**。
- **Persistent on-disk zip cache** (since v0.x): the downloaded zip is written to `<cache_dir>/shs/<host>/<appID>_<lastUpdated>.zip` (atomic tmp+rename) and reused across CLI invocations. Subsequent commands on the same appID only fetch the cheap metadata JSON to compare `lastUpdated`; on a hit the zip is read from disk. Stale attempts (older `lastUpdated` for the same appID) are swept on each successful download. `--no-cache` falls back to a one-shot system tempfile.
- Zip bodies up to 256 MiB without disk caching are decoded in memory; with disk caching enabled the response always lands on disk via tmp+rename.

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
| `spark-cli slow-stages <appId>` | Stages by wall time |
| `spark-cli data-skew <appId>` | Skewed stages |
| `spark-cli gc-pressure <appId>` | GC ratio per stage / executor |
| `spark-cli config show [--format json]` | Print effective configuration (yaml / env / default sources) |
| `spark-cli cache list` / `cache clear [--app <id>] [--dry-run]` | Inspect / prune the parsed-application + SHS zip caches |
| `spark-cli version` (also `--version`) | Print spark-cli version |

All accept `--top N`, `--format json|table|markdown`, `--dry-run`, `--log-dirs`,
`--cache-dir`, `--no-cache`, `--shs-timeout`, `--no-progress`,
`--sql-detail truncate|full|none` (default `truncate` — first 500 runes of the
SQL description with a `...(truncated, total <N> chars)` marker; `full` keeps
the original; `none` omits the entire `sql_executions` map).

## For AI agents

The repo ships `.claude/skills/spark/SKILL.md`. Claude Code auto-loads it when present. The skill teaches the diagnose-first workflow.

## Output contract

```json
{
  "scenario": "data-skew",
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
- `diagnose` returns `summary: {critical, warn, ok, top_findings_by_impact?, findings_wall_coverage?}`. `top_findings_by_impact` ranks findings by `wall_share` (max across primary + `similar_stages`); `findings_wall_coverage` is the deduped (max-per-stage) sum **capped at 1.0** (parallel stages can naively sum > 1.0). Both omit when `app.DurationMs == 0`. Coverage `< 0.05` ⇒ bottleneck is structural — read `app-summary.top_busy_stages` / `top_io_bound_stages` instead of drilling further. **`severity` is diagnostic confidence, not ROI** — always sort by `top_findings_by_impact` first.
- `data_skew` finding evidence carries `similar_stages: [{stage_id, wall_share, skew_factor}]` when more than one stage qualifies (primary picks the one with the largest `wall_share`, ties resolved by `skew_factor`). `top_findings_by_impact` and `findings_wall_coverage` aggregate across primary + similar_stages — agents can read evidence directly instead of re-running `data-skew`.
- `app-summary` returns three orthogonal stage views: `top_stages_by_duration` (raw wall, includes driver-side waits), `top_busy_stages` (`busy_ratio > 0.8` only — true CPU hotspots), and `top_io_bound_stages` (`busy_ratio < 0.8` but `spill >= 0.5 GB` or `shuffle_read >= 1 GB` — IO-stalled stages that `top_busy_stages` filters out). **All three together** prevent missing the real bottleneck.
- `slow-stages` and `data-skew` return `sql_executions: {<id>: <description>}` at the top level — rows reference it via `sql_execution_id`. Descriptions are **truncated to the first 500 runes by default** (use `--sql-detail=full` to restore the original). Callsite-only entries (DataFrame jobs whose description and details are both `org.apache.spark.SparkContext.getCallSite(...)` placeholders) are filtered; if every entry would be noise the entire map is omitted.

Errors → stderr as `{"error":{"code":..., "message":..., "hint":...}}`. Exit codes: `0` success · `1` internal · `2` user · `3` IO.

## Supported EventLog formats

- V1 single file (`application_<id>` with optional `.inprogress`, `.zstd`, `.lz4`, `.snappy`)
- V2 rolling directory (`eventlog_v2_<id>/events_<n>_<id>`)

Compression: `zstd` and uncompressed are first-class; `lz4` and `snappy` are experimental (Hadoop block framing — open an issue if your logs fail to parse).

## License

MIT
