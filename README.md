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

Each row carries `input_skew_factor` alongside `skew_factor`; verdicts are downgraded to `warn` when input is uniform (`input_skew_factor < 1.2`) and `p99/p50 < 20` so jitter on idle stages stops triggering false `severe`. `slow-stages` rows expose `gc_ratio` (sum(task_gc) / sum(task_run)); `app-summary` exposes `top_stages_by_duration[].busy_ratio` so driver-side idle stages are visible at a glance.

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
  timeout: 60s
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
- Timeout precedence (highest → lowest): `--shs-timeout` flag → `SPARK_CLI_SHS_TIMEOUT` env → `shs.timeout` in YAML → default `60s`.
- Zip bodies up to 256 MiB are decoded in memory; larger or unknown-length
  responses spill to a tempfile that is removed when the process exits.
- **Known caveat:** even a parsed-application cache hit still downloads the
  zip on every invocation, because the locator must inspect zip contents to
  decide V1 vs V2 layout. A persistent on-disk zip cache is on the roadmap.

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

All accept `--top N`, `--format json|table|markdown`, `--dry-run`, `--log-dirs`.

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
  "data": [...]
}
```

Errors → stderr as `{"error":{"code":..., "message":..., "hint":...}}`. Exit codes: `0` success · `1` internal · `2` user · `3` IO.

## Supported EventLog formats

- V1 single file (`application_<id>` with optional `.inprogress`, `.zstd`, `.lz4`, `.snappy`)
- V2 rolling directory (`eventlog_v2_<id>/events_<n>_<id>`)

Compression: `zstd` and uncompressed are first-class; `lz4` and `snappy` are experimental (Hadoop block framing — open an issue if your logs fail to parse).

## License

MIT
