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

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/opay-bigdata/spark-cli/main/scripts/install.sh | bash
```

Or:

```bash
go install github.com/opay-bigdata/spark-cli@latest
```

## Configure

```bash
spark-cli config init       # writes ~/.config/spark-cli/config.yaml
$EDITOR ~/.config/spark-cli/config.yaml
```

```yaml
log_dirs:
  - file:///var/log/spark-history
  - hdfs://nn:8020/spark-history
hdfs:
  user: hadoop
timeout: 30s
```

Override per-invocation via `--log-dirs`, env var `SPARK_CLI_LOG_DIRS`.

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
