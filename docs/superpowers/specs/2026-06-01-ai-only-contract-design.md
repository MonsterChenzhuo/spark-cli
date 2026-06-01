# AI-Only Contract Design

## Goal

Make `spark-cli` a CLI built only for AI agents. Successful stdout must be a single JSON object for every command, and failures must remain the existing structured stderr error envelope. Human-readable table, markdown, text, and interactive prompt flows are no longer product goals.

## Context

The repository already treats scenario commands as AI-facing JSON envelopes, but several surfaces still preserve human-first behavior:

- EventLog and live diagnostic commands accept `--format table|markdown`.
- `config show`, `config cluster list`, and `cache list` default to text and require `--format json` for structured output.
- `config init` is interactive and can block an agent waiting for stdin prompts.
- `config cluster add`, `cache clear`, `self-update`, and version output print prose rather than structured JSON.
- README and bundled skills still mention human usage or markdown/table chat presentation.

These paths increase implementation branches and make agent orchestration less reliable.

## Proposed Approach

Adopt a JSON-only CLI contract:

- Every successful command writes exactly one JSON object to stdout.
- Every failing command continues writing exactly one `{"error": ...}` JSON object to stderr.
- EventLog and live diagnostic commands keep the `scenario.Envelope` shape and gain `contract_version`.
- Utility commands return command-specific JSON objects with stable field names.
- `--format` becomes either absent or `json`; any other value returns `FLAG_INVALID`.
- Human-readable renderers and tests are removed once equivalent JSON contract tests exist.

## Command Behavior

### Scenario And Live Diagnostics

Commands:

- `app-summary`
- `spark-conf`
- `slow-stages`
- `data-skew`
- `gc-pressure`
- `native-io`
- `diagnose`
- `yarn-logs`
- `driver-thread-dump`
- `paimon-diagnostics`

Behavior:

- Default output stays JSON.
- `--format json` is accepted for compatibility.
- `--format table` and `--format markdown` fail with `FLAG_INVALID`.
- The envelope adds `contract_version`, starting at `1`.
- Existing envelope fields keep their current JSON tags and meanings.

### Configuration Commands

`spark-cli config show`

- Defaults to the current JSON shape.
- `--format json` is accepted.
- `--format text` fails with `FLAG_INVALID`.

`spark-cli config cluster list`

- Defaults to the current JSON shape: `{active_cluster, clusters}`.
- `--format json` is accepted.
- `--format text` fails with `FLAG_INVALID`.

`spark-cli config cluster add <name>`

- Writes or updates the profile as today.
- On success, stdout is JSON:

```json
{
  "command": "config cluster add",
  "cluster": "prod",
  "path": "/home/user/.config/spark-cli/config.yaml",
  "activated": true,
  "written": true
}
```

`spark-cli config init`

- Becomes non-interactive.
- Creates the config file from flags and defaults.
- Flags provide the old prompt values: `--local-log-dir`, `--hdfs-namenode`, `--hdfs-history-dir`, `--hdfs-user`, `--hadoop-conf-dir`, `--timeout`.
- On success, stdout is JSON:

```json
{
  "command": "config init",
  "path": "/home/user/.config/spark-cli/config.yaml",
  "written": true,
  "config": {
    "log_dirs": ["file:///tmp/spark-events"],
    "timeout": "30s"
  }
}
```

### Cache Commands

`spark-cli cache list`

- Defaults to the current JSON shape.
- `--format json` is accepted.
- `--format text` fails with `FLAG_INVALID`.

`spark-cli cache clear`

- Deletes the same entries as today.
- On success or no-op, stdout is JSON:

```json
{
  "command": "cache clear",
  "cache_dir": "/home/user/.cache/spark-cli",
  "app_id": "application_1_1",
  "dry_run": false,
  "removed": 2,
  "freed_mib": 14.25,
  "entries": [
    {
      "path": "/home/user/.cache/spark-cli/application_1_1.gob.zst",
      "size_mib": 1.23,
      "removed": true
    }
  ]
}
```

Per-entry removal failures should not abort traversal. They should be represented in `entries[].error`; command failure is reserved for setup-level errors that prevent scanning.

### Version And Self Update

`spark-cli version` and `spark-cli --version`

- Both output JSON:

```json
{
  "command": "version",
  "version": "dev"
}
```

`spark-cli self-update`

- Keeps checksum and replacement behavior.
- On dry run, stdout is JSON:

```json
{
  "command": "self-update",
  "dry_run": true,
  "asset": "spark-cli_v0.1.2_darwin_arm64.tar.gz",
  "target": "/usr/local/bin/spark-cli"
}
```

- On installation, stdout is JSON:

```json
{
  "command": "self-update",
  "dry_run": false,
  "version": "v0.1.2",
  "target": "/usr/local/bin/spark-cli"
}
```

## Error Handling

Keep the existing stderr shape and exit-code mapping:

```json
{"error":{"code":"FLAG_INVALID","message":"unknown --format \"table\"","hint":"use json"}}
```

Use `FLAG_INVALID` for removed output formats. This makes the behavior explicit without changing error consumers.

## Testing

Add or update tests before implementation:

- EventLog scenarios reject `--format table` and `--format markdown`.
- EventLog scenarios include `contract_version`.
- `config show`, `config cluster list`, and `cache list` default to JSON.
- The removed `text` formats fail with `FLAG_INVALID`.
- `config init` returns immediately with JSON and does not read stdin.
- `config cluster add`, `cache clear`, `self-update`, `version`, and `--version` return JSON.
- Existing E2E JSON envelope tests still pass.

Remove table/markdown renderer tests after JSON-only tests cover the intended contract.

## Documentation

Update these files in the implementation commit:

- `README.md`
- `README.zh.md`
- `CHANGELOG.md`
- `CHANGELOG.zh.md`
- `AGENTS.md`
- `CLAUDE.md`
- `.agents/skills/spark/SKILL.md`
- `.claude/skills/spark/SKILL.md`

Documentation must describe the project as AI-only and remove instructions that recommend table, markdown, text, or interactive usage.

## Non-Goals

- Do not redesign scenario row schemas beyond adding `contract_version`.
- Do not remove configuration persistence, cache management, self-update, or named cluster profiles.
- Do not add a separate agent protocol server. This change is limited to the CLI contract.
- Do not change stderr error JSON shape or exit-code semantics.
