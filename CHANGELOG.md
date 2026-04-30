# Changelog

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
