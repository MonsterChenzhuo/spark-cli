# Native IO EventLog diagnostics

Use `native-io` when a Paimon job enables native IO EventLog metrics and you need to compare reader/export/DV phases without opening Spark UI pages.

## 1. Run the EventLog command

```bash
spark-cli native-io application_1779838558973_3619 --top 20
```

The command reads Spark EventLog, not live Spark UI state. It supports both Paimon's newer top-level `native_io_*` fields and older `eventJson`-only records.

## 2. Read the summary first

```json
{
  "scenario": "native-io",
  "summary": {
    "events_total": 1280,
    "operations_total": 96,
    "reader_events": 1216,
    "export_events": 64,
    "error_events": 0,
    "total_duration_ms": 842130,
    "total_rows": 100000000,
    "total_bytes": 193273528320,
    "throughput_mb_per_s": 218.875,
    "top_phases": [
      {"phase": "READ_BATCH", "events": 900, "duration_ms": 620000},
      {"phase": "DV_FILTER", "events": 260, "duration_ms": 180000}
    ],
    "top_operations": [
      {"operation_id": "scan-7", "operation_name": "native-columnar-read", "duration_ms": 45600}
    ]
  }
}
```

For native IO ROI questions, compare `top_phases[].duration_ms` first. If `DV_FILTER` dominates, the deletion-vector path is the bottleneck. If `READ_BATCH` dominates with low `throughput_mb_per_s`, inspect object store latency, batch sizing, and file layout.

## 3. Drill into slow native IO events

`data[]` is ranked by `duration_ms` and keeps raw numeric `metrics` so an agent can reason without reparsing `eventJson`.

```json
{
  "event_id": "scan-7-3000",
  "event_type": "OPERATION_END",
  "ai_kind": "paimon_native_io_reader",
  "operation_id": "scan-7",
  "operation_name": "native-columnar-read",
  "phase": "READ_BATCH",
  "sql_execution_id": 17,
  "stage_id": 3,
  "task_attempt_id": 99,
  "executor_id": "5",
  "host": "worker-5",
  "file_path": "obs://bucket/table/file.parquet",
  "duration_ms": 500,
  "rows": 4096,
  "bytes": 16777216,
  "throughput_mb_per_s": 32,
  "metrics": {"read_batch_ms": 500, "rows": 4096, "bytes": 16777216},
  "verdict": "ok"
}
```

Use `stage_id` and `task_attempt_id` to connect slow native IO events back to `slow-stages`. Use `file_path` to check whether a small number of files or partitions dominate.

## 4. Agent workflow

1. Start with `spark-cli diagnose <appId>` for Spark-level bottlenecks.
2. If the question is native IO-specific, run `spark-cli native-io <appId> --top 20`.
3. Read `summary.top_phases` before individual rows.
4. For the slowest row, quote `operation_name`, `phase`, `duration_ms`, throughput, `stage_id`, and the most relevant `metrics` keys.
5. If EventLog is incomplete or has no native IO events, use `spark-cli paimon-diagnostics <appId>` or `spark-cli driver-thread-dump <appId> --thread-summary-only` for live state.

## Limits

- The command only sees events that Paimon wrote into Spark EventLog.
- It cannot prove native code executed unless the Paimon listener emitted native IO records.
- Cached applications parsed by older spark-cli versions are invalidated by the v2 cache schema and reparsed automatically.
