# Walkthrough: diagnosing a slow Spark job

Below is a transcript an AI agent would produce after the user reports "etl_daily ran 3× longer than usual yesterday."

## 1. Run diagnose

```bash
$ spark-cli diagnose application_1735000000_0001
```
```json
{
  "scenario": "diagnose",
  "app_id": "application_1735000000_0001",
  "app_name": "etl_daily",
  "log_path": "hdfs://nn:8020/spark-history/application_1735000000_0001.zstd",
  "log_format": "v1",
  "compression": "zstd",
  "incomplete": false,
  "parsed_events": 482113,
  "elapsed_ms": 1842,
  "columns": ["rule_id", "severity", "title", "evidence", "suggestion"],
  "data": [
    {"rule_id": "data_skew", "severity": "critical", "title": "Data skew detected",
     "evidence": {"stage_id": 7, "skew_factor": 23.95, "p50_task_ms": 410, "p99_task_ms": 9820},
     "suggestion": "stage 7 任务长尾严重..."},
    {"rule_id": "gc_pressure", "severity": "warn", "title": "GC pressure",
     "evidence": {"executor_id": "12", "gc_ratio": 0.14, "total_gc_ms": 92000, "total_run_ms": 657000},
     "suggestion": "executor 12 GC 占比 14.0%..."},
    {"rule_id": "disk_spill", "severity": "ok", "title": "Disk spill", "evidence": null, "suggestion": null},
    {"rule_id": "failed_tasks", "severity": "ok", "title": "Failed tasks", "evidence": null, "suggestion": null},
    {"rule_id": "tiny_tasks", "severity": "ok", "title": "Tiny tasks", "evidence": null, "suggestion": null}
  ],
  "summary": {"critical": 1, "warn": 1, "ok": 3}
}
```

Agent reads: `summary.critical=1` from `data_skew` on stage 7. Drill down.

## 2. Drill into data-skew

```bash
$ spark-cli data-skew application_1735000000_0001 --top 5
```
Agent gets the full skewed-stage list with `verdict: "severe"` for stage 7 and surfaces "stage 7 has a 24× long-tail; check join keys or enable AQE skew join."

## 3. Confirm GC isn't masking the issue

```bash
$ spark-cli gc-pressure application_1735000000_0001
```
Agent reads `by_executor` and notes only one executor (12) is hot — confirms it's localized, not cluster-wide.

## 4. Suggest next steps

The agent now has enough to write a remediation plan referencing both the skew (root cause) and GC (secondary symptom on the affected executor). The user's question is answered without ever opening the Spark UI.
