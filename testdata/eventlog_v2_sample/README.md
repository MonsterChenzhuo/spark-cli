# EventLog V2 Sample Fixture

`eventlog_v2_application_1234567890_0001/` is a sanitized snapshot of a real
Spark 3.4.2 + Kyuubi rolling-format EventLog, kept here for parser and
end-to-end tests.

## What's inside

- `appstatus_application_1234567890_0001` — empty marker file (V2 convention).
- `events_1_application_1234567890_0001` — JSONL stream of 296 events:

| Event                          | Count |
|--------------------------------|------:|
| SparkListenerLogStart          |     1 |
| SparkListenerApplicationStart  |     1 |
| SparkListenerApplicationEnd    |     1 |
| SparkListenerExecutorAdded     |     7 |
| SparkListenerBlockManagerAdded |     8 |
| SparkListenerJobStart          |     7 |
| SparkListenerJobEnd            |     7 |
| SparkListenerStageSubmitted    |     7 |
| SparkListenerStageCompleted    |     7 |
| SparkListenerTaskStart         |   125 |
| SparkListenerTaskEnd           |   125 |

Only the listener events the parser consumes are retained. SQL / Kyuubi /
Environment / ResourceProfile events were dropped; parser robustness against
unknown event types is covered by inline unit tests, not this fixture.

## Sanitization

Sensitive fields were rewritten before commit:

- Hosts → `driver.example.com`, `executor-{1..5}.example.com`, `10.0.0.1`.
- Application name → `sample_app`; user → `spark`.
- Application ID → `application_1234567890_0001` (numeric component
  `1234567890_0001`).
- Free-text fields that may carry SQL, business-table names, or stack traces
  (`Description`, `Details`, `Stage Name`, `Properties`, `Accumulables[].Name`,
  …) were replaced with the literal string `<sanitized>`.

The Task Metrics objects (`Executor Run Time`, `JVM GC Time`,
`Memory/Disk Bytes Spilled`, shuffle read/write counters, …) are preserved
verbatim — those drive the diagnose rules and golden tests.
