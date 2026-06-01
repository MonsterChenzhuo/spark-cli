# Spark Diagnostic SOP

This workflow is for agents and humans who need to diagnose a Spark application
without accidentally mixing Spark History Server and YARN endpoints from
different clusters.

## Problem

Production environments often have multiple Spark clusters. A correct diagnosis
needs the EventLog source and the YARN gateway from the same physical cluster.
If they are passed independently, an agent can parse one cluster's EventLog and
then inspect another cluster's YARN state.

## SOP

1. Inspect configured clusters:

```bash
spark-cli config cluster list --format json
```

2. Inspect the effective config and source labels:

```bash
spark-cli config show --format json
```

3. If no cluster exists, record one:

```bash
spark-cli config cluster add prod \
  --log-dirs shs://history.example.com:18081 \
  --yarn-base-urls http://203.123.81.20:7765/gateway/hadoop-prod/yarn \
  --shs-timeout 5m \
  --activate
```

4. If multiple clusters exist, choose one explicitly:

```bash
spark-cli --cluster prod diagnose application_1772605260987_35693 --guided
```

5. If one cluster exists, guided diagnose selects it and then runs the normal
diagnose pipeline:

```bash
spark-cli diagnose application_1772605260987_35693 --guided
```

## Output Contract

`--guided` writes preflight notes to stderr and preserves stdout as the normal
single `diagnose` JSON envelope:

```json
{
  "scenario": "diagnose",
  "app_id": "application_1772605260987_35693",
  "log_path": "shs://history.example.com:18081/application_1772605260987_35693/...",
  "columns": ["rule_id", "severity", "title", "evidence", "suggestion"],
  "data": [],
  "summary": {"critical": 0, "warn": 1, "ok": 6}
}
```

Agent consumers should parse stdout only. stderr is human-readable progress and
SOP guidance.

## Reading The Result

Start with `summary.top_findings_by_impact` and
`summary.findings_wall_coverage`. If coverage is below `0.05`, the rule set is
not explaining the wall time; run `spark-cli app-summary <appId>` and inspect
`top_busy_stages`, `top_io_bound_stages`, `jobs_total`, and `stages_total`.

When a finding points to a specific rule, drill down:

```bash
spark-cli data-skew <appId> --top 10
spark-cli slow-stages <appId> --top 10
spark-cli gc-pressure <appId>
spark-cli native-io <appId> --top 20
```

If `yarn.base_urls` is configured and the app is still running, add live probes:

```bash
spark-cli driver-thread-dump <appId> --executor-id driver --thread-summary-only
spark-cli yarn-logs <appId> --top 5 --yarn-log-bytes 65536
```

## Failure Paths

- No cluster and no `log_dirs`: guided diagnose returns `CONFIG_MISSING` with a
  `config cluster add ... --activate` hint.
- Multiple clusters and no selected cluster: guided diagnose returns
  `FLAG_INVALID` and asks for `--cluster <name>` or an active cluster.
- Missing `yarn.base_urls`: EventLog diagnosis still runs, but live YARN and
  thread-dump commands need `--yarn-base-urls` or a cluster profile update.

## Limits

`--guided` does not prompt interactively and does not mutate config. It enforces
the SOP boundary before diagnosis while keeping machine-readable stdout stable.
