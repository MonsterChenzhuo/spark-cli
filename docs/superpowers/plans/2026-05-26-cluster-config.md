# Cluster Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist Spark History Server and YARN URLs as named cluster profiles so commands can select a cluster instead of wiring unrelated URLs by hand.

**Architecture:** Extend `internal/config.Config` with `active_cluster` and `clusters`, where each cluster owns `log_dirs`, `yarn.base_urls`, and optional `shs.timeout`. Keep legacy top-level config and flags compatible. Add a root `--cluster` flag plus `spark-cli config cluster add|list` commands that update the local YAML.

**Tech Stack:** Go 1.22, Cobra, `gopkg.in/yaml.v3`, existing `internal/config` and `cmd/configcmd` packages.

---

### Task 1: Configuration Model

**Files:**
- Modify: `internal/config/config.go`
- Test: `internal/config/config_test.go`

- [x] **Step 1: Write failing tests**

Add tests that load this YAML and assert `active_cluster` selects the named cluster:

```yaml
active_cluster: prod
clusters:
  prod:
    log_dirs:
      - shs://shs-prod:18081
    yarn:
      base_urls:
        - http://gw/prod/yarn
    shs:
      timeout: 7m
```

Expected effective values: `LogDirs=["shs://shs-prod:18081"]`, `YARN.BaseURLs=["http://gw/prod/yarn"]`, `SHS.Timeout=7m`.

- [x] **Step 2: Run tests to verify failure**

Run: `go test ./internal/config -run 'TestLoadParsesClusterProfiles|TestApplyClusterSelectsNamedCluster' -count=1`

Expected: FAIL because `Config` does not yet have clusters or selection logic.

- [x] **Step 3: Implement minimal model**

Add `ClusterConfig`, `Clusters map[string]ClusterConfig`, `ActiveCluster string`, `SelectedCluster string`, and `ApplyCluster(cfg, name)` with top-level fallback compatibility.

- [x] **Step 4: Run tests to verify pass**

Run: `go test ./internal/config -count=1`

Expected: PASS.

### Task 2: Runtime Cluster Selection

**Files:**
- Modify: `cmd/scenarios/state.go`
- Modify: `cmd/scenarios/register.go`
- Modify: `cmd/scenarios/runner.go`
- Test: `cmd/scenarios/runner_test.go`

- [x] **Step 1: Write failing test**

Add a runner test that writes a temp config with `active_cluster: prod`, one cluster containing `log_dirs`, then runs `Run(... Options{Scenario:"app-summary"})` without explicit `LogDirs`; dry-run should locate the fixture via the selected cluster.

- [x] **Step 2: Run test to verify failure**

Run: `go test ./cmd/scenarios -run TestRunnerUsesActiveClusterLogDirs -count=1`

Expected: FAIL because runner ignores cluster profiles.

- [x] **Step 3: Implement root flag and config merge**

Add `Cluster string` to scenario options/state, bind `--cluster`, and call `config.ApplyCluster` after env but before explicit flag overrides.

- [x] **Step 4: Run tests to verify pass**

Run: `go test ./cmd/scenarios -count=1`

Expected: PASS.

### Task 3: Config Cluster Commands

**Files:**
- Create: `cmd/configcmd/cluster.go`
- Modify: `cmd/configcmd/configcmd.go`
- Modify: `cmd/configcmd/show.go`
- Test: `cmd/configcmd/cluster_test.go`
- Test: `cmd/configcmd/show_test.go`

- [x] **Step 1: Write failing tests**

Add tests for `config cluster add prod --log-dirs shs://h:18081 --yarn-base-urls http://gw/yarn --shs-timeout 5m --activate` and `config cluster list --format json`.

- [x] **Step 2: Run tests to verify failure**

Run: `go test ./cmd/configcmd -run 'TestClusterAddWritesConfig|TestClusterListJSON|TestRenderJSONIncludesClusters' -count=1`

Expected: FAIL because the command and JSON fields do not exist.

- [x] **Step 3: Implement commands**

Add YAML read/write helpers scoped to `cmd/configcmd`, preserving known config fields and writing `config.yaml` under `SPARK_CLI_CONFIG_DIR` or `~/.config/spark-cli`.

- [x] **Step 4: Run tests to verify pass**

Run: `go test ./cmd/configcmd -count=1`

Expected: PASS.

### Task 4: Documentation and Verification

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md`
- Modify: `README.zh.md`
- Modify: `CHANGELOG.md`
- Modify: `CHANGELOG.zh.md`

- [x] **Step 1: Update docs**

Document `active_cluster`, `clusters`, `--cluster`, and `config cluster add/list`. Add the user-requested repo rule that work may be done directly on `main` and pushed to `main` when explicitly requested.

- [x] **Step 2: Run verification**

Run:

```bash
make tidy
make lint
make unit-test
```

Expected: all commands exit 0.

- [x] **Step 3: Commit and push**

Run:

```bash
git add .
git commit -m "feat(config): add named cluster profiles"
git push origin main
```

Expected: push succeeds on `main`.
