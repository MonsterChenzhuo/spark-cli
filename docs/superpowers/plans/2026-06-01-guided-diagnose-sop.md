# Guided Diagnose SOP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a guided Spark diagnosis SOP that confirms cluster selection before reading EventLogs.

**Architecture:** Keep normal scenario output unchanged. Add a `--guided` flag for `diagnose` that performs configuration preflight before the existing runner pipeline: require a selected cluster when multiple profiles exist, auto-select the only configured cluster, and emit cluster/YARN status on stderr while stdout remains the existing diagnose envelope.

**Tech Stack:** Go 1.22, Cobra, existing `internal/config` cluster profiles, existing scenario runner tests.

---

### Task 1: Guided Preflight Tests

**Files:**
- Modify: `cmd/scenarios/runner_test.go`
- Modify: `cmd/scenarios/state.go`
- Modify: `cmd/scenarios/register.go`
- Modify: `cmd/scenarios/runner.go`

- [ ] **Step 1: Write failing tests**

Add tests that prove:
- `diagnose --guided` fails when multiple clusters exist and none is selected.
- `diagnose --guided` auto-selects the only configured cluster.
- `diagnose --guided` gives a cluster-add hint when there are no clusters and no `log_dirs`.

- [ ] **Step 2: Verify RED**

Run:

```bash
go test ./cmd/scenarios -run Guided -count=1
```

Expected: compile failure or test failure because `Options.Guided` does not exist.

- [ ] **Step 3: Implement minimal guided preflight**

Add `Guided bool` to scenario options/state, register `--guided`, and call a helper before the existing `diagnose` flow.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
go test ./cmd/scenarios -run Guided -count=1
```

Expected: PASS.

### Task 2: CLI Wiring Test

**Files:**
- Modify: `cmd/root_test.go`

- [ ] **Step 1: Write failing test**

Use `cmd.RunWith` to confirm `diagnose <appId> --guided --dry-run` reaches the scenario runner and resolves the only configured cluster.

- [ ] **Step 2: Verify RED/GREEN**

Run:

```bash
go test ./cmd -run Guided -count=1
```

Expected: fail before wiring, pass after `--guided` is registered and mapped into `Options`.

### Task 3: Documentation

**Files:**
- Modify: `README.md`
- Modify: `README.zh.md`
- Modify: `AGENTS.md`
- Modify: `.claude/skills/spark/SKILL.md`
- Modify: `CHANGELOG.md`
- Modify: `CHANGELOG.zh.md`

- [ ] **Step 1: Document the SOP**

Add the canonical order:
`config cluster list` / `config show` → add/select cluster if missing → `diagnose --guided` → live/thread dump or drill-down.

- [ ] **Step 2: Verify docs mention the new flag**

Run:

```bash
rg -n -- '--guided|guided' README.md README.zh.md AGENTS.md .claude/skills/spark/SKILL.md CHANGELOG.md CHANGELOG.zh.md
```

Expected: all required docs include the guided SOP or release note.

### Task 4: Final Verification

- [ ] **Step 1: Run focused tests**

```bash
go test ./cmd ./cmd/scenarios ./cmd/configcmd ./internal/config -count=1
```

- [ ] **Step 2: Run project gates**

```bash
make tidy
make lint
make unit-test
```

- [ ] **Step 3: Commit and push main**

Commit with:

```bash
git commit -m "feat(diagnose): add guided cluster preflight"
```

Include:

```text
Co-Authored-By: Codex Opus 4.7 <noreply@anthropic.com>
```

Push:

```bash
git push origin main
```
