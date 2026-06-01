# AI-Only Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every successful `spark-cli` command emit exactly one JSON object and remove human-readable output paths.

**Architecture:** Keep scenario diagnostics on `scenario.Envelope`, adding `contract_version`. Utility commands use small command-specific response structs and shared JSON encoders. Removed table, markdown, text, and interactive paths become either deleted code or `FLAG_INVALID` errors.

**Tech Stack:** Go 1.22, cobra, existing `internal/errors` JSON stderr contract, existing E2E tests through `cmd.RunWith`.

---

## File Structure

- Modify `internal/scenario/result.go`: add `ContractVersion` to `Envelope`.
- Modify `cmd/scenarios/runner.go`: initialize envelope contract version and reject non-JSON formats.
- Delete `internal/output/table.go` and `internal/output/markdown.go`; remove related tests.
- Modify `cmd/configcmd/show.go`, `cmd/configcmd/cluster.go`, `cmd/configcmd/init.go`: JSON-only config commands.
- Modify `cmd/cachecmd/list.go`, `cmd/cachecmd/clear.go`: JSON-only cache commands.
- Modify `cmd/version.go`, `cmd/root.go`, `cmd/self_update.go`: JSON version and update results.
- Modify `tests/e2e/e2e_test.go` and command unit tests: assert JSON-only behavior.
- Modify `README.md`, `README.zh.md`, `CHANGELOG.md`, `CHANGELOG.zh.md`, `AGENTS.md`, `CLAUDE.md`, `.agents/skills/spark/SKILL.md`, `.claude/skills/spark/SKILL.md`: AI-only docs.

## Task 1: Scenario JSON Contract

**Files:**
- Modify: `internal/scenario/result.go`
- Modify: `cmd/scenarios/runner.go`
- Modify: `tests/e2e/e2e_test.go`
- Delete or rewrite: `internal/output/table_test.go`, `internal/output/markdown_test.go`

- [ ] **Step 1: Write failing tests**

Add E2E assertions that JSON envelopes include `contract_version` and that `--format table|markdown` returns `FLAG_INVALID`.

- [ ] **Step 2: Run targeted tests and confirm failure**

Run: `go test -count=1 ./tests/e2e`

Expected: failures for missing `contract_version` and accepted table/markdown formats.

- [ ] **Step 3: Implement minimal JSON-only scenario rendering**

Add `ContractVersion int json:"contract_version"` to `scenario.Envelope`, set it to `1` for every scenario/live envelope, and change `render` to accept only `""` or `"json"`.

- [ ] **Step 4: Remove unused human renderers**

Delete table/markdown implementation and tests once no production code references them.

- [ ] **Step 5: Run targeted tests**

Run: `go test -count=1 ./tests/e2e ./internal/output`

Expected: pass.

## Task 2: Utility Commands Emit JSON By Default

**Files:**
- Modify: `cmd/configcmd/show.go`
- Modify: `cmd/configcmd/cluster.go`
- Modify: `cmd/cachecmd/list.go`
- Modify: `cmd/configcmd/show_test.go`
- Modify: `cmd/configcmd/cluster_test.go`
- Modify: `cmd/cachecmd/cachecmd_test.go` or add focused tests

- [ ] **Step 1: Write failing tests**

Assert `config show`, `config cluster list`, and `cache list` default to JSON and reject `--format text`.

- [ ] **Step 2: Run targeted tests and confirm failure**

Run: `go test -count=1 ./cmd/configcmd ./cmd/cachecmd`

Expected: current text defaults make JSON parsing fail.

- [ ] **Step 3: Implement JSON-only defaults**

Change default format handling to JSON, keep `--format json`, and return a `FLAG_INVALID`-style cobra error message for `text`.

- [ ] **Step 4: Run targeted tests**

Run: `go test -count=1 ./cmd/configcmd ./cmd/cachecmd`

Expected: pass.

## Task 3: Non-Interactive Config Init

**Files:**
- Modify: `cmd/configcmd/init.go`
- Modify: `cmd/configcmd/init_test.go`

- [ ] **Step 1: Write failing tests**

Assert `config init` completes with empty stdin, writes config from flags/defaults, and stdout is JSON containing `command`, `path`, `written`, and `config`.

- [ ] **Step 2: Run targeted tests and confirm failure**

Run: `go test -count=1 ./cmd/configcmd`

Expected: current interactive prompts break the JSON assertion.

- [ ] **Step 3: Implement flag-driven init**

Add flags matching the old prompt values and write config without reading stdin.

- [ ] **Step 4: Run targeted tests**

Run: `go test -count=1 ./cmd/configcmd`

Expected: pass.

## Task 4: JSON Results For Mutating Utility Commands

**Files:**
- Modify: `cmd/configcmd/cluster.go`
- Modify: `cmd/cachecmd/clear.go`
- Modify: `cmd/self_update.go`
- Modify: `cmd/version.go`
- Modify: `cmd/root.go`
- Modify: relevant tests under `cmd/`, `cmd/configcmd/`, and `cmd/cachecmd/`

- [ ] **Step 1: Write failing tests**

Assert `config cluster add`, `cache clear`, `self-update --dry-run`, `version`, and `--version` output JSON.

- [ ] **Step 2: Run targeted tests and confirm failure**

Run: `go test -count=1 ./cmd ./cmd/configcmd ./cmd/cachecmd`

Expected: prose output breaks JSON assertions.

- [ ] **Step 3: Implement response structs and JSON encoders**

Return stable JSON objects for each command. Preserve existing side effects.

- [ ] **Step 4: Run targeted tests**

Run: `go test -count=1 ./cmd ./cmd/configcmd ./cmd/cachecmd`

Expected: pass.

## Task 5: Documentation And Skills

**Files:**
- Modify: `README.md`
- Modify: `README.zh.md`
- Modify: `CHANGELOG.md`
- Modify: `CHANGELOG.zh.md`
- Modify: `AGENTS.md`
- Modify: `CLAUDE.md`
- Modify: `.agents/skills/spark/SKILL.md`
- Modify: `.claude/skills/spark/SKILL.md`

- [ ] **Step 1: Update AI-only wording**

Remove human-facing positioning and references to `table`, `markdown`, `text`, and interactive setup.

- [ ] **Step 2: Document JSON utility outputs**

Add concise examples for `config init`, `config cluster add`, `cache clear`, `version`, and `self-update --dry-run`.

- [ ] **Step 3: Update changelogs**

Record the breaking JSON-only contract.

- [ ] **Step 4: Search for stale wording**

Run: `rg -n "human|humans|运维人员|table|markdown|text|interactive|prompt|--format json\\|table|--format json\\|markdown" README.md README.zh.md CHANGELOG.md CHANGELOG.zh.md AGENTS.md CLAUDE.md .agents/skills/spark/SKILL.md .claude/skills/spark/SKILL.md`

Expected: no stale product guidance remains; occurrences only explain removed formats or error behavior.

## Task 6: Final Verification

**Files:**
- All modified files

- [ ] **Step 1: Run tidy**

Run: `make tidy`

Expected: no unexpected `go.mod` or `go.sum` diff.

- [ ] **Step 2: Run lint**

Run: `make lint`

Expected: pass.

- [ ] **Step 3: Run unit tests**

Run: `make unit-test`

Expected: pass.

- [ ] **Step 4: Run E2E tests**

Run: `make e2e`

Expected: pass.
