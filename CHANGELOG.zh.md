# 变更日志

## Unreleased

### CI / Release
- `ci.yml` 改为单 job 全流程：用 `go.mod` 锁定 Go 版本、`go mod tidy` 清洁度校验、gofmt、`go run` 调起 golangci-lint v2、`-race` 单测、带版本 ldflag 的 build、烟囱测试、`-tags=e2e` 的 e2e dry-run，以及 SKILL.md 前置元数据校验。
- `release.yml` 同时响应 `push` 到 `main`（自动 bump patch tag）和 `v*` tag 推送；由 `release` concurrency group 串行化。
- `.goreleaser.yml` 归档现在同时打包 `README.zh.md` 与 `CHANGELOG.zh.md`，与英文版及内置 skill 并列。
- `.golangci.yml` 新增 `formatters`（gofmt + goimports）、errcheck 排除函数集、并把 `dist/` 排除在扫描之外。

### 安装脚本
- `scripts/install.sh` 重写为 hbase-metrics-cli 同款：SHA-256 校验、redirect+API 双路解析最新 tag、sudo 兜底、skill 目录树镜像，环境变量改为 `VERSION` / `PREFIX` / `SKILL_DIR` / `NO_SUDO` / `NO_SKILL` / `REPO`。
- **不兼容变更：** 旧的 `SPARK_CLI_BIN_DIR` / `SPARK_CLI_VERSION` / `SPARK_CLI_SKILL_DIR` 分别更名为 `PREFIX` / `VERSION` / `SKILL_DIR`；默认安装目录由 `~/.local/bin` 改为 `/usr/local/bin`，通过 `PREFIX=...` 覆盖。
- 默认仓库 slug 修正为 `MonsterChenzhuo/spark-cli`；README 同步更新。

## v0.1.0 — 2026-04-29

MVP 首版。

### 场景命令
- `app-summary` —— 应用级总览 (executor、stage、task、GC 占比、Top 耗时 stage)
- `slow-stages` —— 按 wall-clock 耗时排序的 stage,含任务分位数
- `data-skew` —— 倾斜 stage 列表,输出 `skew_factor` / `input_skew_factor` 与 `severe`/`warn`/`mild` 评级
- `gc-pressure` —— 按 executor 维度的 GC 占比分级
- `diagnose` —— 一次性跑 5 条规则 (data_skew、gc_pressure、disk_spill、failed_tasks、tiny_tasks) 并汇总输出

### 输出格式
- JSON (默认; AI agent 的契约格式)
- Table (对齐文本)
- Markdown (管道表格,适合聊天嵌入)

### EventLog 支持
- V1 单文件日志 (识别 `.inprogress` / `.zstd` / `.lz4` / `.snappy` 后缀)
- V2 滚动目录 (`eventlog_v2_<id>/events_<n>_*`),多分片拼接解码
- 文件系统抽象统一 `file://` 与 `hdfs://` URI

### Agent 集成
- 内置 `.claude/skills/spark/SKILL.md`,教 Claude Code「先 diagnose 后下钻」流程
- 结构化 stderr 错误,错误码包括 `APP_NOT_FOUND`、`APP_AMBIGUOUS`、`LOG_UNREADABLE`、`LOG_PARSE_FAILED`、`LOG_INCOMPLETE`、`FLAG_INVALID`、`INTERNAL`

### 分发
- `scripts/install.sh` 一行安装脚本
- goreleaser 多平台发布 (linux/darwin × amd64/arm64)
