# 变更日志

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
