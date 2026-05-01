# spark-cli

用于解析 Apache Spark EventLog、定位性能问题的单二进制 CLI。面向 AI agent (Claude Code) 与运维人员双场景设计 —— 每条命令在 stdout 输出统一的 JSON 信封。

## 快速开始

```bash
spark-cli diagnose application_1735000000_0001
```

`diagnose` 报告 `data_skew` 严重时,继续下钻:

```bash
spark-cli data-skew application_1735000000_0001 --top 10
```

## 安装

### 一键脚本（推荐）

将最新 release 二进制安装到 `/usr/local/bin`，并把内置 Claude Code skill 安装到 `~/.claude/skills/spark/`。重复执行该命令即可升级。

```bash
curl -fsSL https://raw.githubusercontent.com/MonsterChenzhuo/spark-cli/main/scripts/install.sh | bash
```

常用覆盖：

```bash
# 锁定版本
curl -fsSL https://raw.githubusercontent.com/MonsterChenzhuo/spark-cli/main/scripts/install.sh | VERSION=v0.1.0 bash

# 装到无需 sudo 的路径，跳过 skill
curl -fsSL https://raw.githubusercontent.com/MonsterChenzhuo/spark-cli/main/scripts/install.sh | PREFIX="$HOME/.local/bin" NO_SKILL=1 bash
```

支持的环境变量：`VERSION`、`PREFIX`、`SKILL_DIR`、`NO_SUDO`、`NO_SKILL`、`REPO`，详见 `scripts/install.sh` 头部注释。

### 源码安装

```bash
go install github.com/opay-bigdata/spark-cli@latest
```

### 从 Release 手动安装

从 GitHub Releases 下载对应平台的归档，再 `tar -xzf … && mv spark-cli /usr/local/bin/` 即可。

## 配置

```bash
spark-cli config init       # 写入 ~/.config/spark-cli/config.yaml
$EDITOR ~/.config/spark-cli/config.yaml
```

```yaml
log_dirs:
  - file:///var/log/spark-history
  - hdfs://nn:8020/spark-history
hdfs:
  user: hadoop
timeout: 30s
```

也可通过 `--log-dirs` 标志或 `SPARK_CLI_LOG_DIRS` 环境变量逐次覆盖。

## 命令

| 命令 | 用途 |
|---|---|
| `spark-cli diagnose <appId>` | 一次跑全部规则; agent 首选入口 |
| `spark-cli app-summary <appId>` | 应用级总览 |
| `spark-cli slow-stages <appId>` | 按耗时排序的 Stage |
| `spark-cli data-skew <appId>` | 倾斜 Stage |
| `spark-cli gc-pressure <appId>` | 每 Stage / Executor 的 GC 占比 |

均支持 `--top N`、`--format json|table|markdown`、`--dry-run`、`--log-dirs`。

## 给 AI agent

仓库内置 `.claude/skills/spark/SKILL.md`。Claude Code 检测到即自动加载,告知 agent 「先 diagnose 再下钻」的标准流程。

## 输出契约

```json
{
  "scenario": "data-skew",
  "app_id": "...",
  "log_path": "hdfs://...",
  "log_format": "v1",
  "compression": "zstd",
  "incomplete": false,
  "parsed_events": 482113,
  "elapsed_ms": 1842,
  "columns": [...],
  "data": [...]
}
```

错误走 stderr,格式为 `{"error":{"code":..., "message":..., "hint":...}}`。退出码: `0` 成功 · `1` 内部错误 · `2` 用户错误 · `3` IO 不可达。

## 支持的 EventLog 格式

- V1 单文件 (`application_<id>`,可带 `.inprogress` / `.zstd` / `.lz4` / `.snappy` 后缀)
- V2 滚动目录 (`eventlog_v2_<id>/events_<n>_<id>`)

压缩: `zstd` 与无压缩是一等支持; `lz4` 与 `snappy` 实验性 (Hadoop block framing — 解析失败请开 issue)。

## 许可

MIT
