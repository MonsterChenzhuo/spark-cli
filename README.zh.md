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

每行除 `skew_factor` 外还带 `input_skew_factor` 与 `wall_share`。`data_skew` 现在有四道闸门:**任务时长紧致**(`p99/p50 < 1.5`)直接 ok(任务时长本身就均匀,不存在真倾斜);输入均匀(`input_skew_factor < 1.2` 且 `p99/p50 < 20`);stage `wall_share < 1%`;命中 idle stage 条件(busy < 0.2)。极端 `p99/p50 ≥ 20` 跨越任何闸门保留 critical。`slow-stages` 行带 `gc_ratio`、`busy_ratio` 与三档 `*_mb_per_task`(input / shuffle_read / shuffle_write),读侧 / 写侧 / source-scan 阶段都能一眼判 partition 粒度。`app-summary` 同时给 `top_stages_by_duration[]` 与 `top_busy_stages[]`(后者过滤 `busy_ratio > 0.8` 后按 `busy_ratio * duration` 倒序 —— 这才是真正吃 CPU 值得调优的热点)。`diagnose.summary` 加 `top_findings_by_impact: [{rule_id, severity, wall_share}]`(按 wall_share 倒序)与 `findings_wall_coverage`(按 stage 去重的总 wall_share);**coverage < 0.05 时瓶颈在作业结构层,直接看 `top_busy_stages` 而非继续下钻 finding**。每行重复嵌入的 SQL 文本上提到 envelope 顶层 `sql_executions: {<id>: <description>}` 一份共享(仅 slow-stages / data-skew);DataFrame 作业那种 callsite 占位文本会被整行过滤,所有条目都是噪音时整段 map 缺失。

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
  - hdfs://mycluster/spark-history     # 写 HA NameService 逻辑名 (推荐)
  # - hdfs://nn:8020/spark-history     # 或写具体 host:port
  # - shs://history.example.com:18081  # Spark History Server REST API
hdfs:
  user: hadoop
  conf_dir: /etc/hadoop/conf           # 可选; 留空则按 HADOOP_CONF_DIR / HADOOP_HOME 自动发现
shs:
  timeout: 5m   # 默认值;生产 EventLog zip 几个 GB 是常态,首次下载会在 stderr 打进度提示(SPARK_CLI_QUIET=1 静默)
yarn:
  base_urls:
    - http://203.123.81.20:7765/gateway/hadoop-prod/yarn  # 可选;RM/gateway 前缀
timeout: 30s
```

也可通过 `--log-dirs` / `--yarn-base-urls` 标志或 `SPARK_CLI_LOG_DIRS` /
`SPARK_CLI_YARN_BASE_URLS` 环境变量逐次覆盖。

### HDFS 配置

- 使用纯 Go 客户端 (`github.com/colinmarc/hdfs/v2`),**自动读取** `core-site.xml` / `hdfs-site.xml`,支持 HA NameService。
- NameNode 地址优先级 (高 → 低):
  1. `--hadoop-conf-dir <path>` 命令行
  2. `SPARK_CLI_HADOOP_CONF_DIR` 环境变量
  3. `config.yaml` 的 `hdfs.conf_dir`
  4. `HADOOP_CONF_DIR` 环境变量
  5. `HADOOP_HOME/etc/hadoop` 或 `HADOOP_HOME/conf`
  6. 都没拿到 conf 时,退回 `--log-dirs` 里 `hdfs://host:port/...` 的字面 `host:port` (此时不支持 HA 逻辑名)
- HDFS 用户名优先级 (高 → 低): `--hdfs-user` → `SPARK_CLI_HDFS_USER` → `hdfs.user` → `$USER`。注意是 `$USER`,**不是** Hadoop 原生的 `$HADOOP_USER_NAME`。
- **暂不支持** Kerberos / SASL / TLS,仅适用于 simple-auth 集群。

### Spark History Server

把 `--log-dirs` 指向 SHS REST 端点,spark-cli 会通过
`GET /api/v1/applications/<id>/<attempt>/logs`(返回 zip 包)拉取 EventLog,
然后让定位器、解码器、规则、应用解析缓存全部透明工作 —— 跟读 `file://` /
`hdfs://` 没有区别。

```bash
spark-cli diagnose application_1771556836054_861265 \
  --log-dirs shs://history.example.com:18081
```

- 自动选择数值最大的 `attemptId`。
- 仅 HTTP;**不支持** TLS、Basic Auth、Bearer Token、Kerberos。
- 超时优先级(高 → 低):`--shs-timeout` flag → `SPARK_CLI_SHS_TIMEOUT` 环境变量
  → `config.yaml` 的 `shs.timeout` → 默认 `5m`。timeout 失败会返回结构化 `LOG_UNREADABLE`,`hint` 直接告诉用户去调这个 flag —— 首次失败后无需翻文档。
- 进度提示优先级:`--no-progress` flag → `SPARK_CLI_QUIET`(`1/true` 静默 / `0/false` 强制显示) → stdout TTY 检测;**agent 重定向 stdout 时默认静默,交互终端默认显示**。
- **持久化磁盘 zip 缓存**(自 v0.x):下载的 zip 落到 `<cache_dir>/shs/<host>/<appID>_<lastUpdated>.zip`(原子 tmp+rename),后续 CLI 调用同一 appID 时只发 metadata JSON 比 lastUpdated,命中即直接读盘。同 appID 的旧 attempt zip 在每次成功下载后自动 sweep。`--no-cache` 退化为一次性 system temp。
- 不开磁盘缓存时:`Content-Length` ≤ 256 MiB 整包读到内存,更大或长度未知时落 `os.CreateTemp` 进程退出清理;开磁盘缓存时一律走 tmp+rename 落盘。
- `shs://` 支持 gateway path,例如 `shs://host:7765/gateway/prod/sparkhistory`;
  CLI 会保留 path 前缀拼接 `/api/v1/applications/...`。

### YARN 日志

配置 `yarn.base_urls` 后,`diagnose` 会在原有 Spark EventLog 诊断 envelope 顶层
追加 `yarn` 字段,包含 RM application 状态、diagnostics、最新 attempt 的 container
日志入口 URL。默认不把日志正文塞进 `diagnose`,避免一次诊断输出过大。

需要单独看 NodeManager 日志时:

```bash
spark-cli yarn-logs application_1772605260987_20682 \
  --yarn-base-urls http://203.123.81.20:7765/gateway/hadoop-prod/yarn \
  --top 5 --yarn-log-bytes 65536
```

`yarn-logs` 会通过 RM REST 找 application/user/attempt/container,再生成类似
`/nodemanager/node/containerlogs/<container>/<user>?scheme=http&host=<nm>&port=<port>`
的 gateway URL,并抓取 `stderr` / `stdout` / `syslog` 的前 N 字节摘要。

需要看 driver 是否卡在 SQL 优化、driver 端 IO、RPC 等逻辑时,可以直接通过
YARN tracking/proxy URL 拉 Spark UI thread dump:

```bash
spark-cli driver-thread-dump application_1772605260987_20765 \
  --yarn-base-urls http://203.123.81.20:7765/gateway/hadoop-prod/yarn \
  --executor-id driver
```

输出包含 `state_counts` 和 Spark UI 原始线程栈;`--executor-id` 也可传具体
executor id。

### 缓存

第一次解析某个 EventLog 后,`*model.Application` 会以 `gob+zstd` 形式写到 `<cache_dir>/<appId>.gob.zst`。同一 appId 的后续命令直接读缓存,无论日志多大都能 <300 ms 返回(信封 `parsed_events=0` 标识命中)。

| 来源 | 缓存目录 |
|---|---|
| `--cache-dir` flag | (最高优先级) |
| `SPARK_CLI_CACHE_DIR` 环境变量 | |
| `config.yaml: cache.dir` | |
| `$XDG_CACHE_HOME/spark-cli` | |
| `~/.cache/spark-cli` | (默认) |

`--no-cache` 让本次执行不读不写缓存。`.inprogress` 日志永远不缓存。源文件 mtime/size(V1)或任一分片 mtime / 总 size / 分片数(V2)变化都会自动失效。所有缓存失败(损坏文件、schema 不匹配、写盘失败)都静默退化为 "miss + 重新解析"。

## 命令

| 命令 | 用途 |
|---|---|
| `spark-cli diagnose <appId>` | 一次跑全部规则; agent 首选入口 |
| `spark-cli app-summary <appId>` | 应用级总览 |
| `spark-cli slow-stages <appId>` | 按耗时排序的 Stage |
| `spark-cli data-skew <appId>` | 倾斜 Stage |
| `spark-cli gc-pressure <appId>` | 每 Stage / Executor 的 GC 占比 |
| `spark-cli yarn-logs <appId>` | 通过 YARN RM/NM 获取应用 diagnostics 与 container 日志摘要 |
| `spark-cli driver-thread-dump <appId>` | 通过 YARN tracking/proxy URL 获取 Spark UI driver/executor thread dump |
| `spark-cli config show [--format json]` | 打印当前生效配置(yaml / env / default 来源标注) |
| `spark-cli cache list` / `cache clear [--app <id>] [--dry-run]` | 查看 / 清理本地的应用 + SHS zip 缓存 |
| `spark-cli version` (与 `--version`) | 打印 spark-cli 版本 |

均支持 `--top N`、`--format json|table|markdown`、`--dry-run`、`--log-dirs`、
`--cache-dir`、`--no-cache`、`--shs-timeout`、`--no-progress`、
`--yarn-base-urls`、`--yarn-log-bytes`、`--executor-id`、`--sql-detail truncate|full|none`(默认 `truncate` 把 SQL description 截到前
500 个 rune 加 `...(truncated, total <N> chars)`;`full` 还原原始 SQL,`none`
让整段 `sql_executions` 缺失)。

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
  "data": [...],
  "sql_executions": {
    "0": "select count(*) from orders where dt = '2026-05-01'"
  }
}
```

各场景特例:
- `gc-pressure` 的 `data` 是 executor 行的扁平数组(`[{executor_id, host, tasks, run_ms, gc_ms, gc_ratio, verdict}, ...]`);早期 spec 设想的 `{by_stage, by_executor}` 双段已收敛为单段。
- `diagnose` 顶层带 `summary: {critical, warn, ok, top_findings_by_impact?, findings_wall_coverage?}`。`top_findings_by_impact` 按 `wall_share` 倒序(单条 finding 取 primary + similar_stages 的 max);`findings_wall_coverage` 是这些 wall_share 按 stage 去重后加和,**cap 到 1.0**(stage 在 wall 上并行时 naive sum 可能 > 1)。两者在 `app.DurationMs == 0` 时整段 omitempty。**coverage < 0.05 ⇒ 瓶颈在作业结构层,跳到 `app-summary.top_busy_stages` / `top_io_bound_stages` 看真热点,别继续下钻 finding**。**`severity` 是诊断置信度,不是 ROI** —— 永远以 `top_findings_by_impact` 排序为准。
- `diagnose` 新增 `executor_supply` 规则。静态分配时会比较 `spark.executor.instances` 与 EventLog 观察到的 `executors_added` / `max_concurrent_executors`;命中只能证明 EventLog 里 executor 供给不足,**不能**证明 YARN ResourceManager 为什么没继续分配。根因仍需查 RM/AM 的 pending containers、user limit、队列容量、节点标签、reserved containers、资源碎片等诊断。
- `data_skew` finding 在多 stage 命中时,evidence 含 `similar_stages: [{stage_id, wall_share, skew_factor}]`(按 wall_share 倒序最多 4 条);primary `stage_id` 选 wall_share 最大的(平局回退 skew_factor)。`top_findings_by_impact` 与 `findings_wall_coverage` 都跨 primary + similar_stages 聚合 —— agent 直接读 evidence 就能看到完整列表,不必再回头跑 `data-skew`。
- `app-summary` 在实际 executor 计数旁输出 executor 请求配置(`dynamic_allocation_enabled`、`configured_executor_instances`、`executor_cores`、`executor_memory`、`executor_memory_overhead`),并同时输出三个互补 stage 切面:`top_stages_by_duration`(按 wall 倒序,含 driver-side 等待)、`top_busy_stages`(`busy_ratio > 0.8` 真 CPU 热点)、**`top_io_bound_stages`**(`busy_ratio < 0.8` 但 `spill >= 0.5 GB` 或 `shuffle_read >= 1 GB`,IO 阻塞但 wall 大的 stage)。**只看 `top_busy_stages` 会错过 spill 主导的最大瓶颈**,三个切面合起来才不漏。
- `slow-stages` 与 `data-skew` 顶层带 `sql_executions: {<id>: <description>}`,row 通过 `sql_execution_id` 引用。**description 默认 truncate 到前 500 个 rune**(过长追加 `...(truncated, total <N> chars)`),`--sql-detail=full` 还原原始 SQL,`--sql-detail=none` 整段 omit。DataFrame 作业那种 callsite 占位(`org.apache.spark.SparkContext.getCallSite(...)`)会被过滤,所有条目都是噪音时整段 map 走 `omitempty` 缺失。

错误走 stderr,格式为 `{"error":{"code":..., "message":..., "hint":...}}`。退出码: `0` 成功 · `1` 内部错误 · `2` 用户错误 · `3` IO 不可达。

## 支持的 EventLog 格式

- V1 单文件 (`application_<id>`,可带 `.inprogress` / `.zstd` / `.lz4` / `.snappy` 后缀)
- V2 滚动目录 (`eventlog_v2_<id>/events_<n>_<id>`)

压缩: `zstd` 与无压缩是一等支持; `lz4` 与 `snappy` 实验性 (Hadoop block framing — 解析失败请开 issue)。

## 许可

MIT
