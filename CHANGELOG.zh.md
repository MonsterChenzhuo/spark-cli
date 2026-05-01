# 变更日志

## Unreleased

### Spark History Server EventLog 源

- 新增 `shs://host:port` scheme,可写入 `--log-dirs`。spark-cli 通过 `GET /api/v1/applications/<id>/<attempt>/logs`(返回 zip 包)拉日志,把 zip 内部条目以现有 `fs.FS` 抽象暴露,定位器、解码器、规则、应用解析缓存全部透明工作。
- 自动选取 `/api/v1/applications/<id>` 返回数值最大的 `attemptId`。当 SHS 返回的 attempt 完全没有 `attemptId` 字段(Spark 3.4+ 单 attempt 默认行为)时,spark-cli 会省略 attempt 段、改用 `/api/v1/applications/<id>/logs` —— 修复对该类应用报 APP_NOT_FOUND 的问题。
- 新增 flag `--shs-timeout`、环境变量 `SPARK_CLI_SHS_TIMEOUT`、YAML 字段 `shs.timeout`(默认 `60s`)。`spark-cli config show` 输出当前值与来源。
- 仅 HTTP —— TLS、Basic Auth、Bearer Token、Kerberos 暂不在 v1 范围内。
- `Content-Length` ≤ 256 MiB 的 zip 在内存解码;更大或未知长度时 spill 到 `os.CreateTemp`,进程退出时清理。
- **已知限制**:即便应用解析缓存命中,每次调用仍要下载 zip —— `Locator.Resolve` 必须读 zip 内容才能判定 V1 / V2 布局。持久化 zip 缓存在 roadmap 上。

### Application 缓存层

- `internal/cache` 把解析后的 `*model.Application` 用 `gob+zstd` 序列化到 `$XDG_CACHE_HOME/spark-cli/`(或 `~/.cache/spark-cli/`)。同一 `appId` 的首条命令照常解析;之后的命令绕过 Open + Decode + Aggregate,<300 ms 返回(信封 `parsed_events=0` 标识命中)。
- 新增 flag `--cache-dir <path>`、`--no-cache`(本次执行不读不写);环境变量 `SPARK_CLI_CACHE_DIR`;YAML 字段 `cache.dir`。
- 缓存失效条件: V1 源文件 mtime/size 改变;V2 任一分片 mtime / 总 size / 分片数变化。`.inprogress` 日志永不缓存。`spark-cli config show` 输出 `cache.dir` 及其来源(flag/env/file/default)。
- 缓存失败(损坏文件、schema 不匹配、写盘失败)静默退化为 "miss + 重新解析",绝不向用户报错。
- `internal/stats.Digest` 新增 `gob.GobEncoder` / `GobDecoder`,缓存中的 `*Application` 完整保留分位数状态。
- `internal/fs.FileInfo` 新增 `ModTime`(UnixNano),作为缓存 key 失效判定的依据。

### 诊断规则 —— 携带 SparkConf 的建议
- `internal/eventlog` 新增解析 `SparkListenerEnvironmentUpdate`,把运行时 Spark Properties 写入 `Application.SparkConf`。
- `disk_spill` / `gc_pressure` / `data_skew` 规则在 evidence 和 suggestion 中带出当前真实配置(`spark.sql.shuffle.partitions`、`spark.executor.memory`、`spark.sql.adaptive.skewJoin.enabled` 等)。skew 规则在检测到 AQE skewJoin 已开启仍长尾时,建议改为调整 `skewedPartitionFactor`,而不是再喊"启用 AQE"。

### slow-stages / data-skew 关联 SQL execution
- `slow-stages` 和 `data-skew` 行新增 `sql_execution_id` 与 `sql_description` 字段。映射来自 `SparkListenerJobStart.Properties` 中的 `spark.sql.execution.id` + `SparkListenerSQLExecutionStart`。非 SQL job 触发的 stage 输出 `sql_execution_id: -1`、`sql_description` 为空字符串。
- `Application` 新增字段 `SparkConf`、`SQLExecutions`、`StageToSQL`。

### failed_tasks —— 节点级失败模式识别
- `internal/eventlog` 新增解析 `Node{Blacklisted,Excluded}ForStage` 与 `Executor{Blacklisted,Excluded}ForStage` 事件,写入 `Application.Blacklists`。
- `failed_tasks` 规则在同一台 host 出现 ≥2 次 blacklist 事件时,即使整体失败率不高也会升级为 `critical`,evidence 中带出 `blacklisted_hosts` / `blacklist_node_events` / `blacklist_executor_events`,suggestion 直接指向具体节点的硬件/网络/磁盘排查。普通随机失败仍走原文案。

### EventLog 定位
- `internal/eventlog/locate.go` 支持 V2 目录的可选 `_<attempt>` 后缀(`eventlog_v2_<appId>` 或 `eventlog_v2_<appId>_<n>`),多 attempt 共存时自动取最大,与 Spark History Server 行为一致。此前严格等值匹配,无法识别 Spark 实际写出的带 attempt 计数器的滚动日志目录。
- `resolveV1` 同样容忍 V1 单文件的 `_<attempt>` 后缀(`application_<id>_<attempt>`,对应 `spark.eventLog.rolling.enabled=false` 时 Spark 写出的命名)。裸名仍优先于带 attempt 的兄弟文件;裸名不存在时取最大 attempt。codec / `.inprogress` 后缀正常叠加。

### HDFS 配置
- `internal/fs/hdfs.go` + 新增 `internal/fs/hdfs_conf.go` —— 通过 `hadoopconf.Load` + `hdfs.ClientOptionsFromConf` 加载 `core-site.xml` / `hdfs-site.xml`,支持 HA NameService 地址解析。
- 新增 `--hadoop-conf-dir <path>` flag、`SPARK_CLI_HADOOP_CONF_DIR` 环境变量、YAML 字段 `hdfs.conf_dir`。自动发现路径: `HADOOP_CONF_DIR` → `HADOOP_HOME/etc/hadoop` → `HADOOP_HOME/conf`。都没拿到 conf 时退回 URI 字面 `host:port`。
- `spark-cli config show` 现在显示 `hdfs.conf_dir` 及其来源(flag/env/file/default);`spark-cli config init` 增加可选的 Hadoop conf dir 输入。
- Kerberos / SASL / TLS 仍不支持 —— 仅适用于 simple auth + HA 集群。

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
