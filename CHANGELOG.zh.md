# 变更日志

## Unreleased

### Agent 友好性一揽子改进(2026-05-02 真实日志 dogfooding 驱动)

- **BREAKING — `sql_executions` 顶层 map description 默认截断到前 500 个 rune**(UTF-8 安全,中文 SQL 不会切坏),过长追加 `...(truncated, total <N> chars)` 标记。生产 ETL SQL 单条常 5K+ tokens,长 agent 会话经常被一份 envelope 吃光上下文。`--sql-detail=full`(或 `SPARK_CLI_SQL_DETAIL=full`、yaml `sql.detail: full`)还原原始 SQL,`--sql-detail=none` 让整段 map 缺失。
- **`data_skew` finding 改用 `wall_share` 选 primary stage**(平局回退 `skew_factor`),其余过闸门的候选按 wall_share 倒序进 `evidence.similar_stages: [{stage_id, wall_share, skew_factor}]`(最多 4 条,只收 wall_share > 0)。历史按 `skew_factor` 排,导致 wall_share 92% 的 stage 14 被 wall_share 26% 但 ratio 极端的 stage 7 盖过 —— 真实瓶颈漏报。`summary.top_findings_by_impact` 与 `findings_wall_coverage` 都跨 primary + similar_stages 聚合。
- **`SkewRule.isIdleStage` 加 `NumTasks > 2*MaxConcurrentExecutors` 守门**:任务多但因 spill / shuffle 阻塞导致 `busy_ratio` 低的 stage(实测 1000 task / 6 executor / busy_ratio 0.07)不再被误当 driver-side idle 排除。`IdleStageRule` 自身保持原阈值 —— driver-side stage 通常 NumTasks 很少。
- **`summary.findings_wall_coverage` cap 1.0**。Stage 在 wall 上并行,naive sum 可能 > 1.0(实测 4.337);cap 之后字段直观对应"finding 触及范围占应用 wall 的比例"。
- **`top_findings_by_impact[].wall_share` 取 primary + similar_stages 的 max**,而非 sum,避免并行 stage 时溢出 1.0;同时仍能反映本规则击中的最严重 stage。
- **`app-summary.top_io_bound_stages`** —— 与 `top_busy_stages` 互补的新切面,过滤 `busy_ratio < 0.8` 且(`spill_disk_gb >= 0.5` 或 `shuffle_read_gb >= 1.0`),按 wall_share 倒序 limit 3。spill 主导的真瓶颈 stage(busy_ratio 因等盘 IO 被压低,但 wall 占比大)以前被 `top_busy_stages` 阈值过滤掉,agent 完全看不到。`app-summary` 现在输出三个互补切面(by-duration / top-busy / top-io-bound),三个一起看才不漏瓶颈。
- **SHS zip 持久化磁盘缓存**。下载的 zip 落到 `<cache_dir>/shs/<host_safe>/<appID>_<lastUpdated>.zip`(原子 tmp+rename),后续 CLI 调用同一 appID 时只发 metadata JSON 比 lastUpdated,命中即直接读盘,旧 attempt zip 自动 sweep。损坏文件自动恢复(删除 + 重新下载)。`--no-cache` 退化为一次性 system temp。实际效果:warm `app-summary` / `slow-stages` 在 1 秒内返回,不再等 4-7 秒重下几 GB zip。
- **`SPARK_CLI_QUIET` 默认 TTY-aware**。未设时按 stdout 是否 TTY 决定(管道 / 重定向 / agent 调用默认静默,交互终端默认显示);`1`/`true` 强制静默,`0`/`false` 强制显示。原本注册了但没接线的 `--no-progress` flag 现在真正工作,优先级高于环境变量与 TTY 检测。`NewSHS` 不再自己读 env,由 `cmd/scenarios.resolveQuiet` 集中决断后通过 `SHSOptions{Quiet: bool}` 注入。
- **`severity` 现明确文档化为"诊断置信度",不是"ROI 优先级"**。永远按 `top_findings_by_impact[].wall_share` 排优先级 —— `disk_spill warn (wall_share 0.5)` 实际比 `data_skew critical (wall_share 0.05)` 更值得修;按 severity 字符串排会错位。文档此处明确说明,免得 agent 被误导。

### UX 与诊断覆盖度修复(基于真实作业反馈)

- **SHS 默认 timeout 60s → 5m**,且 timeout 错误统一升级成结构化 `LOG_UNREADABLE`,`hint` 直接告知 `--shs-timeout` / `SPARK_CLI_SHS_TIMEOUT`。生产 EventLog zip 几个 GB 是常态,旧默认 60s 让首次诊断直接撞墙、再去翻文档。
- **SHS 首次下载在 stderr 打进度**:`bundleFor` 在 HTTP 拉取前打 `spark-cli: downloading EventLog zip from SHS for <app> (timeout 5m0s; set SPARK_CLI_QUIET=1 to silence) ...`,完成后打 `ready in <duration>`。`SPARK_CLI_QUIET=1` 全局静默(供脚本/测试)。即使 cache 命中,Locator 仍要拉 zip 判 V1/V2 layout,这条提示消除"CLI 是不是挂了"的误判。
- **`sql_executions` map 过滤 callsite 噪音**。`isCallSiteDescription` 现在还匹配 `org.apache.spark.SparkContext.getCallSite(SparkContext.scala:2205)` 这种 DataFrame 提交常见形态;`BuildSQLExecutionMap` 把 fallback 后仍是 callsite 的条目剔除,所有条目都是噪音时整个 map 走 `omitempty` 缺失。今天复现的 sparkETL 应用从 81 行 callsite 噪音直接坍缩为 0;slow-stages 信封从 ~30 KB 缩到几 KB。
- **`data_skew` 新增任务时长紧致闸门**:`p99/p50 < 1.5` 时规则直接报 `ok`(`data-skew` 行 verdict 降到 `mild`),不论 `input_skew_factor` 多大。常见伪倾斜:某个 task 接近 0ms 把 `input_skew_factor` 拉到几千,但所有任务时长其实非常一致 —— 这种"warn"白白消耗用户注意力。极端 `p99/p50 ≥ 20` 仍跨越任何闸门保留 critical。
- **`diagnose.summary.findings_wall_coverage`** —— 所有非 ok finding 涉及 stage 的 wall_share 加和(按 stage_id 去重,每 stage 取最大值)。直接告诉调用方"finding 解释了多少 wall";低于约 0.05 时几乎所有 wall 都不在 finding 范围内,通常是作业结构 / driver 端等待问题,agent 应跳到 `app-summary.top_busy_stages` 而非继续下钻 finding。`app.DurationMs == 0` 时整段缺失。
- **`app-summary.top_busy_stages`** —— 与 `top_stages_by_duration` 并列的另一切面,过滤 `busy_ratio > 0.8`,按 `busy_ratio * duration_ms` 倒序。读 summary 时 driver-side 等待 stage 不再遮蔽真正吃 CPU 的热点。
- **`slow-stages` 行新增 `input_mb_per_task` / `shuffle_write_mb_per_task`**,与既有 `shuffle_read_mb_per_task` 并列。写侧 / source-scan 阶段也能一眼看到 partition 粒度 —— 之前 shuffle-write stage 上 read 字段恒为 0,无任何信号。

### LLM 友好的信封形状 + 按耗时占比排序的诊断

- **不兼容变更:** `slow-stages` 与 `data-skew` 信封顶层新增一份共享的 `sql_executions: {<int64 id>: <description>}` map;每行的 `sql_description` 列已删除,改为通过 `sql_execution_id` 引用。生产作业(SQL 文本动辄几十行)JSON 体积从 40+ KB(每行重复嵌入)降到几 KB(顶层一份)。`omitempty` 保证非 SQL 作业 / 无需 SQL 关联的场景下该字段缺失。
- `data_skew` 规则与 `data-skew` 行新增 **`wall_share` 闸门**:当 stage 的 wall-clock 不到应用 wall 的 1% 且 `p99/p50 < 20` 时,severity 降级(规则 → `warn`,行 verdict → `mild`)。短 stage 的尾抖动不应占用优先级。`app.DurationMs <= 0`(没 ApplicationEnd 事件)时 wall_share 视作"未知",**不**触发闸门,避免没结束事件的日志被全面降级。规则 evidence 在 wall_share 已知时输出该字段,DataSkew 行始终带 `wall_share` 字段(未知则为 0)。
- `slow-stages` 行新增 `busy_ratio`(与 `app-summary.top_stages_by_duration[]` 同公式 `TotalRunMs / (wall * effective_slots)`)与 `shuffle_read_mb_per_task`(`TotalShuffleReadBytes / NumTasks`,MiB),agent 直接看出 driver-idle stage 与 partition 过粗导致 spill 的场景,不必再心算。
- `disk_spill` 规则 evidence 新增 `partitions`(stage 的 `NumTasks`)与 `est_partition_size_mb`(`shuffle_read / num_tasks` 转 MiB),让建议可以基于实际 partition 大小对比 `spark.executor.memory`,不再只是套模板说"调高 shuffle.partitions"。
- `diagnose` 信封的 `summary` 新增 `top_findings_by_impact: [{rule_id, severity, wall_share}]`,按 `wall_share` 倒序。只收录 evidence 含 `stage_id` 关联且 `wall_share > 0` 的 finding;`app.DurationMs == 0` 时整段缺失(`omitempty`)。agent / 用户直接读优先级,不必再二次下钻 + 心算。
- 新增基于反射的列契约测试 `SlowStageRow` ↔ `SlowStagesColumns()`、`DataSkewRow` ↔ `DataSkewColumns()`,与既有 `app_summary` 守门测试一致;column / row 字段错配将在单测层面立刻报警。

### 诊断精度与可读性修复

- `slow-stages` 行新增 `gc_ratio` 字段(`sum(task_gc) / sum(task_run)`),解决调用方用 `gc_ms / duration_ms` 在多 executor 并发下得到 >100% 的诡异值。
- `app-summary.top_stages_by_duration[]` 行新增 `busy_ratio` 字段,driver 端 idle stage(broadcast / planning / 文件 listing)在 top 列表里一眼可辨,不再伪装成"最慢真实 stage"。
- `data_skew` 规则在输入均匀(`input_skew_factor < 1.2`)且 `p99/p50 < 20` 时,把 critical 降为 warn —— 数据均匀的长尾通常是抖动而非真倾斜。极端 ratio(≥ 20)仍保留 critical。`data-skew` 行 verdict 同口径降级。
- `data_skew` 规则跳过命中 `idle_stage` 条件的候选 stage(wall ≥ 30s 且 `busy_ratio < 0.2`)—— idle stage 上的 task 长尾本质是调度噪音,不应报数据倾斜。
- `data_skew` finding 的 `evidence` 新增 `input_skew_factor`。
- `stageSQL` 在 `description` 是 Spark 默认 callsite (`getCallSite at SQLExecution.scala:74`) 或为空时,回退到 `details` 首行(典型 DataFrame API 作业行为)。`data-skew` / `slow-stages` 行的 `sql_description` 对 DataFrame 作业终于有可用输出。

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
