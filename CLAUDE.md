# CLAUDE.md

Guidance for Claude Code (and other AI agents) working in this repository.

## What this is

`spark-cli` —— 单二进制 Go CLI,解析 Apache Spark EventLog,输出结构化 JSON 信封,面向 AI agent 的 Spark 性能诊断流程。Go 1.22,模块路径 `github.com/opay-bigdata/spark-cli`。

## 顶层契约

每条场景命令 (`app-summary` / `slow-stages` / `data-skew` / `gc-pressure` / `diagnose`) 在 stdout 输出**一个** `scenario.Envelope` JSON 对象;错误统一走 stderr,格式 `{"error":{"code","message","hint"}}`,退出码 `0/1/2/3`。改动任何场景或输出层时**不要破坏这个信封形状** —— `tests/e2e/e2e_test.go` 是契约守门人。

特例:
- `gc-pressure` 的 `data` 是数组 (与其他场景一致),非对象 —— 早期 spec 设想的双段已收敛为单段
- `diagnose` 的信封额外带 `summary: {critical, warn, ok}`

## 仓库布局

| 路径 | 用途 |
|---|---|
| `cmd/` | cobra 根命令 + version + config 子命令 |
| `cmd/scenarios/` | 5 个场景命令 + `runner.Run()` 主管线 + `dispatch.go` 分发 |
| `internal/config/` | YAML 配置加载、环境变量、flag 覆盖 |
| `internal/errors/` | `cerrors.Error` 结构 + 退出码映射 |
| `internal/fs/` | `file://` / `hdfs://` 抽象 (FS 接口) |
| `internal/eventlog/` | 日志定位 (V1/V2)、解压、JSONL 解码、事件分发 |
| `internal/model/` | Application/Stage/Executor/Job 聚合模型 + Aggregator |
| `internal/stats/` | t-digest 分位数封装 |
| `internal/scenario/` | 场景纯函数 + Envelope 类型 |
| `internal/rules/` | Rule 接口 + 6 条规则 (skew/gc/spill/failed/tiny/idle_stage) |
| `internal/output/` | JSON / Table / Markdown formatter |
| `tests/e2e/` | 通过 `cmd.RunWith` 跑全场景 + 错误路径 |
| `tests/testdata/tiny_app.json` | 10 行合成 EventLog,所有 E2E 共用 |
| `.claude/skills/spark/SKILL.md` | 内置 agent skill — 教 Claude Code 「先 diagnose 再下钻」 |

## 关键数据流

```
appId (CLI) → config → fs.FS map → eventlog.Locator.Resolve → LogSource
LogSource → eventlog.Open (decompress + V2 拼接) → eventlog.Decode → model.Aggregator → *model.Application
*model.Application → scenario.<X>(app) → Envelope.{Columns, Data, Summary}
Envelope → output.Write{JSON|Table|Markdown}
```

`runner.Run` 是唯一入口 (`cmd/scenarios/runner.go`);所有 cobra 命令通过 `register.go` 收敛到它。

## 开发约定

- **Go 1.22 锁定**: `go.mod` 显式 `go 1.22`,不要被工具链自动 bump (有依赖如 tdigest 想要更高版本,但本仓库限定 1.22)。
- **TDD**: 新场景/规则先写失败测试再写实现,见每个 `*_test.go`。
- **回应中文**: 仓库内交互、commit 信息可中英混合,但解释/讨论默认中文。
- **Commit 信息格式**: `type(scope): subject` —— `feat(scenario):`、`feat(output):`、`fix(model):`、`test(e2e):`、`docs(skill):`、`ci:`、`style:` 等。每个 commit 末尾带 `Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>`。
- **不要随手扩功能**: 任务/bug 只动相关代码,别顺手重构 (除非任务即重构)。
- **Envelope 改动需同步**: 改 `scenario.Envelope` JSON tag 或字段时,务必跑 `tests/e2e` + 更新 `.claude/skills/spark/SKILL.md` 的 envelope 文档 + `README.md` / `CHANGELOG.md`。
- **CLAUDE.md 同步规则**: 每次新增/修改用户可见功能(CLI flag、环境变量、配置项、命令行为、输出契约、依赖发现路径)或调整开发流程时,**必须**同步更新本文件相关章节(常见入口:开发约定、HDFS 连接、添加新场景/规则、已知踩坑),并在同一 commit 中带上 README/CHANGELOG 的对应变更。原因:本仓库的 AI agent 工作流强依赖 CLAUDE.md 作为唯一权威上下文,文档漂移会让后续会话直接做错事。

## 常用命令

所有 gate 都从仓库根执行；提交前必须 `make tidy && make lint && make unit-test` 全绿（CI 同款）。

```bash
make tidy            # go mod tidy —— go.mod / go.sum 不能产生 diff
make lint            # go vet + gofmt -l + golangci-lint v2 (含 formatters)
make unit-test       # go test -race -count=1 ./...
make e2e             # build 后跑 -tags=e2e 的 tests/e2e/...
make build           # 出 ./spark-cli，带 git describe ldflag
make release-snapshot  # goreleaser snapshot —— 4 平台 tarball 进 dist/
```

CI（`.github/workflows/ci.yml`）单 job 串行跑：`go.mod` 锁版本 → `go mod download` → tidy 清洁 → `go vet` → gofmt → `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.1.6 run` → race 单测 → 带 `cmd.version=ci` ldflag 的 build → `--help` / `version` 烟囱 → `-tags=e2e` 的 e2e dry-run → SKILL.md 前置元数据校验。

```bash
# 烟囱（手动验证 envelope 形状）
mkdir -p /tmp/spark-cli-smoke
cp tests/testdata/tiny_app.json /tmp/spark-cli-smoke/application_1_1
go run . diagnose application_1_1 --log-dirs file:///tmp/spark-cli-smoke
go run . app-summary application_1_1 --log-dirs file:///tmp/spark-cli-smoke --format table
```

## 添加新场景的标准步骤

1. `internal/scenario/<name>.go` + `<name>_test.go` —— 纯函数,输入 `*model.Application`,输出 row 列表 + `Columns()` 函数
2. 若需新模型字段 —— 同步扩 `internal/model/{model.go,aggregator.go}` + 加单元测试 (`OnTaskEnd` 累计逻辑别打破现有)
3. `cmd/scenarios/dispatch.go` 加 `case` 分支,把 row 转 `[]any` 装进 `env.Data`
4. `cmd/scenarios/register.go` 加一行 `newScenarioCmd("<name>", "<short>")`
5. `tests/e2e/e2e_test.go` 加一个 case
6. `.claude/skills/spark/SKILL.md` 与 `README.md` 同步说明
7. `CHANGELOG.md` / `CHANGELOG.zh.md` 写一条
8. 若 step 2 改了 `model` 现有字段的**类型**或**名称**,bump `internal/cache/envelope.go` 的 `currentSchemaVersion` —— 仅新增字段则不需要

## 添加新诊断规则

1. `internal/rules/<x>_rule.go` 实现 `Rule` 接口 (`ID()`、`Title()`、`Eval(*model.Application) Finding`)
2. `internal/rules/rule.go` 的 `All()` 列表追加
3. `internal/rules/rule_test.go` 加触发 + 静默测试
4. `tests/e2e` 自动覆盖 (因为 `diagnose` 跑全部规则)

## 发版

`.github/workflows/release.yml` 同时响应两类事件，由 `release` concurrency group 串行化：

1. **`push` 到 `main`**：runner 自动从 `git tag -l 'v*'` 找出最大 tag，bump patch 段（`v0.1.0` → `v0.1.1`），打 annotated tag 推回 origin —— 这一步会再次触发本 workflow 进入下面的 tag 分支。
2. **`v*` tag 推送**：跑 `goreleaser release --clean`，出 4 平台 tarball（linux/darwin × amd64/arm64），打包 `LICENSE`、`README.md`、`README.zh.md`、`CHANGELOG.md`、`CHANGELOG.zh.md`、`.claude/skills/spark/SKILL.md`，并附 `checksums.txt`。

> 想跳过自动 bump、自己控制版本号？直接 `git tag -a vX.Y.Z -m vX.Y.Z && git push origin vX.Y.Z`，main 分支别动即可。

`scripts/install.sh` 通过 GitHub `releases/latest` redirect 解析最新 tag（API 限流时回落到 `/repos/.../releases/latest`），下载归档后用 `checksums.txt` 做 sha256 校验，再把 binary 装到 `PREFIX` (默认 `/usr/local/bin`，必要时 `sudo`)、把 skill 树镜像到 `SKILL_DIR` (默认 `~/.claude/skills/spark`)。环境变量：`VERSION` / `PREFIX` / `SKILL_DIR` / `NO_SUDO` / `NO_SKILL` / `REPO`，详见脚本头部注释。

## HDFS 连接

`internal/fs/hdfs.go` + `hdfs_conf.go` 走纯 Go 客户端 (`github.com/colinmarc/hdfs/v2`),**支持** Hadoop XML 配置和 HA NameService,**不支持** Kerberos / SASL / TLS。

NameNode 地址来源 (按优先级,高 → 低):

1. `--hadoop-conf-dir <path>` flag → `cfg.HDFS.ConfDir`
2. `SPARK_CLI_HADOOP_CONF_DIR` 环境变量
3. `config.yaml` 的 `hdfs.conf_dir`
4. `HADOOP_CONF_DIR` 环境变量
5. `HADOOP_HOME/etc/hadoop` 或 `HADOOP_HOME/conf`
6. 上述都没拿到 conf 时,退回 `--log-dirs` 里 `hdfs://host:port/...` 的字面 `host:port`

`fs.LoadHadoopConf("")` 内部统一处理 4-5 (用 `hadoopconf.LoadFromEnvironment` 不行,因为它静默吞错;我们自己读 env 以便 `--hadoop-conf-dir` 显式失败时报错)。`fs.BuildClientOptions(conf, user)` 把 `core-site.xml` 的 `fs.defaultFS` + `hdfs-site.xml` 的 `dfs.nameservices` / `dfs.namenode.rpc-address.<ns>.<nn>` 解析成 `[]string` Addresses,自带 HA failover。

HDFS 用户名优先级 (高 → 低): `--hdfs-user` flag → `SPARK_CLI_HDFS_USER` → `config.yaml hdfs.user` → `$USER`。注意是 `$USER`,**不是** Hadoop 原生客户端的 `$HADOOP_USER_NAME`。

**关键约束**: `internal/fs/hdfs.go` 的 `HDFS.addr` 字段保存的是用户在 URI 里写的字面 host (例如 `mycluster` 或 `nn1:8020`),**不是**实际连接的 NameNode 地址。`List()` 用它拼回 `hdfs://<addr>/...` URI,这套 URI 会回流到 `eventlog.Locator` 做 prefix matching 对照 `cfg.LogDirs`。改造时不要把 `addr` 替换成 `opts.Addresses[0]`,否则 HA 场景下 Locator 直接匹配不上。

## Spark History Server 接入

`internal/fs/shs.go` 把 Spark History Server 的 REST API (`GET /api/v1/applications/<id>/<attempt>/logs` 返回 zip) 包成第三种 `fs.FS` backend。用户在 `--log-dirs` 写 `shs://host:port`,Locator / decoder / 规则 / 缓存层都不需要改 —— SHS 把 zip 内文件暴露成 `shs://host:port/<appID>/<inner-zip-path>` 形式的合成 URI,FileInfo 的 `ModTime` 取自 attempt.lastUpdated(毫秒 → 纳秒),让 `cache.computeSourceKey` 的失效逻辑天然生效。

**HTTP 行为**(全部不可配,简单粗暴):
- 仅支持匿名 HTTP,**不支持** HTTPS / Basic Auth / Bearer Token / Kerberos
- attempt 自动取 `attempts[]` 里数字 attemptId 最大那个(对齐 Spark History Server UI 默认行为)
- HTTP timeout 由 `cfg.SHS.Timeout` 控制,优先级:`--shs-timeout` flag → `SPARK_CLI_SHS_TIMEOUT` 环境变量 → `config.yaml` `shs.timeout` → 默认 60s
- zip body ≤ 256 MiB(由 `Content-Length` 判定)走 `bytes.Reader` 全内存解析;> 256 MiB 或 `Content-Length` 缺失时落 `os.CreateTemp`,`SHS.Close()` 删除 tmp 文件
- 同一 appID 的 zip 在 SHS 实例生命周期内只下载一次(`bundles map[appID]*shsBundle`),之后所有 List/Stat/Open 都走内存索引

**关键约束**:
- `splitSHSURI` 把 `shs://host[:port][/appID[/inner...]]` 解析成 `(host, appID, inner)`;新增的 URI 路径段都要走它,**不要**自己 sprintf 凑路径,否则 `appIDFromListPrefix` 的还原逻辑会和 List 的入参对不上。
- Locator 调 `List(base, prefix)` 时,我们从 prefix 反推 appID(剥 `eventlog_v2_` 前缀)——所以 prefix **必须**是 `application_<id>` 或 `eventlog_v2_application_<id>`;新增带 attempt 后缀的 prefix 时记得回头改 `appIDFromListPrefix`。
- 缓存命中仍要下载 zip:`internal/cache` 命中只是跳过 `Open + Decode + Aggregate`,但 `Locator.Resolve` 仍要 List + Stat,这两步触发 SHS 下载 zip 来判断 V1/V2 layout。预算评估按"每条 SHS 命令至少一次 zip 下载"算。后续优化方向:把 zip 持久化到 `<cache_dir>/shs/<host>/<appID>_<lastUpdated>.zip` 让 warm 命令变成 HTTP HEAD + 本地读盘,**当前 PR 不做**。

## Application 缓存

`internal/cache` 把首次解析得到的 `*model.Application` 用 `gob+zstd` 序列化到磁盘,让同一 appId 的后续命令绕过 `Open + Decode + Aggregate`,目标是把多 GB EventLog 的二次访问从秒级降到 <300 ms。

**缓存目录优先级**(高 → 低):

1. `--cache-dir <path>` flag
2. `SPARK_CLI_CACHE_DIR` 环境变量
3. `config.yaml` 的 `cache.dir` 字段
4. `$XDG_CACHE_HOME/spark-cli/`
5. `~/.cache/spark-cli/`

`runner.Run` 在 `Locator.Resolve` 之后、`parseApp` 之前调 `Cache.Get`;命中时直接走 `buildScenarioBody`,信封里 `parsed_events=0`,首条 `--format json` 仍然能区分 cold/warm。

**失效规则**:

- V1: `{URI, mtime, size}` 任一变化触发 miss
- V2: `{URI, max(part_mtime), sum(part_size), part_count}` 任一变化触发 miss
- `.inprogress` 日志整体跳过缓存(不读不写)
- `--no-cache` 旁路缓存(不读不写),仍可与 `--cache-dir` 共存(后者只决定写盘位置,本次执行依然不写)

**Schema 版本**: `internal/cache/envelope.go` 的 `currentSchemaVersion` 是手动维护的整型常量。**字段加/删** gob 天然容忍,**不需要** bump;**字段改类型或重命名时**必须 bump,否则用户拿到的是用零值/老结构填充的 `*Application`。bump 后旧缓存自动作废重建,对用户透明。

**关键约束**: 缓存层永远不能让 CLI 失败 —— 所有错误(写盘失败、损坏文件、解码错误)都退化为 "miss + 重新解析"。损坏文件读到时直接 `os.Remove` 删除,下次 `Put` 写盘清白。

## 已知踩坑

- **MinTaskMs 哨兵 bug 历史**: `Aggregator.OnTaskEnd` 不能用 `if s.MinTaskMs == 0 { ... }` 当初始化哨兵 —— 真实 0ms 任务会被覆盖。已用 `firstTask := s.TaskDurations.Count() == 0` 修复 (commit `4ada8eb`),不要回退。
- **负任务时长**: 某些 EventLog 的 `RunMs` 可能为负 (clock skew),`OnTaskEnd` 已 clamp 到 0,不要去掉。
- **V2 解码空 Parts**: `eventlog.Open` 拒绝 `Parts == nil` 的 V2 LogSource,`multiCloser.Close` 是幂等的 —— 见 commit `5a98097`。
- **Top-level App ID vs CLI App ID**: Envelope 顶层 `app_id` 来自 CLI 输入 + 文件名归一化;`data[].app_id` 来自 EventLog `SparkListenerApplicationStart` 事件。两者可能不同 (尤其当 fixture 与文件名不一致时),这是预期行为。
- **AppSummary 列契约**: `scenario.AppSummaryColumns()` 必须与 `AppSummaryRow` 的 JSON tag 完全一一对应,下游按 `columns` 解析 `data` 才不会丢字段。`internal/scenario/app_summary_test.go` 的 `TestAppSummaryColumnsMatchRowFields` 通过反射守门,新增 row 字段时**同步**更新 columns 列表,**不要**只改一边。
- **idle_stage 误报口径**: `IdleStageRule` 用 `MaxConcurrentExecutors` 估算有效 slot;若 EventLog 缺 `SparkListenerExecutorAdded` (例如局部日志),slot 估值会偏大导致误报偏低。阈值 `wall ≥ 30s`、`busy_ratio < 0.2` 是经验值,改阈值前先用真实日志回归。
- **V1/V2 attempt 后缀匹配**: Spark 实际写出的日志(无论 V1 单文件 `application_<id>_<attempt>` 还是 V2 滚动目录 `eventlog_v2_<appId>_<attempt>`)经常带 `_<attempt>` 计数器后缀,**不是**裸 appId。`resolveV1` / `resolveV2` 都用 `^<appID>(?:_(\d+))?$` 容忍可选 attempt,多 attempt 共存时取最大 (Spark History Server 行为)。V1 路径还保留一条短路:同时存在裸名 + attempt 命名时,裸名优先(对应 `spark.eventLog.rolling.enabled=false` 的旧行为)。**别**把任何一边改回精确等值 —— 真实 EventLog 几乎都带 attempt,改回去会全线 APP_NOT_FOUND。
- **dispatch 复杂度上限**: `internal/eventlog/decoder.go` 的事件分发被拆成 `dispatchAppLifecycle` / `dispatchSQLAndBlacklist` / `dispatchStageAndTask` 三组,每组独立 switch 返回 `(handled, err)`。原因:`gocyclo` lint 在单 switch 阈值 22,塞下全部事件后会触线。新增事件类型时按业务归类追加到对应分组;不要把所有 case 重新塞回一个大 switch。
- **SparkConf / SQLExecutions / Blacklists 模型**: `Application.SparkConf`(来自 `EnvironmentUpdate` 的 Spark Properties)、`SQLExecutions`(`SQLExecutionStart`)、`StageToSQL`(`JobStart.Properties[spark.sql.execution.id]`)、`Blacklists`(`Node/Executor{Blacklisted,Excluded}ForStage`)是规则给 LLM 输出"具体可执行建议"的依赖来源。改造规则时优先从这些字段取上下文:`disk_spill` / `gc_pressure` / `data_skew` 引用 SparkConf 当前值,`failed_tasks` 用 `Blacklists` 检测节点级故障(同一 host ≥2 次自动升级 critical)。`slow-stages` / `data-skew` 行通过 `StageToSQL` → `SQLExecutions` 关联 `sql_execution_id` + `sql_description`,非 SQL job 输出 `-1` + 空串。
- **缓存 schema 不 bump 的隐性 bug**: `internal/cache/envelope.go` 的 `currentSchemaVersion` 是手动维护的整型常量。改 `model.Application` / `Stage` / `Executor` / `BlacklistEvent` 等的字段**类型**或**重命名**字段时**必须** bump 一档,否则旧缓存会被当成有效,反序列化结果可能字段错位(gob 对类型不匹配会报错被当成 miss,但对兼容类型(如 int → int64)可能静默错位)。**只新增字段不需要 bump**。每次走完上面"添加新场景的标准步骤"之后,自检一下是否动了字段类型,动了就 bump。
- **缓存层 tmp 文件并发**: `Cache.Put` 用 `os.CreateTemp(dir, base+".tmp.*")` 拿到独占 tmp 文件再 `os.Rename`,**不要**改回 `<file>.tmp.<pid>` 的旧方案 —— 同进程多 goroutine 会撞同一个 tmp 名,出现 "rename: no such file or directory" 警告 spam。
- **shs 缓存命中仍要下载 zip**: `internal/cache` 命中只是跳过 `Open + Decode + Aggregate`,但 `Locator.Resolve` 仍要 `List + Stat`,这两步会触发 `internal/fs.SHS.bundleFor` 从 SHS 拉一次完整 zip 来判断 V1/V2 layout。不要把 SHS 的 zip 下载移到 cache 命中之后 —— Locator 在 cache 之前就需要确切的 LogSource。修这个意味着持久化 zip 到磁盘缓存,目前**不做**。

## 文档与计划

- `docs/superpowers/specs/2026-04-29-spark-cli-design.md` —— 设计 spec (8 章节)
- `docs/superpowers/plans/2026-04-29-spark-cli-mvp.md` —— 31 任务实施计划 (本仓库的施工蓝图)
- `docs/examples/diagnose-walkthrough.md` —— 给 agent 看的标准下钻流程
