# CLAUDE.md

Guidance for Claude Code (and other AI agents) working in this repository.

## What this is

`spark-cli` —— 单二进制 Go CLI,解析 Apache Spark EventLog,输出结构化 JSON 信封,面向 AI agent 的 Spark 性能诊断流程。Go 1.22,模块路径 `github.com/opay-bigdata/spark-cli`。

## 顶层契约

每条 EventLog 场景命令 (`app-summary` / `spark-conf` / `slow-stages` / `data-skew` / `gc-pressure` / `native-io` / `diagnose`) 和 live 诊断命令 (`yarn-logs` / `driver-thread-dump` / `paimon-diagnostics`) 在 stdout 输出**一个** `scenario.Envelope` JSON 对象;顶层必须带 `contract_version: 1`。工具类命令 (`config show` / `config cluster add|list` / `cache list|clear` / `self-update` / `version` / `--help`) 在 stdout 输出命令专用 JSON 对象;`completion` 已禁用。成功 stdout 不支持 table / markdown / text。stderr 也必须按行 JSON:错误格式 `{"error":{"code","message","hint"}}`,非错误进度/告警格式 `{"event":{"code","level","message","hint?","fields?"}}`;退出码 `0/1/2/3`。改动任何场景或输出层时**不要破坏这个信封形状** —— `tests/e2e/e2e_test.go` 是契约守门人。

特例:
- envelope 顶层 `contract_version` 当前固定为 `1`;新增不兼容 JSON 契约时先设计版本迁移,并同步 e2e / README / skills / CHANGELOG
- envelope 顶层 `app_duration_ms` 来自 `model.Application.DurationMs`(`SparkListenerApplicationEnd - Start`),`omitempty` 在没 ApplicationEnd 事件时缺失。所有 EventLog 场景一致输出 —— agent 看 `wall_share` / native IO 耗时时就能直接换算绝对秒数,不必再额外跑 `app-summary`
- `gc-pressure` 的 `data` 是数组 (与其他场景一致),非对象 —— 早期 spec 设想的双段已收敛为单段
- `diagnose` 的信封额外带 `summary: {critical, warn, ok, top_findings_by_impact?, findings_wall_coverage?}`。`top_findings_by_impact` 是按 `wall_share` 倒序的 `[{rule_id, severity, wall_share}]` 摘要,只收录有 `stage_id` 关联且 `wall_share > 0` 的 finding;**`wall_share` 取 finding 命中所有 stage(primary + similar_stages)的 max**(用 max 而非 sum,避免并行 stage 时 > 1.0 让人迷惑;全局总覆盖留给 findings_wall_coverage)。`findings_wall_coverage` 是这些 wall_share 按 stage 去重后加和(同一 stage 多个 finding 取 max),**cap 到 1.0**(stage 在 wall 上并行时 naive sum 可能 > 1,语义上不应超 100%);**< 0.05 表示瓶颈不在 finding 范围内**(常见为作业结构碎片化 / driver-side 等待),agent 应当跳到 `app-summary.top_busy_stages` / `top_io_bound_stages`。两个字段在 `app.DurationMs == 0`(没 ApplicationEnd 事件)时一并 omitempty 缺失
- **`severity` 是诊断置信度,不是 ROI 优先级**:`disk_spill warn (wall_share 0.5)` 实际比 `data_skew critical (wall_share 0.05)` 更值得修;按 severity 字符串排优先级会错位,永远以 `top_findings_by_impact` 为准。文档此处说明给 LLM agent 消费。
- **`data_skew` finding 在多 stage 命中时**,evidence 含 `similar_stages: [{stage_id, wall_share, skew_factor}]`(按 wall_share 倒序最多 4 条)。primary `stage_id` 选 wall_share 最大的(平局回退 skew_factor),`top_findings_by_impact` 与 `findings_wall_coverage` 都会跨 primary + similar_stages 聚合 —— agent 直接读 evidence 就能看到完整多 stage 列表,不必再回头跑 `data-skew`。
- `slow-stages` / `data-skew` 信封顶层带 `sql_executions: map[int64]string`,key 是 `sql_execution_id`、value 是 description。**默认按 `--sql-detail=truncate` 截到前 500 个 rune,过长追加 `...(truncated, total <N> chars)`**;`--sql-detail=full` 还原原始 SQL,`--sql-detail=none` 整段 omit(也可 `SPARK_CLI_SQL_DETAIL` / yaml `sql.detail` 覆盖)。对 DataFrame 作业自动回退到 details 首行;若 description / details 都是 callsite 占位形态则该条整行过滤。row 内只保 `sql_execution_id`,**不再带** `sql_description` 字段 —— 切走重复嵌入,把 JSON 体积从几十 KB 压到几 KB。所有条目都是 callsite 噪音时整段 map 走 `omitempty` 缺失;app-summary / gc-pressure / diagnose 不带这个字段
- `native-io` 解析 Paimon `SparkListenerNativeIOEvent`,同时支持顶层 `native_io_*` 字段和旧版内嵌 `eventJson`。`summary` 给 event/operation 数、reader/export/error 数、total rows/bytes/duration、`top_phases`、`top_operations`;`data` 按 `duration_ms` 倒序输出 `--top` 条 native IO 事件,包含 Spark 上下文、文件/object 字段、吞吐、memory、原始数值 `metrics` 和 `verdict`。
- `diagnose --guided` 是诊断 SOP 入口:先做命名集群预检,只有一个 cluster 时自动选择,多个 cluster 未选择时返回 `FLAG_INVALID`,没有 cluster/log_dirs 时返回带 `config cluster add ... --activate` 的 `CONFIG_MISSING` hint。预检说明以 `{"event":...}` JSON 行写 stderr,stdout 仍然是原 `diagnose` 的单一 JSON envelope。
- `app-summary` 的 `data` row 同时带 **三个** stage 切面,**永远要三个一起看**才不漏瓶颈:`top_stages_by_duration[]`(按 wall 倒序,包含 driver-side 等待 stage),`top_busy_stages[]`(过滤 `busy_ratio > 0.8` 后按 `busy_ratio * duration` 倒序,真正 executor 吃 CPU 的热点),**`top_io_bound_stages[]`**(`busy_ratio < 0.8` 但 `spill_disk_gb >= 0.5` 或 `shuffle_read_gb >= 1.0` 的 stage,按 wall_share 倒序 limit 3)。`top_io_bound_stages` 是 `top_busy_stages` 的互补切面 —— spill 主导的 stage executor 都在等盘,busy_ratio 被压得很低,`top_busy_stages` 阈值看不到但才是真瓶颈(实测 stage spill 9.86 GB / busy_ratio 0.048,只看 top_busy_stages 完全错过)。
- `slow-stages` row 三档 `*_mb_per_task`(`input_mb_per_task` / `shuffle_read_mb_per_task` / `shuffle_write_mb_per_task`)。读侧 / 写侧 / source-scan stage 分别看对应字段判 partition 粒度,`NumTasks=0` 时三个一律 0

## 仓库布局

| 路径 | 用途 |
|---|---|
| `cmd/` | cobra 根命令 + version + config 子命令 |
| `cmd/scenarios/` | EventLog 场景命令 + live 诊断命令 + `runner.Run()` 主管线 + `dispatch.go` 分发 |
| `internal/config/` | YAML 配置加载、环境变量、flag 覆盖 |
| `internal/errors/` | `cerrors.Error` 结构 + 退出码映射 |
| `internal/fs/` | `file://` / `hdfs://` 抽象 (FS 接口) |
| `internal/eventlog/` | 日志定位 (V1/V2)、解压、JSONL 解码、事件分发 |
| `internal/model/` | Application/Stage/Executor/Job 聚合模型 + Aggregator |
| `internal/stats/` | t-digest 分位数封装 |
| `internal/scenario/` | 场景纯函数 + Envelope 类型 |
| `internal/rules/` | Rule 接口 + 6 条规则 (skew/gc/spill/failed/tiny/idle_stage) |
| `internal/output/` | JSON formatter |
| `tests/e2e/` | 通过 `cmd.RunWith` 跑全场景 + 错误路径 |
| `tests/testdata/tiny_app.json` | 10 行合成 EventLog,所有 E2E 共用 |
| `.agents/skills/spark/SKILL.md` / `.claude/skills/spark/SKILL.md` | 内置 agent skills — 教 agent 「先 diagnose 再下钻」 |

## 关键数据流

```
appId (CLI) → config → fs.FS map → eventlog.Locator.Resolve → LogSource
LogSource → eventlog.Open (decompress + V2 拼接) → eventlog.Decode → model.Aggregator → *model.Application
*model.Application → scenario.<X>(app) → Envelope.{Columns, Data, Summary, SQLExecutions}
Envelope → output.WriteJSON
```

`runner.Run` 是唯一入口 (`cmd/scenarios/runner.go`);所有 cobra 命令通过 `register.go` 收敛到它。

## 配置与命名集群

生产环境里 Spark History Server 与 YARN gateway 必须按同一个物理集群成组配置,不要让用户分别手填 `--log-dirs shs://...` 和 `--yarn-base-urls http://...` 后意外串到不同集群。推荐配置形态:

```yaml
active_cluster: prod
clusters:
  prod:
    log_dirs:
      - shs://history.example.com:18081
    yarn:
      base_urls:
        - http://203.123.81.20:7765/gateway/hadoop-prod/yarn
    shs:
      timeout: 5m
    tls:
      insecure_skip_verify: false
```

HTTPS gateway 的 SHS 要写 `shs+https://host[:port][/path]`;若内网证书是自签或本机 CA 不信任,必须显式配 `tls.insecure_skip_verify: true`(也可用 `SPARK_CLI_TLS_INSECURE_SKIP_VERIFY=true` / `--tls-insecure-skip-verify`),该开关同时作用于 SHS 与 YARN client。`config.Load()` 会先应用 `active_cluster`;运行时 `--cluster <name>` 在 env 之后、显式 `--log-dirs` / `--yarn-base-urls` 等 flag 之前应用。也就是说:默认用 `active_cluster`,临时切集群用 `--cluster prod`,而逐次调试仍可用具体 flag 最终覆盖。`spark-cli config cluster add <name> --log-dirs ... --yarn-base-urls ... [--shs-timeout 5m] [--tls-insecure-skip-verify] [--activate]` 负责把 profile 写入本地 `config.yaml`;`spark-cli config cluster list` 用于查看已沉淀的集群。

标准排障 SOP:

1. 先跑 `spark-cli config cluster list` 与 `spark-cli config show`,确认 `active_cluster` / `selected_cluster` / `log_dirs` / `yarn.base_urls`。
2. 没有集群时,先 `spark-cli config cluster add <name> --log-dirs ... --yarn-base-urls ... --activate` 录入;多个集群但未选时,必须让用户确认目标集群并用 `--cluster <name>`。
3. 再跑 `spark-cli diagnose <appId> --guided`。`--guided` 不改 stdout envelope,只在 stderr 输出 `{"event":...}` 预检事件。

## 开发约定

- **Go 1.22 锁定**: `go.mod` 显式 `go 1.22`,不要被工具链自动 bump (有依赖如 tdigest 想要更高版本,但本仓库限定 1.22)。
- **TDD**: 新场景/规则先写失败测试再写实现,见每个 `*_test.go`。
- **回应中文**: 仓库内交互、commit 信息可中英混合,但解释/讨论默认中文。
- **Commit 信息格式**: `type(scope): subject` —— `feat(scenario):`、`feat(output):`、`fix(model):`、`test(e2e):`、`docs(skill):`、`ci:`、`style:` 等。每个 commit 末尾带 `Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>`(更早的 commit 可能是 Sonnet 4.6,新 commit 用当前模型即可)。
- **main 分支直改规则**: 默认仍按任务需要保持工作区清晰;但用户明确要求"直接在 main 上改 / 改完 push 到 main"时,可以在 `main` 分支完成修改、验证、提交并 push 到 `origin/main`,不要强制创建 feature branch。
- **Envelope 改动需同步**: 改 `scenario.Envelope` JSON tag 或字段时,务必跑 `tests/e2e` + 更新 `.agents/skills/spark/SKILL.md` / `.claude/skills/spark/SKILL.md` 的 envelope 文档 + `README.md` / `CHANGELOG.md`。
- **CLAUDE.md 同步规则**: 每次新增/修改用户可见功能(CLI flag、环境变量、配置项、命令行为、输出契约、依赖发现路径)或调整开发流程时,**必须**同步更新本文件相关章节(常见入口:开发约定、HDFS 连接、添加新场景/规则、已知踩坑),并在同一 commit 中带上 README/CHANGELOG 的对应变更。原因:本仓库的 AI agent 工作流强依赖 CLAUDE.md 作为唯一权威上下文,文档漂移会让后续会话直接做错事。
- **首页 / 博客同步规则**: 对较大的用户可见需求或完整诊断方案,交付时必须同步评估项目首页入口和博客/文章。博客至少覆盖场景、痛点、方案、命令示例、JSON 输出契约、AI agent 消费方式、排障路径和限制。若没有独立官网,以 `README.md` / `README.zh.md` 作为首页入口,必要时新增 `docs/` 示例文档并在 CHANGELOG 中标注。只改 CLI 不补首页/博客说明,视为交付不完整。

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
go run . app-summary application_1_1 --log-dirs file:///tmp/spark-cli-smoke
```

## 添加新场景的标准步骤

1. `internal/scenario/<name>.go` + `<name>_test.go` —— 纯函数,输入 `*model.Application`,输出 row 列表 + `Columns()` 函数
2. 若需新模型字段 —— 同步扩 `internal/model/{model.go,aggregator.go}` + 加单元测试 (`OnTaskEnd` 累计逻辑别打破现有)
3. `cmd/scenarios/dispatch.go` 加 `case` 分支,把 row 转 `[]any` 装进 `env.Data`
4. `cmd/scenarios/register.go` 加一行 `newScenarioCmd("<name>", "<short>")`
5. `tests/e2e/e2e_test.go` 加一个 case
6. `.agents/skills/spark/SKILL.md` / `.claude/skills/spark/SKILL.md` 与 `README.md` 同步说明
7. `CHANGELOG.md` / `CHANGELOG.zh.md` 写一条
8. 大需求或完整方案同步补首页入口和博客/文章说明，明确 agent 如何使用该命令完成诊断
9. 若 step 2 改了 `model` 现有字段的**类型**或**名称**,bump `internal/cache/envelope.go` 的 `currentSchemaVersion` —— 仅新增字段则不需要

## 添加新诊断规则

1. `internal/rules/<x>_rule.go` 实现 `Rule` 接口 (`ID()`、`Title()`、`Eval(*model.Application) Finding`)
2. `internal/rules/rule.go` 的 `All()` 列表追加
3. `internal/rules/rule_test.go` 加触发 + 静默测试
4. `tests/e2e` 自动覆盖 (因为 `diagnose` 跑全部规则)

## 发版

`.github/workflows/release.yml` 同时响应两类事件，由 `release` concurrency group 串行化：

1. **`push` 到 `main`**：runner 自动从 `git tag -l 'v*'` 找出最大 tag，bump patch 段（`v0.1.0` → `v0.1.1`），打 annotated tag 推回 origin —— 这一步会再次触发本 workflow 进入下面的 tag 分支。
2. **`v*` tag 推送**：跑 `goreleaser release --clean`，出 4 平台 tarball（linux/darwin × amd64/arm64），打包 `LICENSE`、`README.md`、`README.zh.md`、`CHANGELOG.md`、`CHANGELOG.zh.md`、`.agents/skills/spark/SKILL.md`、`.claude/skills/spark/SKILL.md`，并附 `checksums.txt`。

> 想跳过自动 bump、自己控制版本号？直接 `git tag -a vX.Y.Z -m vX.Y.Z && git push origin vX.Y.Z`，main 分支别动即可。

`scripts/install.sh` 通过 GitHub `releases/latest` redirect 解析最新 tag（API 限流时回落到 `/repos/.../releases/latest`），下载归档后用 `checksums.txt` 做 sha256 校验，再把 binary 装到 `PREFIX` (默认 `/usr/local/bin`，必要时 `sudo`)、把 skill 树镜像到 `CLAUDE_SKILL_DIR`(默认 `~/.claude/skills/spark`)与 `AGENTS_SKILL_DIR`(默认 `~/.agents/skills/spark`)。`SKILL_DIR` 是兼容旧用法的 Claude skill 目录别名。环境变量：`VERSION` / `PREFIX` / `CLAUDE_SKILL_DIR` / `AGENTS_SKILL_DIR` / `SKILL_DIR` / `NO_SUDO` / `NO_SKILL` / `REPO`，详见脚本头部注释。

`spark-cli self-update` 是已安装用户的二进制自更新入口(alias:`update` / `upgrade`)。实现位于 `internal/selfupdate`,逻辑是解析 latest release → 下载 `spark-cli_<ver>_<os>_<arch>.tar.gz` + `checksums.txt` → SHA256 校验 → 解压 `spark-cli` → 用同目录临时文件 + `os.Rename` 替换当前 executable。它**只更新 binary**,不镜像 `.agents/skills/spark` / `.claude/skills/spark`;需要同步 skill 时仍用 `scripts/install.sh`。受保护目录(`/usr/local/bin`)无写权限时返回带 `sudo` / `--install-dir` 指引的错误。改这里时保留 `--dry-run` 和 `--version` 单测,避免真实网络依赖进入单测。

## HDFS 连接

`internal/fs/hdfs.go` + `hdfs_conf.go` 走纯 Go 客户端 (`github.com/colinmarc/hdfs/v2`),**支持** Hadoop XML 配置和 HA NameService,**HDFS 不支持** Kerberos / SASL / TLS。

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

`internal/fs/shs.go` 把 Spark History Server 的 REST API (`GET /api/v1/applications/<id>/<attempt>/logs` 返回 zip) 包成第三种 `fs.FS` backend。用户在 `--log-dirs` 写 `shs://host:port`(HTTP)或 `shs+https://host:port[/gateway/path]`(HTTPS),Locator / decoder / 规则 / 缓存层都不需要改 —— SHS 把 zip 内文件暴露成同 scheme 的 `<base>/<appID>/<inner-zip-path>` 形式的合成 URI,FileInfo 的 `ModTime` 取自 attempt.lastUpdated(毫秒 → 纳秒),让 `cache.computeSourceKey` 的失效逻辑天然生效。

**HTTP/HTTPS 行为**:
- `shs://` 固定转 HTTP,`shs+https://` 固定转 HTTPS;仅支持匿名访问,**不支持** Basic Auth, Bearer Token, Kerberos
- `cfg.TLS.InsecureSkipVerify` 来自 yaml `tls.insecure_skip_verify`、env `SPARK_CLI_TLS_INSECURE_SKIP_VERIFY` 或 flag `--tls-insecure-skip-verify`,由 runner 注入 `SHSOptions.InsecureSkipVerify`;只在用户显式开启时跳过 HTTPS 证书校验。YARN client 也使用同一个配置。
- attempt 自动取 `attempts[]` 里数字 attemptId 最大那个(对齐 Spark History Server UI 默认行为)。如果 `attempts[]` 条目**完全没有** `attemptId` 字段(Spark 3.4+ 单 attempt 默认行为),logs URL 改用 `/api/v1/applications/<id>/logs`(不带 attempt 段)—— SHS 对 `/<id>//logs`(空段)直接 404,所以**必须**省略段而不是传空字符串
- HTTP timeout 由 `cfg.SHS.Timeout` 控制,优先级:`--shs-timeout` flag → `SPARK_CLI_SHS_TIMEOUT` 环境变量 → `config.yaml` `shs.timeout` → **默认 5 min**(生产 zip 几 GB 是常态,旧 60s 默认会让首次诊断撞墙;改回 60s 之前先看 CHANGELOG 那条 UX 修复)
- timeout 类错误(`url.Error.Timeout()` / `context.DeadlineExceeded` / 字符串兜底)由 `SHS.wrapTimeout` 升级成结构化 `cerrors.Error{Code: LOG_UNREADABLE, Hint: "increase --shs-timeout (current: ...) ..."}`;非 timeout 错误维持原 `fmt.Errorf` 包装。改造时新增的 net/http 调用点都要走 `wrapTimeout`,否则用户撞墙后看不到 hint
- zip body 落盘策略:`SHSOptions.CacheDir` 非空时**一律走磁盘**到 `<CacheDir>/shs/<host>/<appID>_<lastUpdated>.zip`(tmp + rename 原子写,attempt 更新时 sweep 同 prefix 旧文件);CacheDir 空(--no-cache)时小 zip(≤ 256 MiB 且 Content-Length 已知)走内存,大 zip 落 system temp 由 `SHS.Close()` 清理
- 同一 appID 的 zip **跨 CLI 调用复用**:首次 CLI 落盘到 cache,后续 CLI 实例 `bundleFor` 仅发 metadata JSON 调用拿 lastUpdated → 命中本地 zip 直接读盘(实测 warm 命令 < 1s)
- 首次为某 appID 走 `bundleFor` 时,会往 `s.stderr`(默认 `os.Stderr`)打两行 JSON event:`SHS_DOWNLOAD_START` / `SHS_DOWNLOAD_READY`。**这条提示放在 cache 之前**(因为缓存命中也要 zip 来判 V1/V2 layout),改造时**不要**把 `Locator.Resolve` 移到 cache 之后
- 静默控制由 `SHSOptions.Quiet` 决定,值由 `cmd/scenarios.resolveQuiet` 综合 `--no-progress` flag、`SPARK_CLI_QUIET`(`1/true` 静默 / `0/false` 不静默 / 未设走 stdout TTY 检测)算出 —— **agent 重定向 stdout 时默认静默,交互终端默认显示**;NewSHS 内部不再自己读 env

**关键约束**:
- `splitSHSURI` 把 `shs://host[:port][/appID[/inner...]]` 和 `shs+https://host[:port][/appID[/inner...]]` 解析成 `(host, appID, inner)`;新增的 URI 路径段都要走它,**不要**自己 sprintf 凑路径,否则 `appIDFromListPrefix` 的还原逻辑会和 List 的入参对不上。
- Locator 调 `List(base, prefix)` 时,我们从 prefix 反推 appID(剥 `eventlog_v2_` 前缀)——所以 prefix **必须**是 `application_<id>` 或 `eventlog_v2_application_<id>`;新增带 attempt 后缀的 prefix 时记得回头改 `appIDFromListPrefix`。
- **磁盘 zip 缓存绝不让 CLI 失败**:`openCachedZip` 损坏 → `os.Remove` + fallback 下载;落盘失败 → 结构化 `wrapTimeout`-style 报错;sweep 失败一律忽略。`Close()` 不删 cache 路径(由 sweep 管),只删 tmp 路径。改造时新增的 cache 操作都要遵守这条容错纪律。
- `NewSHS` 签名是 `(base, timeout, opts SHSOptions)`,**不再读环境变量**。新增 SHS 调用点要从 `cmd/scenarios.runner` 计算后通过 opts 注入,免得行为分散。SHS 单测显式传 `SHSOptions{Quiet: true}`,不依赖全局 env state。

## YARN 日志与 executor GC

`internal/yarn.Client` 同时服务 `diagnose` 顶层 `yarn` payload、`yarn-logs` 和 `driver-thread-dump`。YARN REST / HTML 在不同 gateway 后形态不稳定,这里的容错规则是用户可见契约:

- `appattempts` JSON 可能同时返回数字 `id: 2` 和完整 `appAttemptId: appattempt_..._000002`。请求 containers API 时必须优先用完整 `appAttemptId`;只有缺失时才由 `application_<ts>_<seq>` + 数字 id 拼成 `appattempt_<ts>_<seq>_<attempt:06d>`。直接拼 `/appattempts/2/containers` 会被 Hadoop 以 `Invalid AppAttemptId prefix` 返回 400。
- 某些 gateway 对 `/ws/v1/cluster/apps/<app>/appattempts/<attempt>/containers` 返回 400 或 `{}`。`yarn-logs` 必须回退到 appAttempt metadata(`containerId` / `logsLink`)和 YARN HTML (`/cluster/app/<app>`、`/cluster/appattempt/<attempt>`)里的 `containerlogs` / `jobhistory/logs` 链接。不要因为 containers API 失败就让整个命令失败;把 REST 失败写入 `warnings`,但尽量返回能拿到的 AM/container log URL。
- `--yarn-log-types` 控制 `yarn-logs` 抓取的文件,默认 `stderr,stdout,syslog`;`gc` 是别名,展开成 `gc.log.0.current,gc.log.1.current,gc.log`。YARN / JobHistory 日志页常返回 HTML 包 `<pre>`,抓日志时要抽出 `<pre>` 内容后再按 `--yarn-log-bytes` 截断,不要把 HTML 头当日志正文返回给 agent。
- `--executor-id` 对 `driver-thread-dump` 仍默认 driver;对 `yarn-logs` 表示按 Spark executor id 过滤。实现优先走 Spark UI `/api/v1/applications/<app>/executors` 的 `executorLogs` 映射,并把输出写到 `containers[].spark_executor_id`。如果这个 API 不可用,保留 warning 并退回普通 attempt/container 列表。
- `paimon-diagnostics` 是 live Spark UI 命令,不读 EventLog。它先通过 YARN app `trackingUrl` / gateway `/proxy/<appId>` 定位 Spark UI,再读取 `/paimon-diagnostics/json`、`/paimon-diagnostics/threadDump/json?executorId=<id>`、`/paimon-diagnostics/profiler/json`。任一 Paimon JSON 端点不可用时写入 `warnings` 而不是让整个命令失败;这样 agent 能区分"插件没装/页面没挂"和"应用不可达"。
- `containers[].log_findings` 是从日志片段里提取的轻量诊断信号。目前 `full_gc` 来自 `Full GC` / `Pause Full` 等 GC 日志证据,用于补 EventLog 覆盖不到的场景:第一次 attempt 没 EventLog、driver 只看到 heartbeat timeout,但 executor `gc.log.*` 显示 Full GC 卡死。新增日志规则时保持 evidence 短小,不要把整段日志复制进 JSON。

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
- **列契约(四处反射守门)**: `scenario.AppSummaryColumns()` / `SlowStagesColumns()` / `DataSkewColumns()` / `GCPressureColumns()` 必须与对应 row 类型的 JSON tag 完全一一对应,下游按 `columns` 解析 `data` 才不会丢字段。`internal/scenario/{app_summary,slow_stages,data_skew,gc_pressure}_test.go` 各有一个 `Test*ColumnsMatchRowFields` 反射测试守门,新增 row 字段时**同步**更新 columns 列表,**不要**只改一边。
- **idle_stage 误报口径**: `IdleStageRule` 用 `MaxConcurrentExecutors` 估算有效 slot;若 EventLog 缺 `SparkListenerExecutorAdded` (例如局部日志),slot 估值会偏大导致误报偏低。阈值 `wall ≥ 30s`、`busy_ratio < 0.2` 是经验值,改阈值前先用真实日志回归。
- **V1/V2 attempt 后缀匹配**: Spark 实际写出的日志(无论 V1 单文件 `application_<id>_<attempt>` 还是 V2 滚动目录 `eventlog_v2_<appId>_<attempt>`)经常带 `_<attempt>` 计数器后缀,**不是**裸 appId。`resolveV1` / `resolveV2` 都用 `^<appID>(?:_(\d+))?$` 容忍可选 attempt,多 attempt 共存时取最大 (Spark History Server 行为)。V1 路径还保留一条短路:同时存在裸名 + attempt 命名时,裸名优先(对应 `spark.eventLog.rolling.enabled=false` 的旧行为)。**别**把任何一边改回精确等值 —— 真实 EventLog 几乎都带 attempt,改回去会全线 APP_NOT_FOUND。
- **dispatch 复杂度上限**: `internal/eventlog/decoder.go` 的事件分发被拆成 `dispatchAppLifecycle` / `dispatchSQLAndBlacklist` / `dispatchStageAndTask` 三组,每组独立 switch 返回 `(handled, err)`。原因:`gocyclo` lint 在单 switch 阈值 22,塞下全部事件后会触线。新增事件类型时按业务归类追加到对应分组;不要把所有 case 重新塞回一个大 switch。
- **SparkConf / SQLExecutions / Blacklists 模型**: `Application.SparkConf`(来自 `EnvironmentUpdate` 的 Spark Properties)、`SQLExecutions`(`SQLExecutionStart`)、`StageToSQL`(`JobStart.Properties[spark.sql.execution.id]`)、`Blacklists`(`Node/Executor{Blacklisted,Excluded}ForStage`)是规则给 LLM 输出"具体可执行建议"的依赖来源。改造规则时优先从这些字段取上下文:`disk_spill` / `gc_pressure` / `data_skew` 引用 SparkConf 当前值,`failed_tasks` 用 `Blacklists` 检测节点级故障(同一 host ≥2 次自动升级 critical)。`slow-stages` / `data-skew` row 仅保 `sql_execution_id`(非 SQL job 为 `-1`),description 文本由 `cmd/scenarios/dispatch.go` 调 `scenario.BuildSQLExecutionMap(app)` 写到 envelope 顶层 `sql_executions: map[int64]string`(`omitempty`,无关联时整段缺失)。回退逻辑(callsite description → details 首行)集中在 `internal/scenario/result.go` 的 `stageSQL`,`BuildSQLExecutionMap` 复用同一 helper 去重。
- **缓存 schema 不 bump 的隐性 bug**: `internal/cache/envelope.go` 的 `currentSchemaVersion` 是手动维护的整型常量。改 `model.Application` / `Stage` / `Executor` / `BlacklistEvent` 等的字段**类型**或**重命名**字段时**必须** bump 一档,否则旧缓存会被当成有效,反序列化结果可能字段错位(gob 对类型不匹配会报错被当成 miss,但对兼容类型(如 int → int64)可能静默错位)。**只新增字段不需要 bump**。每次走完上面"添加新场景的标准步骤"之后,自检一下是否动了字段类型,动了就 bump。
- **缓存层 tmp 文件并发**: `Cache.Put` 用 `os.CreateTemp(dir, base+".tmp.*")` 拿到独占 tmp 文件再 `os.Rename`,**不要**改回 `<file>.tmp.<pid>` 的旧方案 —— 同进程多 goroutine 会撞同一个 tmp 名,出现 "rename: no such file or directory" 警告 spam。
- **SHS zip 持久化磁盘缓存**: 历史上 Locator.Resolve 在 application cache 之前,每次 CLI 调用都得重新下 zip(几 GB,4-7 秒)。现在 `SHS.bundleFor` 在 `fetchAttempt` 拿到 lastUpdated 后,优先尝试 `<cache_dir>/shs/<host_safe>/<appID>_<lastUpdated>.zip`(host 中 `:` 替换 `_`),命中即直接 zip-parse 跳过下载;未命中则下载并 `tmp+rename` 落盘 + sweep 同 appID 旧 attempt。`--no-cache` 旁路(走 system temp,Close 时清理)。**写盘 / 读盘 / sweep 任一失败一律退化为重新下载**,缓存层永远不让 CLI 失败。改 `bundleFor` 时千万不要破坏这条容错纪律,也不要把 zip 下载移到 application cache 命中之后(Locator 仍需要 zip 判 V1/V2 layout)。
- **sql_description 对 DataFrame 作业默认是 callsite**: `SparkListenerSQLExecutionStart.description` 在 DataFrame API 提交时通常是两种 callsite 形态:`getCallSite at SQLExecution.scala:74`(SparkSQL 早期默认)或 `org.apache.spark.SparkContext.getCallSite(SparkContext.scala:2205)`(Spark 现代版本 SparkContext 自反射)。`stageSQL` 在 `internal/scenario/result.go` 已实现 fallback:description 命中 `isCallSiteDescription` 或为空时,改取 `SQLExecution.Details` 的首行(通常是用户实际调用位置)。**重要**:`BuildSQLExecutionMap` 在 fallback **之后**还会再跑一次 `isCallSiteDescription` 过滤 —— 当 description 与 details **都**是 callsite 占位(真实生产 ETL 经常如此)时,该条目从 envelope `sql_executions` 中剔除;所有条目都被剔除时整段 map 走 `omitempty` 缺失。**不要**只判 description / 不做最终 noise 过滤,否则一次 ETL 可能给信封塞 80+ 行重复 callsite。判定窗口包含两种前缀 + `SparkContext.getCallSite(` 包含子串,不做更激进的模糊匹配以免误判真 SQL。
- **SQL description 默认截断**: `BuildSQLExecutionMap` 第二参数 `detail` 取 `truncate`(默认) / `full` / `none`,由 `cmd/scenarios.Options.SQLDetail` 注入(`--sql-detail` flag → `SPARK_CLI_SQL_DETAIL` → yaml `sql.detail`,空 / 非法值 normalize 落到 truncate)。truncate 用 `truncateSQL` 按 **rune** 切前 500 个字符,过长追加 `...(truncated, total <N> chars)` 让 agent 知道被截断。**用 rune 而非 byte 是关键** —— 真实生产 ETL 里 SQL 含中文字段名,按 byte 切会破坏 UTF-8 多字节字符。`none` 模式直接返回 nil 让整段 sql_executions 走 omitempty。改阈值 `sqlTruncateRunes` 前先看 e2e 是否有依赖完整 SQL 的断言。
- **SkewRule 选 primary stage 用 wall_share**: `SkewRule.Eval` 用 wall_share 倒序选 primary,平局回退 skew_factor;wall_share 全 0 时(`app.DurationMs == 0`)按 skew_factor 排。**这是 2026-05-02 的修复** —— 历史实现按 skew_factor 排,导致 wall_share 92% 的 stage 14 被 wall_share 26% 但 ratio 极端的 stage 7 盖过。其他过 4 道闸门的候选按 wall_share 倒序进 evidence.`similar_stages: [{stage_id, wall_share, skew_factor}]`(只收 wall_share > 0 的,最多 4 条);`diagnose.collectTopFindingsByImpact` 与 `computeFindingsWallCoverage` 都跨 primary + similar_stages 聚合 —— 改这两段时**必须**用 `evidenceStageIDs` helper,不要直接读 `evidence["stage_id"]`。
- **SkewRule isIdleStage 的 NumTasks 守门**: SkewRule 共享的 `isIdleStage` 在 `wall ≥ 30s + busy_ratio < 0.2` 之外多了一道 **NumTasks > 2*MaxConcurrentExecutors → 不算 idle** —— 任务多但因 IO/spill/shuffle 阻塞导致 busy_ratio 低的 stage(实测 stage 14:1000 task / 6 executor / busy_ratio 0.07)不再被误当 driver-side idle 排除。CLAUDE.md 历史早就写了"两条规则可独立演进";IdleStageRule 自己的判定**没**加这道守门(driver-side stage 通常 NumTasks 很少,直接命中原阈值即可)。改 SkewRule 的 isIdleStage 不要顺手抽 helper 共用 IdleStageRule。
- **findings_wall_coverage cap 1.0**: stage 在 wall 上并行,几个大 stage(skew + spill 同时命中)的 wall_share max-per-stage sum 可能 > 1.0(实测 ~4.3)。`computeFindingsWallCoverage` cap 到 1.0;1.0 等价于"几乎全部 wall 都在 finding 触及范围内"。改算法时不要把 cap 去掉。
- **app-summary 的 top_io_bound_stages 切面**: `AppSummaryRow.TopIOBoundStages` 与 `TopBusyStages` 互补,过滤 `busy_ratio < 0.8` **且**(`spill_disk_gb >= 0.5` 或 `shuffle_read_gb >= 1.0`)的 stage,按 wall_share 倒序 limit 3。这是给"executor 都在等盘 / 等 shuffle"的 IO-bound stage 的专门切面 —— `top_busy_stages` 用 0.8 阈值会把 spill 9 GB / busy 0.05 的 stage 全过滤掉,agent 只看 top_busy_stages 会错过最大瓶颈。新加的字段也走反射测试 `TestAppSummaryColumnsMatchRowFields` 守门,`AppSummaryColumns()` 末尾必须同步追加 `"top_io_bound_stages"`。改阈值前注意它与 `top_busy_stages` 的 0.8 刚好互补、不会双面命中。
- **data_skew 四道闸门**: `SkewRule` / `DataSkew.skewVerdict` 共有四道降级闸门,顺序应用:
  1. **任务时长紧致闸门**:`p99/p50 < 1.5`(常量 `tightTaskTimeRatio` / `dataSkewTightTaskTimeRatio`)→ SkewRule 直接 `ok`,DataSkew row 直接 `mild`。任务时长本身就高度均匀就不存在真倾斜,无视 input_skew_factor 多大(常见伪影:某个 task min 接近 0 把 ratio 拉成几千)。**这条放在 SkewRule.Eval 的 evidence 构造之前**,所以紧致情况下 evidence/Suggestion 不会构造,符合 ok 语义
  2. **均匀输入闸门**:`input_skew_factor < 1.2` 且 `p99/p50 < 20` 时降级 → 数据均匀的长尾多半是抖动
  3. **wall_share 闸门**:`stage.duration / app.duration < 1%` 且 `p99/p50 < 20` 时降级(SkewRule → warn,DataSkew → mild)→ 短 stage 长尾即使指标很差也几乎没优化收益。`app.DurationMs <= 0`(没 ApplicationEnd 事件)时 wall_share 算 0,被视作"未知",**不**触发闸门
  4. **idle_stage 跳过**:候选 stage 命中 `IdleStageRule` 条件(wall ≥ 30s + busy_ratio < 0.2)时整个跳过,选下一个 hot stage
  
  极端 ratio (≥ 20) 跨越闸门 2/3/4(但**不**跨越闸门 1 —— 紧致时长根本就不是倾斜)。**改任一闸门阈值时**:`wall_share` helper 在 `internal/rules/rule.go`(`wallShare` + `wallShareNegligible` + `tightTaskTimeRatio`),DataSkew 镜像在 `internal/scenario/data_skew.go`(`dataSkewWallShare` + `dataSkewNegligibleWallShare` + `dataSkewTightTaskTimeRatio`),idle 闸门两边各保一份内联(`SkewRule.isIdleStage`)—— 故意不抽 helper 让两条规则可独立演进。SkewRule 的 evidence 在 wall_share > 0 时输出 `wall_share` 字段,DataSkew row 总是带 `wall_share` 字段(未知则为 0)。
- **三层 gc_ratio 口径**: `Envelope.gc_ratio`(`app-summary` 顶层)= `app.TotalGCMs / app.TotalRunMs`;`SlowStageRow.gc_ratio` = `stage.TotalGCMs / stage.TotalRunMs`;`GCExecRow.gc_ratio` = `executor.TotalGCMs / executor.TotalRunMs`。**三层分母都是 task 累加**,**不要**用 wall(`duration_ms`)做分母,否则多 executor 并发场景下会出现 >100% 的诡异值。SKILL.md 已显式列出三层口径表,改任何一层时记得同步。
- **app-summary top stages 的 busy_ratio + slow-stages busy_ratio**: `TopStage.BusyRatio` 与 `SlowStageRow.BusyRatio` 共用 `internal/scenario/app_summary.go:stageBusyRatio` —— `TotalRunMs / (wall * effective_slots)`,clamp 到 [0,1],`effective_slots = min(NumTasks, MaxConcurrentExecutors)`(与 `IdleStageRule` 同口径)。接近 0 的 stage 是 driver-side idle stage(broadcast/planning/listing 等),按 wall 排序会把它们推到前面但优化方向完全不同 —— agent / 用户读 `slow-stages` 或 `app-summary.top_stages` 的列表时**必须**先看 `busy_ratio` 再决定是否值得动 stage 本身。
- **app-summary 的 top_busy_stages 切面**: `AppSummaryRow.TopBusyStages` 与 `TopStagesByDuration` 并列输出,过滤 `busy_ratio >= topBusyStagesMinRatio`(0.8)后按 `busy_ratio * duration_ms` 倒序,limit `topBusyStagesLimit`(3)。这才是真正吃 CPU 值得调优的 executor 热点;driver-side 等待 stage 自动被过滤。`findings_wall_coverage` 低 + 用户报慢时,agent 应当从这个数组挑 stage 看执行计划。**改阈值前**注意 `IdleStageRule` 用 0.2 做 idle 判定 —— 0.8 是为留足缓冲,把"勉强算忙"的 stage 也滤掉,不要拉得太低让 driver-side stage 重新混进来。
- **slow-stages 三档 *_mb_per_task**: `SlowStageRow` 同时输出 `input_mb_per_task` / `shuffle_read_mb_per_task` / `shuffle_write_mb_per_task`,共用 helper `bytesPerTaskToMB(total, numTasks)`,`numTasks <= 0` 时三个一律 0。读侧 / 写侧 / source-scan stage 分别看对应字段判 partition 粒度 —— shuffle-write stage 上 read 字段恒为 0 不能当 "无信号",得交叉看 write 与 input。**改字段时**反射测试 `TestSlowStagesColumnsMatchRowFields` 守门,`SlowStagesColumns()` 列表必须同步。
- **diagnose summary.top_findings_by_impact + findings_wall_coverage**: `internal/scenario/diagnose.go` 的 `collectTopFindingsByImpact` 把所有非 ok finding 按 `wall_share` 倒序(复用 `dataSkewWallShare` 公式),只收录 evidence 含 `stage_id` 且 `wall_share > 0` 的 finding。**新增** `computeFindingsWallCoverage` 把所有这种 finding 关联的 stage wall_share 按 stage_id 去重(同一 stage 取 max,避免规则间互相计数)后加和写入 `summary.findings_wall_coverage`。`app.DurationMs == 0` 时两个字段一并 `omitempty` 缺失。**coverage < 0.05** 表示瓶颈不在 finding 范围内 —— 优先级展示与"该不该继续下钻 finding"这两件事都靠这两个字段,不要在 agent / 用户侧二次心算。

## 文档与计划

- `docs/superpowers/specs/2026-04-29-spark-cli-design.md` —— 设计 spec (8 章节)
- `docs/superpowers/plans/2026-04-29-spark-cli-mvp.md` —— 31 任务实施计划 (本仓库的施工蓝图)
- `docs/superpowers/specs/2026-05-02-application-cache-design.md` —— Application 缓存层设计
- `docs/superpowers/plans/2026-05-02-application-cache.md` —— Application 缓存实施计划
- `docs/superpowers/specs/2026-05-02-spark-cli-diagnostic-fixes-design.md` —— 诊断精度修复设计
- `docs/superpowers/plans/2026-05-02-spark-cli-diagnostic-fixes.md` —— 诊断精度修复实施计划
- `docs/examples/diagnose-walkthrough.md` —— 给 agent 看的标准下钻流程
