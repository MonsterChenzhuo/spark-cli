# spark-cli 设计文档

> **状态**：Draft（待用户复审）
> **日期**：2026-04-29
> **作者**：brainstorming session（Claude Code 协作）
> **目标读者**：实现者 + 后续维护者

---

## 0. 背景与目标

### 0.1 背景
- `Spark-Performance-Insight`（下称 SPI）是已有的 Java/Spring Boot 项目，把 Spark EventLog 解析为 Bronze→Silver→Gold（DuckDB），并通过 Web UI + MCP server 暴露分析能力。
- `hbase-metrics-cli` 是已有的 Go 单二进制，12 个 YAML 场景查询 VictoriaMetrics，专为 Claude Code 等 AI agent 设计，输出结构化 JSON。
- 我们希望把"agent-native CLI"模式套到 Spark 上，给出一个独立的 `spark-cli` 工具。

### 0.2 目标
打造一个 **面向 Claude Code（及其他 AI agent）** 的 Spark 性能诊断 CLI。
- 输入：用户给一个 `applicationId`。
- 行为：CLI 自动从配置的 log 目录（local + HDFS）定位对应 EventLog → 流式解析 → 跑场景算法或规则集 → 输出结构化 JSON。
- 用户体验：与 `hbase-metrics-cli` 风格一致——扁平命令、JSON envelope、stderr 结构化错误、退出码 0/1/2/3、自带 `.claude/skills/spark/SKILL.md`。

### 0.3 非目标（详见第 9 节）
不重写 SPI、不复制 SPI 的 SQL plan / DuckDB 持久化 / LLM 诊断 / Web UI。spark-cli 是**只读、无状态、单 app 单次扫描**的诊断工具。

---

## 1. 命令矩阵 & 输出契约

### 1.1 命令一览

| 命令 | 用途 |
|---|---|
| `spark-cli config init` | 交互式生成 `~/.config/spark-cli/config.yaml` |
| `spark-cli config show` | 打印当前生效配置（标注来源 flag/env/file/default） |
| `spark-cli app-summary <appId>` | 应用级总览：耗时、规模、shuffle/GC 总量、top-3 慢 stage |
| `spark-cli slow-stages <appId>` | 按 wall time 排序的 stage 列表（top N） |
| `spark-cli data-skew <appId>` | 每 stage 内 task 长尾因子（P99/median 比、max/median input bytes 比） |
| `spark-cli gc-pressure <appId>` | 每 executor / 每 stage 的 GC 占比 |
| `spark-cli diagnose <appId>` | **agent 首选入口**——跑全部规则，输出扁平 findings 列表 |
| `spark-cli version` | 打印版本（嵌入 git tag） |

### 1.2 全局 flag

| flag | 默认 | 说明 |
|---|---|---|
| `--log-dirs` | from config | 逗号分隔多 URI：`file:///tmp/spark-events,hdfs://nn:8020/spark-history` |
| `--hdfs-user` | `$USER` | HDFS 简单认证用户名 |
| `--timeout` | `30s` | 整体解析超时 |
| `--format` | `json` | `json` / `table` / `markdown` |
| `--top` | `10` | 仅对 slow-stages / data-skew / gc-pressure 生效 |
| `--dry-run` | `false` | 仅定位文件，输出文件元数据，不解析事件 |
| `--no-progress` | `false` | 关闭 stderr 进度（agent 模式应保持开） |

解析优先级（高到低）：`flag` > `env` > `~/.config/spark-cli/config.yaml` > 内置默认。

### 1.3 stdout JSON envelope

所有场景统一：

```json
{
  "scenario": "data-skew",
  "app_id": "application_1735000000_0001",
  "app_name": "etl_daily",
  "log_path": "hdfs://nn:8020/spark-history/application_1735000000_0001.zstd",
  "log_format": "v1",
  "compression": "zstd",
  "incomplete": false,
  "parsed_events": 482113,
  "elapsed_ms": 1842,
  "columns": ["stage_id", "name", "tasks", "p50_ms", "p99_ms", "skew_factor", "max_input_mb"],
  "data": [
    {"stage_id": 7, "name": "shuffle exchange", "tasks": 2000,
     "p50_ms": 410, "p99_ms": 9820, "skew_factor": 23.95, "max_input_mb": 612.4}
  ]
}
```

例外：
- `diagnose` 的 `data` 元素结构固定为 `{rule_id, severity, title, evidence, suggestion}`，并附加 `summary: {critical, warn, ok}`。
- `gc-pressure` 的 `data` 是对象 `{by_stage: [...], by_executor: [...]}`（双段，唯一对象形 envelope）；`columns` 字段提供两个子段各自的列名 `{by_stage: [...], by_executor: [...]}`。

`incomplete: true` 出现在 `.inprogress` 文件被读取时，提醒 agent 数据未必终态。

### 1.4 stderr 错误契约

```json
{"error": {"code": "APP_NOT_FOUND", "message": "...", "hint": "..."}}
```

错误码：`CONFIG_MISSING` / `APP_NOT_FOUND` / `APP_AMBIGUOUS` / `LOG_UNREADABLE` / `LOG_PARSE_FAILED` / `LOG_INCOMPLETE` / `HDFS_UNREACHABLE` / `FLAG_INVALID` / `INTERNAL`。

### 1.5 退出码

`0` 成功 · `1` 内部错误 · `2` 用户错误 · `3` 远端 / IO 失败。

`diagnose` 即使有 `critical` finding 也返回 `0`（成功跑完）；agent 通过 `summary.critical > 0` 判断。

---

## 2. 仓库目录结构

```
spark-cli/
├── main.go                                    # 入口，调用 cmd.Execute()
├── go.mod / go.sum
├── Makefile                                   # build/install/unit-test/e2e/lint/release-snapshot
├── README.md / README.zh.md
├── CHANGELOG.md / CHANGELOG.zh.md
├── LICENSE                                    # MIT
├── .goreleaser.yml                            # 多平台二进制 + tar.gz
├── .golangci.yml
├── .gitignore
│
├── cmd/                                       # cobra 命令树
│   ├── root.go
│   ├── version.go
│   ├── configcmd/
│   │   ├── init.go
│   │   └── show.go
│   └── scenarios/
│       ├── register.go                        # 统一注册
│       ├── runner.go                          # 通用 pipeline (定位→解析→场景→渲染)
│       ├── app_summary.go
│       ├── slow_stages.go
│       ├── data_skew.go
│       ├── gc_pressure.go
│       └── diagnose.go
│
├── internal/
│   ├── config/                                # 配置加载、env 合并、校验
│   ├── eventlog/
│   │   ├── locate.go                          # appId → URI 解析（local + hdfs）
│   │   ├── reader.go                          # io.Reader 抽象（zstd/lz4/snappy/plain/v2 dir）
│   │   ├── decoder.go                         # JSONL → SparkListenerEvent
│   │   └── events.go                          # 事件结构体（部分字段映射）
│   ├── model/
│   │   ├── application.go
│   │   ├── job.go / stage.go / task.go / executor.go
│   │   └── aggregator.go                      # 边读边累加，单遍扫描
│   ├── stats/                                 # percentile / t-digest / duration helpers
│   ├── scenario/                              # 算法层（不依赖 cobra）
│   │   ├── result.go
│   │   ├── app_summary.go
│   │   ├── slow_stages.go
│   │   ├── data_skew.go
│   │   ├── gc_pressure.go
│   │   └── diagnose.go
│   ├── rules/
│   │   ├── rule.go                            # interface { ID, Title, Eval(model) Finding }
│   │   ├── skew_rule.go
│   │   ├── gc_rule.go
│   │   ├── spill_rule.go
│   │   ├── failed_tasks_rule.go
│   │   └── tiny_tasks_rule.go
│   ├── fs/
│   │   ├── fs.go                              # interface { Open, Stat, List }
│   │   ├── local.go
│   │   └── hdfs.go                            # colinmarc/hdfs 封装
│   ├── output/
│   │   ├── json.go
│   │   ├── table.go
│   │   └── markdown.go
│   └── errors/                                # 结构化错误 + exit code 映射
│
├── tests/
│   ├── golden/                                # 每场景 stdout 黄金文件
│   ├── e2e/                                   # 全链路用 testdata 跑
│   └── testdata/
│       ├── small_app.json                     # 合成 EventLog（脱敏）
│       └── skew_app.zstd                      # zstd 样本
│
├── docs/
│   ├── examples/                              # 真实诊断 walk-through
│   └── superpowers/
│       ├── specs/                             # 本设计 spec 落这里
│       └── plans/                             # implementation plan
│
├── scripts/
│   └── install.sh                             # curl | bash：装 binary + 链 skill
│
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── release.yml
│
└── .claude/
    └── skills/
        └── spark/
            └── SKILL.md
```

**说明**：
- 算法层（`internal/scenario/`）与 cobra 严格分层，便于单测。
- `internal/fs.FS` 抽象使场景层完全无视底层 local / HDFS。
- 不保留顶层 `scenarios/` 目录（YAGNI——真要 YAML override 再加）。
- module path：`github.com/opay-bigdata/spark-cli`，Go 版本锁 1.22。

---

## 3. EventLog 解析与聚合模型

### 3.1 解析约束（MVP 边界）
- **只读完整文件**；`.inprogress` 也读但 envelope 标 `incomplete: true`。
- **单遍扫描，O(events)**；不缓存事件原文，仅累加聚合。
- **未知事件直接 skip**——`Event` 字段路由，未识别的不报错。
- **不解析 SQL plan / SQLExecution**；MVP 不用。
- **不解析 BlockManager / Storage**；MVP 不用。

### 3.2 关注事件（共 8 种）

| 事件 type | 用途 |
|---|---|
| `SparkListenerApplicationStart` | app_id, app_name, start_time, spark_user |
| `SparkListenerApplicationEnd` | end_time |
| `SparkListenerExecutorAdded` | executor_id, host, cores, total_cores, add_time |
| `SparkListenerExecutorRemoved` | executor_id, remove_time, reason |
| `SparkListenerJobStart` | job_id, stage_ids, submit_time |
| `SparkListenerJobEnd` | job_id, end_time, result(success/failure) |
| `SparkListenerStageSubmitted` | stage_id, attempt, name, num_tasks, parent_ids, submit_time |
| `SparkListenerStageCompleted` / `SparkListenerTaskEnd` | 真正的指标来源（见 3.3） |

明确不抓：
- `SparkListenerExecutorMetricsUpdate`——事件量大，task metrics 累加已足够 MVP
- `SparkListenerEnvironmentUpdate`——MVP 5 条规则均不依赖 Spark conf，暂不消费（条件型规则若来再开）

### 3.3 TaskEnd 抽取的关键指标

来自 `Task Metrics`：
- `executor_run_time` / `executor_cpu_time` / `executor_deserialize_time`
- `jvm_gc_time`
- `result_size` / `peak_execution_memory`
- `input_metrics.bytes_read` / `records_read`
- `output_metrics.bytes_written` / `records_written`
- `shuffle_read_metrics.{remote_bytes_read, local_bytes_read, fetch_wait_time, total_records_read}`
- `shuffle_write_metrics.{bytes_written, write_time, records_written}`
- `memory_bytes_spilled` / `disk_bytes_spilled`

来自 task info：
- `task_id, stage_id, stage_attempt_id, executor_id, host, launch_time, finish_time, failed, killed`

### 3.4 内存模型

```go
type Application struct {
    ID, Name, User string
    StartMs, EndMs int64
    DurationMs     int64

    Executors map[string]*Executor   // executor_id
    Jobs      map[int]*Job
    Stages    map[StageKey]*Stage    // {stage_id, attempt}
}

type Stage struct {
    ID, Attempt   int
    Name          string
    NumTasks      int
    SubmitMs, CompleteMs int64
    Status        string             // succeeded / failed / skipped

    // t-digest，不存每条 task
    TaskDurations    *tdigest.TDigest
    TaskInputBytes   *tdigest.TDigest
    TotalShuffleReadBytes  int64
    TotalShuffleWriteBytes int64
    TotalSpillDisk         int64
    TotalSpillMem          int64
    TotalGCMs              int64
    TotalRunMs             int64
    FailedTasks, KilledTasks int
    MaxTaskMs, MinTaskMs   int64
    MaxInputBytes          int64
}

type Executor struct {
    ID, Host string
    Cores    int
    AddMs, RemoveMs int64
    RemoveReason    string

    TotalRunMs, TotalGCMs int64
    TaskCount, FailedTaskCount int
}
```

### 3.5 percentile 实现

用 `github.com/influxdata/tdigest`。每个 stage 维护一个 TDigest（内存 ≈ 几 KB），覆盖 P50/P90/P99。误差 <1%，足够 data-skew 用 ratio 判断。

### 3.6 流式 pipeline

```
URI → fs.FS.Open → Reader（zstd/lz4/snappy/plain 解压）
    → bufio.Scanner（按行，buffer 16MB）
    → json.Unmarshal 到最小 Event 结构
    → 按 "Event" 字段路由到 aggregator.OnXxx()
    → 累加到 Application 模型
（结束）→ scenarios 拿 *Application 算结果
```

`bufio.Scanner` 必须显式 `scanner.Buffer(make([]byte, 1<<20), 16<<20)`；EnvironmentUpdate 单行可能 >64KB。

---

## 4. 文件定位 & 压缩格式

### 4.1 URI scheme（MVP 仅两种）

```yaml
log_dirs:
  - file:///tmp/spark-events
  - hdfs://nn-prod.example.com:8020/spark-history
hdfs:
  user: "${USER}"
```

`hdfs.namenodes` HA 配置 **MVP 不做**（单 NN URI）。

### 4.2 applicationId → 文件 / 目录解析

按 `log_dirs` 顺序扫，**首个命中即停**。

**Spark V1（单文件）** 候选后缀（按以下顺序尝试）：
```
<appId>
<appId>.lz4
<appId>.snappy
<appId>.zstd
<appId>.inprogress
<appId>.lz4.inprogress
<appId>.snappy.inprogress
<appId>.zstd.inprogress
```

**Spark V2（目录）**：`<log_dir>/eventlog_v2_<appId>/`。读取顺序按 `events_<n>_<appId>.<ext>` 数字升序拼接为单一逻辑流。`appstatus_<appId>` 标记 V2。

**applicationId 归一化**：
1. 去掉用户传入的已知扩展名 / `.inprogress`
2. 若不以 `application_` 或 `eventlog_v2_application_` 开头，自动补 `application_`
3. 用 `fs.List(prefix)` 找候选
4. 多于一个候选 → `APP_AMBIGUOUS`，列出所有候选；不自动选

### 4.3 压缩格式支持

| 格式 | 库 | MVP 状态 |
|---|---|---|
| 未压缩 | 标准库 | **一等公民** |
| `.zstd` | `klauspost/compress/zstd` | **一等公民**（标准 frame） |
| `.lz4` | `pierrec/lz4/v4` | **experimental**（Spark 用 hadoop block lz4，可能需补 framing reader） |
| `.snappy` | `golang/snappy` | **experimental**（同上） |

详见 §10 风险 R1。

### 4.4 抽象接口

```go
// internal/fs/fs.go
type FS interface {
    Open(uri string) (io.ReadCloser, error)
    Stat(uri string) (FileInfo, error)
    List(dirURI, prefix string) ([]string, error)
}

// internal/eventlog/locate.go
type LogSource struct {
    URI         string
    Format      string   // "v1" | "v2"
    Compression string   // "none" | "zstd" | "lz4" | "snappy"
    Incomplete  bool
    Parts       []string // V2 时多 part；V1 留空
    SizeBytes   int64
}

type Locator struct {
    fsByScheme map[string]fs.FS  // "file" -> local, "hdfs" -> hdfs
    logDirs    []string
}
func (l *Locator) Resolve(appId string) (LogSource, error)
```

### 4.5 dry-run 行为

`spark-cli <scenario> <appId> --dry-run` 走到 `Locator.Resolve` 出 `LogSource` 即停，envelope 中 `data` 是 `LogSource` 字段（path / format / compression / size_mb / incomplete），不解析事件。Agent 用它先确认能定位到文件再决定是否真跑。

---

## 5. 5 个场景的算法

约定：所有 stage 级聚合 **只用最后一次成功的 attempt**。失败 / 重试 attempt 单独记 `failed_attempts` 计数，不进 percentile。

### 5.1 `app-summary <appId>`

`data` 单行：
```json
{
  "app_id": "...", "app_name": "...", "user": "...",
  "duration_ms": 421000,
  "executors_added": 32, "executors_removed": 30, "max_concurrent_executors": 24,
  "jobs_total": 12, "jobs_failed": 0,
  "stages_total": 47, "stages_failed": 1, "stages_skipped": 3,
  "tasks_total": 152340, "tasks_failed": 18,
  "total_input_gb": 412.7, "total_output_gb": 38.2,
  "total_shuffle_read_gb": 921.3, "total_shuffle_write_gb": 921.3,
  "total_spill_disk_gb": 12.4,
  "total_gc_ms": 1820000, "total_run_ms": 28940000, "gc_ratio": 0.063,
  "top_stages_by_duration": [
    {"stage_id": 7, "name": "shuffle exchange", "duration_ms": 184000},
    {"stage_id": 12, "name": "...", "duration_ms": 92000},
    {"stage_id": 3, "name": "...", "duration_ms": 71000}
  ]
}
```
- `gc_ratio = total_gc_ms / total_run_ms`
- `top_stages_by_duration` 固定 top 3
- `max_concurrent_executors` 通过 ExecutorAdded/Removed 时间线扫描得出

### 5.2 `slow-stages <appId> --top N`

排序键：`stage.complete_ms - stage.submit_ms`（wall time，含调度等待）。

每行：
```json
{
  "stage_id": 7, "attempt": 0, "name": "shuffle exchange",
  "duration_ms": 184000,
  "tasks": 2000, "failed_tasks": 0,
  "p50_task_ms": 410, "p99_task_ms": 9820,
  "input_gb": 152.0, "shuffle_read_gb": 0, "shuffle_write_gb": 412.0,
  "spill_disk_gb": 8.1, "gc_ms": 92000
}
```
默认 `--top 10`。

### 5.3 `data-skew <appId> --top N`

每 succeeded stage 算两个倾斜信号：
- `skew_factor = p99_task_ms / max(median_task_ms, 1)`
- `input_skew_factor = max_input_bytes / max(median_input_bytes, 1)`

**过滤**：
- `tasks >= 50`（task 太少 percentile 不可信）
- `p99_task_ms >= 1000`（绝对时间太短的 stage 倾斜也无意义）

排序键：`max(skew_factor, input_skew_factor)` 降序。

每行：
```json
{
  "stage_id": 7, "name": "...", "tasks": 2000,
  "p50_task_ms": 410, "p99_task_ms": 9820, "skew_factor": 23.95,
  "median_input_mb": 25.6, "max_input_mb": 612.4, "input_skew_factor": 23.92,
  "verdict": "severe"
}
```
`verdict` 阈值：令 `f = max(skew_factor, input_skew_factor)`。`f >= 10` → `severe`；`f >= 4` → `warn`；else `mild`。

### 5.4 `gc-pressure <appId> --top N`

**唯一双段输出**——`data` 是对象 `{by_stage, by_executor}`：

`by_stage`：每 stage `gc_ratio = total_gc_ms / total_run_ms`，过滤 `total_run_ms >= 30000`，按 `gc_ratio` 降序：
```json
{"stage_id": 7, "name": "...", "gc_ratio": 0.34, "total_gc_ms": 92000, "total_run_ms": 270000}
```

`by_executor`：每 executor 同算，过滤 `total_run_ms >= 60000`：
```json
{"executor_id": "12", "host": "h-12", "gc_ratio": 0.41,
 "total_gc_ms": 380000, "total_run_ms": 920000, "tasks": 4500}
```

阈值（仅作为列字段 `verdict`）：`gc_ratio >= 0.20` → `severe`；`>= 0.10` → `warn`；else `ok`。

### 5.5 `diagnose <appId>`

跑全部规则，输出扁平 findings，是 **agent 的首选入口**。

#### MVP 规则集（5 条）

| rule_id | 触发条件 | severity | suggestion 模板 |
|---|---|---|---|
| `data_skew` | 任一 stage `skew_factor >= 4` | factor>=10 → critical, >=4 → warn | "stage X 任务长尾严重，median Yms / P99 Zms。检查 join key 分布或开启 AQE skew join。" |
| `gc_pressure` | 任一 executor `gc_ratio >= 0.10` | >=0.20 → critical, >=0.10 → warn | "executor X GC 占比 Y%，考虑增大 executor 内存或减小分区。" |
| `disk_spill` | 任一 stage `spill_disk_gb >= 1` | >=10GB → critical, >=1GB → warn | "stage X 磁盘溢写 Y GB，考虑提高 spark.sql.shuffle.partitions 或增大 executor 内存。" |
| `failed_tasks` | `tasks_failed / tasks_total >= 0.005` | >=5% → critical, >=0.5% → warn | "应用有 X% 任务失败 (Y/Z)，检查 driver 日志中的 task failure reason。" |
| `tiny_tasks` | 任一 stage `tasks >= 200 且 p50_task_ms < 100` | always warn | "stage X 有 Y 个任务但 P50 仅 Zms，分区过细，考虑 coalesce/repartition。" |

每条规则**未触发也输出一行** `severity: "ok"`，给 agent "已检查" 留痕。

#### envelope

```json
{
  "scenario": "diagnose",
  "app_id": "...", "log_path": "...",
  "data": [
    {
      "rule_id": "data_skew", "severity": "critical", "title": "Data skew detected",
      "evidence": {"stage_id": 7, "skew_factor": 23.95, "p50_task_ms": 410, "p99_task_ms": 9820},
      "suggestion": "stage 7 任务长尾严重..."
    },
    {"rule_id": "gc_pressure", "severity": "ok", "title": "GC pressure",
     "evidence": null, "suggestion": null}
  ],
  "summary": {"critical": 1, "warn": 1, "ok": 3}
}
```

退出码恒为 0。

### 5.6 通用约定

- 时长字段统一 `_ms`，字节统一 `_gb` 或 `_mb`（在字段名里明示单位）
- `verdict` / `severity` 离散三档：`ok` / `warn` / `critical`（不数值化）
- 每行输出**首字段**带可下钻 ID（`stage_id` / `executor_id` / `rule_id`），方便 agent 拼下一条命令

---

## 6. CI / 发布 / install / Skill

### 6.1 `.github/workflows/ci.yml`

三个 job：
- `lint`：`go vet` + `gofmt -l` 检查无 diff + `golangci-lint v2.1.6`
- `unit-test`：`go test -race -count=1 ./...`
- `e2e`：`make e2e`，用 `tests/testdata/*.zstd` 跑全部场景，stdout 与 `tests/golden/*.json` diff

触发：push to main + PR。Go 锁 1.22。matrix 仅 linux/amd64。

### 6.2 `.github/workflows/release.yml`

tag `v*` 触发 → goreleaser → 5 平台二进制：darwin-amd64 / darwin-arm64 / linux-amd64 / linux-arm64 / windows-amd64。产物：`spark-cli_<version>_<os>_<arch>.tar.gz` + `checksums.txt`。同时把 `.claude/skills/spark/` 打成 `spark-claude-skill.tar.gz` 单独 attach。

### 6.3 `.goreleaser.yml` 关键

- `builds.ldflags`: `-s -w -X github.com/opay-bigdata/spark-cli/cmd.version={{.Version}}`
- `archives.format: tar.gz`，windows 用 zip
- `archives.files`: 含 `README.md` / `LICENSE` / `.claude/skills/spark/SKILL.md`

### 6.4 `scripts/install.sh`

支持 envs：`VERSION` / `PREFIX` / `SKILL_DIR` / `NO_SUDO` / `NO_SKILL` / `REPO`。行为：
1. 探测 OS/arch
2. `curl` 拉 tar.gz，校验 sha256
3. 解压 binary 到 `${PREFIX:-/usr/local/bin}`
4. 解压 SKILL.md 到 `${SKILL_DIR:-$HOME/.claude/skills/spark}`（除非 `NO_SKILL=1`）
5. 提示运行 `spark-cli config init`

复制（非符号链接）—— 与 hbase-metrics-cli 一致。

### 6.5 `Makefile` 目标

```
build / install / unit-test / e2e / test / lint / fmt / tidy / clean / release-snapshot
```

### 6.6 `.claude/skills/spark/SKILL.md`

YAML frontmatter + sections：when to use / pre-flight / flat command map / output contract / playbook / common errors / hints for agent。

完整草稿见附录 A。

---

## 7. 配置文件

`~/.config/spark-cli/config.yaml`（路径可由 `$SPARK_CLI_CONFIG_DIR` 覆盖）：

```yaml
log_dirs:
  - file:///tmp/spark-events
  - hdfs://nn-prod.example.com:8020/spark-history
hdfs:
  user: "${USER}"
timeout: 30s
```

Env 覆盖：
- `SPARK_CLI_LOG_DIRS` —— 同 `--log-dirs`
- `SPARK_CLI_HDFS_USER` —— 同 `--hdfs-user`
- `SPARK_CLI_TIMEOUT` —— 同 `--timeout`

`config init` 交互式生成（询问每项）；`config show` 打印当前生效配置并标注每项来源。

---

## 8. 验收标准（DoD）

仅 MVP，全部满足才算完成：
- [ ] `make test` 全绿（lint + unit + e2e）
- [ ] 5 个场景命令在合成 testdata 上输出与 golden 完全一致
- [ ] zstd 压缩样本可正确解析；未压缩样本可正确解析
- [ ] HDFS 路径可定位（用 mock HDFS server 或 docker hadoop 跑 e2e 二级测试，至少一个）
- [ ] `--dry-run` 不发起任何 IO 之外的解析
- [ ] `config init` 交互流可生成合法 yaml
- [ ] `~/.claude/skills/spark/SKILL.md` 在 `install.sh` 后落到正确位置
- [ ] README / README.zh / SKILL.md 三份文档齐全
- [ ] `goreleaser release --snapshot --clean` 本地能跑出 5 平台二进制
- [ ] `spark-cli --version` 输出嵌入的 git tag

---

## 9. 显式不做（Out of scope）

1. 不做 SQL plan / DataFrame DAG 解析
2. 不做应用对比 / stage 对比
3. 不做 LLM 诊断（规则结果给 Claude Code，让 LLM 在外面解释）
4. 不做 daemon / 缓存层（每次重解析；同 appId 多次跑会重 IO）
5. 不做 Web UI
6. 不抓 `SparkListenerExecutorMetricsUpdate`
7. 不做 Kerberos（仅简单认证 HDFS user）
8. 不做 S3 / OSS / GCS（仅 local + HDFS）
9. 不做 lz4 / snappy 强保证（experimental）
10. 不做多 NN HA 自动切换
11. 不做 v3+ EventLog 格式（仅 Spark 3.x V1 / V2）

---

## 10. 风险

| # | 风险 | 应对 |
|---|---|---|
| R1 | Spark lz4 用 hadoop block framing，标准 Go lz4 库不兼容 | 标 experimental；首批用户给反馈再补 hadoop block reader（约 80 行） |
| R2 | t-digest 在极端分布下 P99 误差 | 误差 <1%，data-skew 用 ratio 不是绝对值，可容忍 |
| R3 | `bufio.Scanner` 默认 64KB buffer 对大事件行会截断 | 显式设 16MB max token，触发 `LOG_PARSE_FAILED` 而不是静默错 |
| R4 | V2 rolling part 的乱序 / 缺号 | 按 `events_<n>_<appId>` 数字升序拼接；缺号报 `LOG_INCOMPLETE` |
| R5 | HDFS list 在大目录下慢 | log_dirs 顺序匹配；配置时按"最常用目录在前"；实现时加 stderr 进度提示 |
| R6 | 用户输入的 applicationId 歧义 | `APP_AMBIGUOUS` 报错列出候选，不自动选 |

---

## 附录 A · `.claude/skills/spark/SKILL.md`（草稿）

```markdown
---
name: spark
description: Use when diagnosing Spark application performance — slow stages, data skew, GC pressure, disk spill, failed tasks. Locates EventLog by applicationId (local/HDFS), parses it, and returns structured JSON for analysis.
---

# Spark Diagnostic Skill

## When to use
- "Why is this Spark app slow?" / "App XXXX 这个 spark 任务为什么慢"
- "There's data skew in <appId>"
- "Diagnose application_xxx"
- 用户给一个 applicationId 要分析性能。

## Pre-flight
1. `spark-cli config show` —— 确认 log_dirs 已配置
2. 如果未配置 → 引导用户 `spark-cli config init`

## Flat command map

| 命令 | 用于 |
|---|---|
| `spark-cli diagnose <appId>` | **首选入口**——一次跑全部规则，看 critical/warn 决定下钻方向 |
| `spark-cli app-summary <appId>` | 一行总览（耗时、规模、shuffle/GC 总量） |
| `spark-cli slow-stages <appId> --top 10` | 哪些 stage 最慢 |
| `spark-cli data-skew <appId> --top 10` | 长尾 stage 列表 |
| `spark-cli gc-pressure <appId>` | 哪些 executor / stage GC 占比高 |

## Output contract
- stdout = JSON envelope `{scenario, app_id, log_path, log_format, parsed_events, columns, data}`
- stderr = `{error: {code, message, hint}}`
- exit codes: 0 / 1 / 2 / 3

## Diagnostic playbook —— "spark app is slow"
1. `spark-cli diagnose <appId>` 看 `summary.critical`
2. 如果 `data_skew` critical → `spark-cli data-skew <appId> --top 10` 拿具体 stage_id
3. 如果 `gc_pressure` critical → `spark-cli gc-pressure <appId>` 看 by_executor 段
4. 如果 `disk_spill` 触发 → 看 `slow-stages` 中对应 stage 的 `spill_disk_gb`
5. 都没触发但仍慢 → `spark-cli slow-stages <appId> --top 5`

## Common errors
| Code | Action |
|---|---|
| `CONFIG_MISSING` | run `spark-cli config init` |
| `APP_NOT_FOUND` | 检查 log_dirs，或用 `--log-dirs` 覆盖 |
| `APP_AMBIGUOUS` | 给完整 applicationId（含 ts 与 seq） |
| `HDFS_UNREACHABLE` | 检查 NN 地址、可达性、必要时 `--hdfs-user` |
| `LOG_PARSE_FAILED` | 文件可能损坏，先 `--dry-run` 确认基础信息 |

## Hints for agent
- diagnose 中 `severity: "ok"` 行表示**已检查未触发**——给用户解释"GC 已检查通过"比沉默好
- `evidence` 中带 `stage_id` 的可直接拼下一条命令
- 默认输出 JSON；除非用户明确要求"打表给我看"才传 `--format markdown`
```
