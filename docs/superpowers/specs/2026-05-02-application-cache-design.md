# spark-cli Application 缓存层设计文档

> **状态**：Draft（待用户复审）
> **日期**：2026-05-02
> **作者**：brainstorming session（Claude Code 协作）
> **目标读者**：实现者 + 后续维护者
> **关联**：`docs/superpowers/specs/2026-04-29-spark-cli-design.md`(MVP 设计)

---

## 0. 背景与目标

### 0.1 背景

当前 spark-cli 是无状态工具：每条命令都从头执行 `Locator.Resolve` → `Open` → `Decode` → `Aggregator` → `Scenario` 全流程。对于 1.36 GB 的 EventLog（实测 application_1772605260987_17515），完整解析约 7 秒；对于多分片 V2 日志（10+ GB 累计字节）可达 30 秒以上。

典型 agent 工作流是 **diagnose → 多次 drill-down**（`slow-stages`、`data-skew`、`gc-pressure`、`app-summary`），每次 drill-down 都重复同一份解析，浪费 90% 以上 CPU 与墙钟时间。

### 0.2 目标

为 `*model.Application` 建立本地磁盘缓存，让"同一 appId 第二次起的命令"在 ~200 ms 内返回（vs 7 s+ 重新解析）。

具体指标：
- 缓存 hit 路径完全避开 `Open + Decode + Aggregate`
- 缓存 miss 路径不引入额外延迟（写盘异步代价 < 解析总耗时 5%）
- 缓存失效 100% 准确：源 EventLog 任何字节级变化都触发 miss
- 不破坏现有 `Envelope` 输出契约
- 不要求用户手动管理缓存

### 0.3 非目标

- **不**支持跨 app 查询/聚合（不是 SQL 仓库，是单 app 二进制 blob 缓存）
- **不**做 TTL/LRU 自动淘汰（YAGNI；用户嫌大手动 `rm -rf` 即可）
- **不**实现增量更新（`.inprogress` 日志整体跳过缓存）
- **不**支持 SHS REST API zip 缓存（独立后续 spec，本期不涉及）
- **不**改变 `internal/model` 字段结构或对外 API
- **不**做跨进程并发协调（多 spark-cli 同时跑同一 appId 是合法但罕见，靠原子 rename 兜底）

---

## 1. 缓存策略

### 1.1 缓存粒度

**一个 appId 一份 blob**，包含完整 `*model.Application`。

理由：
- 所有现有命令都消费整个 `Application`（`scenario.AppSummary` / `SlowStages` / `DataSkew` / `GCPressure` / `Diagnose` + 6 条 rules 全是输入 `*Application`）
- 命中一次服务后续任意命令组合
- 比"按 scenario 缓存"省 N 倍空间，比"按事件流缓存"省一遍 aggregator 计算

### 1.2 缓存 key（失效检测）

缓存的 *源指纹* 由 `LogSource.Format` 决定：

**V1（单文件）：** `{URI, Mtime, Size}`
**V2（多分片目录）：** `{DirURI, MaxPartMtime, TotalSize, PartCount}`

任何字节级变化（追加、新分片、删分片、重写）都让三元组（V1）或四元组（V2）变化，即触发 miss。

> **不**用 SHA-256 / xxhash —— 那要读全文件，与"避免重新解析"初衷矛盾。

### 1.3 不缓存的情形

- `LogSource.Incomplete == true`（即 `.inprogress` 日志）—— 文件正在写，命中率近 0、空写代价
- 用户传 `--no-cache` —— 调试或质疑缓存正确性时旁路
- 缓存目录不可写（permission denied、disk full）—— 静默 warn 一次，继续走解析路径

### 1.4 缓存目录优先级

由高到低：
1. `--cache-dir <path>` flag
2. `SPARK_CLI_CACHE_DIR` env var
3. `config.yaml` 的 `cache.dir` 字段
4. `$XDG_CACHE_HOME/spark-cli/`
5. `~/.cache/spark-cli/`

`spark-cli config show` 应当列出缓存目录及其来源。

---

## 2. 文件格式

### 2.1 物理布局

每个 app 一个文件：`<cache_dir>/<sanitized_appId>.gob.zst`

`sanitized_appId` 把 `application_<id>_<attempt>` 当文件名直接写（appId 本身已无特殊字符）。`.gob.zst` 后缀显式表明格式 + 压缩。

### 2.2 序列化栈

```
zstd(gob(envelope))
```

- `gob`：Go stdlib 二进制序列化，对 `map[string]*Executor` / `map[StageKey]*Stage` / `[]BlacklistEvent` / 嵌套 `*Stage` 自然支持
- `zstd`：复用现有 `klauspost/compress/zstd`（已用于 EventLog 解压），level 默认（fastest 之上一档）
- 文件大小估算：50 MB Application → ~7 MB 文件

### 2.3 Envelope 结构

```go
// internal/cache/envelope.go
type cacheEnvelope struct {
    SchemaVersion int    // bump 触发缓存失效；初值 1
    CLIVersion    string // cmd.version，调试用，不参与失效判定
    SourceKind    string // "v1" | "v2"
    SourceKey     sourceKey
    App           *model.Application
}

type sourceKey struct {
    URI       string
    MaxMtime  int64 // unix nanoseconds
    TotalSize int64
    PartCount int   // V1 恒为 1，V2 为分片数
}
```

### 2.4 schema 版本管理

`SchemaVersion` 是手动维护的常量。**触发 bump 的条件**：
- `model.Application` / `Stage` / `Executor` / `Job` / `SQLExecution` / `BlacklistEvent` 任意字段**改类型**或**重命名**
- `stats.Digest` gob 编码格式变更
- 字段**新增**或**删除**：gob 已天然容忍，**不需要** bump

bump 后旧缓存 100% 视为 miss，悄悄重建。**绝不向用户报错**。

> 编辑 model 时若忘记 bump：用户拿到的是旧字段值（按零值或老结构填充）—— 风险存在但有限，因为 gob 对类型变化会直接报 decode 错误，被 cache 层 catch 后当作 miss。

---

## 3. 包结构与 API

### 3.1 新包 `internal/cache`

```
internal/cache/
├── cache.go         // Cache struct + Get/Put + 路径处理
├── cache_test.go    // 单测
├── envelope.go      // cacheEnvelope + sourceKey
├── source_key.go    // 从 LogSource 计算 sourceKey
└── digest_gob.go    // stats.Digest 的 GobEncoder/GobDecoder
```

### 3.2 对外 API

```go
package cache

type Cache struct {
    dir     string
    enabled bool
}

// New 返回一个缓存器；dir 不可写时仍返回非 nil 实例，但 Put 会静默失败。
func New(dir string) *Cache

// Disabled 返回一个永远 miss、永不写盘的实例（--no-cache 等价物）。
func Disabled() *Cache

// Get 计算 src 的当前指纹，与磁盘 envelope 比对：
//   - 文件不存在 / 指纹不匹配 / schema 不匹配 / 解码失败 → (nil, false)
//   - 命中 → (app, true)
//   - 永远不会返回 error；所有失败退化为 miss + stderr 一行 warn
func (c *Cache) Get(src eventlog.LogSource, fsys fs.FS) (*model.Application, bool)

// Put 把 app 序列化到 <dir>/<appId>.gob.zst（原子 rename）。
//   - .inprogress 跳过
//   - 写盘失败 → 静默丢弃，不影响 caller
func (c *Cache) Put(src eventlog.LogSource, fsys fs.FS, app *model.Application)

// Path 返回 src 对应的缓存文件全路径（用于测试 + 调试输出）。
func (c *Cache) Path(src eventlog.LogSource) string
```

> Get / Put **不返回 error**。缓存层是性能优化，永远不能因为缓存损坏让 CLI 报错。所有问题转 stderr 一行 `warn: cache <op> <reason>` 文本（直接 `fmt.Fprintln(os.Stderr, ...)`），不经 `internal/errors` 那套结构化 envelope（envelope 是给 agent 解析的，warn 是给人看的）。

### 3.3 stats.Digest 编解码

`stats.Digest` 当前字段（`td *tdigest.TDigest` + `count int`）全部未导出。给它实现：

```go
// internal/stats/tdigest.go 增加方法（非 _gob.go，因为 stats 包应自洽）

func (d *Digest) GobEncode() ([]byte, error) {
    // 写: count int + len(centroids) int + (mean, weight) × N
}

func (d *Digest) GobDecode(buf []byte) error {
    // 读回 → NewWithCompression(100) + AddCentroidList
}
```

`tdigest.TDigest.Centroids()` 返回 `CentroidList`（`[]Centroid{Mean, Weight}` 全导出），`AddCentroidList` 反向构造。Round-trip 测试断言 quantile 在 ±0.5% 内一致即可（t-digest 重建本身有微小数值误差）。

### 3.4 fs.FS 接口扩展

为了拿到 mtime，需要扩展 `fs.FileInfo`：

```go
// internal/fs/fs.go
type FileInfo struct {
    URI     string
    Name    string
    Size    int64
    ModTime int64 // unix nanoseconds，新增；本地走 os.FileInfo.ModTime().UnixNano()，HDFS 走 colinmarc/hdfs FileStatus.ModificationTime（毫秒，转纳秒）
    IsDir   bool
}
```

`local.go` / `hdfs.go` 的 `Stat` 都需要填这个字段。

> HDFS FileStatus 的 mtime 精度是毫秒（Hadoop 协议限制），转 ns 时低 6 位永远为 0，不影响失效判定。

---

## 4. 接入点

### 4.1 runner.Run 改造

```go
// cmd/scenarios/runner.go 现状（伪码）
func Run(ctx Context) (Envelope, error) {
    src := ctx.Locator.Resolve(appId)
    rd := eventlog.Open(src, ctx.FSByScheme)
    defer rd.Close()
    app := aggregator.Aggregate(eventlog.Decode(rd))
    return scenario.Dispatch(ctx.Scenario, app), nil
}

// 改造后
func Run(ctx Context) (Envelope, error) {
    src := ctx.Locator.Resolve(appId)
    fsys := ctx.FSByScheme[scheme(src.URI)]

    if app, hit := ctx.Cache.Get(src, fsys); hit {
        return scenario.Dispatch(ctx.Scenario, app), nil
    }

    rd := eventlog.Open(src, ctx.FSByScheme)
    defer rd.Close()
    app := aggregator.Aggregate(eventlog.Decode(rd))
    ctx.Cache.Put(src, fsys, app)

    return scenario.Dispatch(ctx.Scenario, app), nil
}
```

### 4.2 Cache 注入路径

`cmd/scenarios/register.go` 的 `newScenarioCmd` 在 cobra `RunE` 内构造 Cache：

```go
var c *cache.Cache
if cfg.NoCache {
    c = cache.Disabled()
} else {
    c = cache.New(cfg.CacheDir)
}
ctx.Cache = c
```

`internal/config` 增加：
- `CacheDir string` (resolve 优先级见 §1.4)
- `NoCache bool` （仅 flag/env，不进 yaml；`SPARK_CLI_NO_CACHE`）

### 4.3 新 flag

所有 5 个场景命令 + `diagnose` 共享：

```
--cache-dir <path>   缓存目录（覆盖配置）
--no-cache           本次执行跳过缓存（不读不写）
```

通过 cobra `PersistentFlags` 注册在 root 上。`config show` 同步打印 `cache.dir` 与 `cache.no_cache`（来源标注）。

### 4.4 envelope 字段

`scenario.Envelope` **不**新增 `cache_hit` 字段（保持对外契约稳定）。改在 stderr 用一行结构化 log 标注：

```
{"level":"info","cache":"hit","app_id":"...","elapsed_ms":182}
{"level":"info","cache":"miss","app_id":"...","reason":"size_changed","elapsed_ms":7034}
```

> 这条 log 仅在 `SPARK_CLI_LOG_CACHE=1` 环境变量打开时输出，默认静默以保持 stderr 纯净（`internal/errors` 的 error envelope 不变）。

---

## 5. 错误处理

| 场景 | 行为 |
|---|---|
| 缓存目录不存在 | `MkdirAll` 创建；失败则 disable 本次缓存，warn 一行 |
| 缓存文件读失败 | 视为 miss，**不**报错给用户 |
| zstd 解码失败 | 视为 miss + 删除损坏文件 + warn |
| gob 解码失败（含 schema mismatch） | 视为 miss + 删除损坏文件 + warn |
| 源指纹不匹配 | 视为 miss + 删除旧文件（有意覆盖） |
| `--no-cache` | Cache 实例为 Disabled；Get 永远返回 false，Put 是 no-op |
| `.inprogress` 日志 | Put 直接 no-op；Get 仍照常查（不会有缓存，但接口对称） |
| 写盘失败（disk full / permission） | 静默丢弃 + warn 一次 |
| 多进程并发写同一 app | 每个进程写到 `<file>.tmp.<pid>`（pid 已唯一），再 `os.Rename` 原子替换；最后赢家覆盖 |

**核心原则**：缓存层永远不能让 CLI 失败。所有故障路径都退化到"假装没有缓存"。

---

## 6. 测试矩阵

### 6.1 单元测试 `internal/cache/cache_test.go`

- `TestPutGetRoundTrip` —— 写入合成 Application，读回断言所有字段相等
- `TestGetMissOnEmptyDir`
- `TestGetMissOnSizeChange`
- `TestGetMissOnMtimeChange`
- `TestGetMissOnPartCountChange`（V2）
- `TestGetMissOnSchemaVersion` —— 手动伪造旧 envelope，断言 miss
- `TestGetMissOnCorruptFile` —— 写半截字节，断言 miss + 文件被删
- `TestPutSkipsInprogress`
- `TestPutAtomicReplace` —— 并发写 100 次，最后文件仍可读
- `TestDisabled` —— `Disabled().Get()` 永远 false，`Put()` 不写盘

### 6.2 stats.Digest 测试 `internal/stats/tdigest_test.go`

- `TestDigestGobRoundTrip` —— 加 1000 个值 → encode → decode → quantile P50/P95/P99 与原值在 ±0.5% 内一致
- `TestDigestGobEmpty` —— 空 digest round-trip 仍能 quantile 返回 0

### 6.3 fs.FileInfo 测试

- `TestLocalStatModTime` —— 写文件 → Stat → ModTime > 0 且接近当前时间
- `TestHDFSStatModTime` —— mock HDFS client，断言 ms→ns 转换正确

### 6.4 e2e 测试 `tests/e2e/cache_e2e_test.go`（新增 -tags=e2e 文件）

- `TestE2ECacheHit` —— 跑两次 `app-summary`，第二次 elapsed_ms 远低于第一次（断言 < 1/3）
- `TestE2ECacheInvalidationOnTouch` —— 跑一次 → `os.Chtimes` 改源 mtime → 第二次断言 miss + 重新解析
- `TestE2ENoCacheFlag` —— `--no-cache` 不写文件
- `TestE2ECrossSchema` —— 用旧 SchemaVersion 写假缓存 → 跑命令断言被忽略

### 6.5 现有测试保持绿

`tests/e2e/e2e_test.go` 不动；新增 e2e 测试都用独立 t.TempDir() 的 cache_dir，互不干扰。

---

## 7. 性能预算

| 场景 | 目标 |
|---|---|
| 1.36 GB V2 日志，首次（miss）| 与改造前持平（±5%）|
| 1.36 GB V2 日志，二次（hit）| < 300 ms |
| 1.36 GB V2 日志，hit 路径解码 | < 200 ms |
| 1.36 GB V2 日志，cache 文件大小 | < 15 MB |
| `tiny_app.json` fixture（10 行）| 写 cache 增加 < 5 ms |
| Cache hit 内存峰值 | 与解析路径持平（都得物化 `*Application`）|

> 性能数字写进 spec 是为了实现期间有锚。`make e2e` 不强制时间断言（避免 CI 抖动），但本地手测必须达成。

---

## 8. 文档更新

实现完成后**同一 commit / 同一 PR** 链路同步：

- `CLAUDE.md`
  - 新增"缓存层"章节：缓存目录、失效逻辑、`--no-cache`、`SchemaVersion` bump 规则
  - "已知踩坑"补充：忘记 bump SchemaVersion 时的容错行为
  - "添加新模型字段"步骤补一条："如果改了字段类型或重命名，bump `internal/cache/envelope.go` 的 `currentSchemaVersion`"
- `README.md` / `README.zh.md`
  - "Configuration" 段补 `cache.dir` / `--cache-dir` / `--no-cache` / `SPARK_CLI_CACHE_DIR`
- `CHANGELOG.md` / `CHANGELOG.zh.md`
  - Unreleased 段加一节 `### Application 缓存`
- `.claude/skills/spark/SKILL.md`
  - 不动主流程；在 "Useful flags" 列表加 `--no-cache`
- 不写 `docs/examples/` 教程（YAGNI；行为对 agent 透明）

---

## 9. 实现里程碑

按顺序、每步独立 commit、TDD：

1. **fs.FileInfo.ModTime 字段 + local/hdfs 填充 + 测试**
2. **stats.Digest GobEncoder/GobDecoder + round-trip 测试**
3. **internal/cache 包骨架：sourceKey、envelope、Cache.New/Disabled/Path**
4. **Cache.Put 实现 + 单测（含 .inprogress 跳过、原子 rename）**
5. **Cache.Get 实现 + 单测（含 miss/corrupt/schema mismatch）**
6. **config + flag 接线（`--cache-dir` / `--no-cache` / env / yaml）**
7. **runner.Run 接入 Cache + e2e 测试**
8. **文档同步（CLAUDE.md / README / CHANGELOG / SKILL.md）**

每步前 `make tidy && make lint && make unit-test` 全绿；最后一步前再跑 `make e2e`。

---

## 10. 风险与回滚

**风险**：
1. gob 对 `map[StageKey]*Stage` / 嵌套指针的编解码慢于预期 → 备选 zstd level 降一档；若仍不达标，fallback 到 `encoding/binary` + 手工编 hot path
2. HDFS Stat 的 mtime 精度问题（Hadoop 旧版可能返回 0）→ 在 source_key 计算里若 mtime == 0 退化为只用 size + count
3. `model.Application` 重构导致频繁 schema bump → 不是真正风险，bump 只是把缓存清空，无副作用
4. CI 上 e2e 时间断言波动 → e2e 不做时间断言（详见 §6.5），只断言 hit/miss 语义

**回滚**：
缓存层是纯加法，回滚等价于：
1. 删除 `internal/cache` 包
2. 还原 `runner.Run`、`fs.FileInfo`、`stats.Digest`、cobra flags
3. 删除文档相关章节

任何 commit 都可独立 revert，不会留下半成品。

---

## 11. 与未来 SHS API 集成的关系

本期 cache 是 SHS 集成的**前置依赖**：未来 `--history-url <base>` 拉 zip → 临时盘解压 → 走现有 V1/V2 路径 → cache 命中策略对 SHS source 透明（cache key 用解压后的本地源指纹即可）。

SHS 集成另起独立 spec（`docs/superpowers/specs/2026-XX-XX-spark-history-source-design.md`），不在本期范围。
