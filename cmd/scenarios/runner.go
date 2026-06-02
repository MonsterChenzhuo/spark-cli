// Package scenarios glues cobra commands to the scenario pipeline.
package scenarios

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/opay-bigdata/spark-cli/internal/cache"
	"github.com/opay-bigdata/spark-cli/internal/config"
	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
	"github.com/opay-bigdata/spark-cli/internal/eventlog"
	"github.com/opay-bigdata/spark-cli/internal/fs"
	"github.com/opay-bigdata/spark-cli/internal/model"
	"github.com/opay-bigdata/spark-cli/internal/output"
	"github.com/opay-bigdata/spark-cli/internal/scenario"
	"github.com/opay-bigdata/spark-cli/internal/yarn"
)

type Options struct {
	Scenario              string
	AppID                 string
	Cluster               string
	LogDirs               []string
	YARNBaseURLs          []string
	YARNLogBytes          int64
	YARNLogTypes          []string
	ExecutorID            string
	ThreadSummaryOnly     bool
	HDFSUser              string
	HadoopConfDir         string
	CacheDir              string
	NoCache               bool
	SHSTimeout            time.Duration
	TLSInsecureSkipVerify bool
	Timeout               time.Duration
	Format                string
	Top                   int
	DryRun                bool
	// NoProgress 来自 --no-progress flag,与 SPARK_CLI_QUIET 环境变量、stdout
	// TTY 检测一并由 resolveQuiet 合成最终的 SHS 静默决定。
	NoProgress bool
	// Guided 只对 diagnose 生效:先确认命名集群选择,必要时给出录入/选择提示,
	// stdout 仍保持原 diagnose envelope,预检信息写 stderr。
	Guided bool
	// SQLDetail 控制 envelope.sql_executions 中每条 description 的呈现形态:
	// "truncate"(默认 ~500 rune)、"full"(完整 SQL)、"none"(整段 omit)。
	// 空字符串 / 非法值由 scenario.NormalizeSQLDetail 落到 truncate。
	SQLDetail string
	Stdout    io.Writer
	Stderr    io.Writer
}

func Run(ctx context.Context, opts Options) int {
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}

	cfg, err := prepareConfig(opts)
	if err != nil {
		return writeErr(opts.Stderr, err)
	}
	if opts.Scenario == "yarn-logs" {
		return runYARNLogs(ctx, opts, cfg)
	}
	if opts.Scenario == "driver-thread-dump" {
		return runDriverThreadDump(ctx, opts, cfg)
	}
	if opts.Scenario == "paimon-diagnostics" {
		return runPaimonDiagnostics(ctx, opts, cfg)
	}

	quiet := resolveQuiet(opts.NoProgress, stdoutIsTTY())
	// shsCacheDir: SHS zip 持久化路径。--no-cache 时旁路(走 system temp,与
	// application cache 行为一致),其余复用 application cache 的同一根目录,
	// 命中时第二次 CLI 调用绕过几 GB 的 zip 重下。
	shsCacheDir := ""
	if !opts.NoCache {
		shsCacheDir = cfg.Cache.Dir
		if shsCacheDir == "" {
			shsCacheDir = cache.DefaultDir()
		}
	}
	fsByScheme, closers, err := buildFS(cfg, quiet, shsCacheDir)
	if err != nil {
		return writeErr(opts.Stderr, err)
	}
	defer func() {
		for _, c := range closers {
			_ = c.Close()
		}
	}()

	loc := eventlog.NewLocator(fsByScheme, cfg.LogDirs)
	src, err := loc.Resolve(opts.AppID)
	if err != nil {
		return writeErr(opts.Stderr, err)
	}

	logPath := src.URI
	resolvedAppID := deriveAppID(opts.AppID, src)

	env := scenario.Envelope{
		ContractVersion: scenario.ContractVersion,
		Scenario:        opts.Scenario,
		AppID:           resolvedAppID,
		LogPath:         logPath,
		LogFormat:       src.Format,
		Compression:     string(src.Compression),
		Incomplete:      src.Incomplete,
	}

	if opts.DryRun {
		env.Columns = []string{"app_id", "log_path", "log_format", "compression", "incomplete", "size_mb"}
		env.Data = []any{map[string]any{
			"app_id":      resolvedAppID,
			"log_path":    logPath,
			"log_format":  src.Format,
			"compression": string(src.Compression),
			"incomplete":  src.Incomplete,
			"size_mb":     bytesToMB(src.SizeBytes),
		}}
		return render(opts, env)
	}

	fsys, err := fsForURI(fsByScheme, src.URI)
	if err != nil {
		return writeErr(opts.Stderr, err)
	}
	ch := buildCache(cfg, opts.NoCache, opts.Stderr)

	start := time.Now()
	if cached, hit := ch.Get(src, fsys); hit {
		env.AppName = cached.Name
		env.AppDurationMs = cached.DurationMs
		env.ParsedEvents = 0
		env.ElapsedMs = time.Since(start).Milliseconds()
		if err := buildScenarioBody(opts, cached, &env); err != nil {
			return writeErr(opts.Stderr, err)
		}
		if opts.Scenario == "diagnose" {
			attachYARN(ctx, opts, cfg, &env, 0)
		}
		return render(opts, env)
	}
	app, parsed, err := parseApp(fsys, src, resolvedAppID)
	if err != nil {
		return writeErr(opts.Stderr, err)
	}
	ch.Put(src, fsys, app)
	env.AppName = app.Name
	env.AppDurationMs = app.DurationMs
	env.ParsedEvents = parsed
	env.ElapsedMs = time.Since(start).Milliseconds()

	if err := buildScenarioBody(opts, app, &env); err != nil {
		return writeErr(opts.Stderr, err)
	}
	if opts.Scenario == "diagnose" {
		attachYARN(ctx, opts, cfg, &env, 0)
	}
	return render(opts, env)
}

func prepareConfig(opts Options) (*config.Config, error) {
	cfg, err := buildConfig(opts)
	if err != nil {
		return nil, err
	}
	if opts.Guided && opts.Scenario == "diagnose" {
		if err := guidedPreflight(cfg, opts, opts.Stderr); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func guidedPreflight(cfg *config.Config, opts Options, w io.Writer) error {
	if len(opts.LogDirs) > 0 {
		writePreflightEvent(w, "GUIDED_PREFLIGHT_EXPLICIT_LOG_DIRS", "info", "guided preflight using explicit --log-dirs; named cluster selection is bypassed for this run", nil)
		guidedWarnYARN(cfg, w)
		return nil
	}

	if cfg.SelectedCluster == "" {
		names := clusterNames(cfg.Clusters)
		switch len(names) {
		case 0:
			if len(cfg.LogDirs) > 0 {
				writePreflightEvent(w, "GUIDED_PREFLIGHT_LEGACY_LOG_DIRS", "info", "guided preflight using legacy top-level log_dirs; consider recording them as a named cluster", nil)
				guidedWarnYARN(cfg, w)
				return nil
			}
			return cerrors.New(
				cerrors.CodeConfigMissing,
				"no cluster profile or log_dirs configured",
				"run `spark-cli config cluster add <name> --log-dirs shs://history:18081 --yarn-base-urls http://gateway/yarn --activate`, then retry `spark-cli diagnose <appId> --guided`",
			)
		case 1:
			if err := config.ApplyCluster(cfg, names[0]); err != nil {
				return cerrors.New(cerrors.CodeFlagInvalid, err.Error(), "run `spark-cli config cluster list` to inspect configured clusters")
			}
			writePreflightEvent(w, "GUIDED_PREFLIGHT_CLUSTER_SELECTED", "info", fmt.Sprintf("guided preflight selected only configured cluster %q", names[0]), map[string]any{"cluster": names[0]})
		default:
			return cerrors.New(
				cerrors.CodeFlagInvalid,
				"multiple cluster profiles configured but none selected",
				"run `spark-cli config cluster list --format json`, then retry with `--cluster <name>` or set active_cluster via `spark-cli config cluster add <name> ... --activate`",
			)
		}
	} else {
		writePreflightEvent(w, "GUIDED_PREFLIGHT_CLUSTER_SELECTED", "info", fmt.Sprintf("guided preflight using cluster %q", cfg.SelectedCluster), map[string]any{"cluster": cfg.SelectedCluster})
	}

	if len(cfg.LogDirs) == 0 {
		return cerrors.New(
			cerrors.CodeConfigMissing,
			"selected cluster has no log_dirs",
			"update it with `spark-cli config cluster add "+cfg.SelectedCluster+" --log-dirs shs://history:18081 --yarn-base-urls http://gateway/yarn --activate`",
		)
	}
	guidedWarnYARN(cfg, w)
	return nil
}

func clusterNames(clusters map[string]config.ClusterConfig) []string {
	names := make([]string, 0, len(clusters))
	for name := range clusters {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func guidedWarnYARN(cfg *config.Config, w io.Writer) {
	if len(cfg.YARN.BaseURLs) == 0 {
		writePreflightEvent(w, "GUIDED_PREFLIGHT_YARN_MISSING", "warn", "yarn.base_urls is empty; diagnose will run EventLog rules but live YARN/thread probes need a YARN gateway URL", nil)
		return
	}
	writePreflightEvent(w, "GUIDED_PREFLIGHT_YARN_FOUND", "info", fmt.Sprintf("guided preflight found %d YARN base URL(s) for live probes", len(cfg.YARN.BaseURLs)), map[string]any{"count": len(cfg.YARN.BaseURLs)})
}

func writePreflightEvent(w io.Writer, code, level, message string, fields map[string]any) {
	cerrors.WriteEventJSON(w, cerrors.Event{
		Code:    code,
		Level:   level,
		Message: message,
		Fields:  fields,
	})
}

func runYARNLogs(ctx context.Context, opts Options, cfg *config.Config) int {
	env := scenario.Envelope{
		ContractVersion: scenario.ContractVersion,
		Scenario:        opts.Scenario,
		AppID:           opts.AppID,
		Columns:         []string{"base_url", "app", "containers", "warnings"},
	}
	report, err := fetchYARN(ctx, opts, cfg, opts.YARNLogBytes)
	if err != nil {
		return writeErr(opts.Stderr, err)
	}
	env.Data = []any{report}
	return render(opts, env)
}

func runDriverThreadDump(ctx context.Context, opts Options, cfg *config.Config) int {
	columns := []string{
		"base_url", "app", "ui_url", "thread_dump_url", "executor_id",
		"thread_count", "state_counts", "warnings", "diagnosis",
		"main_thread", "interesting_threads",
	}
	if !opts.ThreadSummaryOnly {
		columns = append(columns, "threads")
	}
	env := scenario.Envelope{
		ContractVersion: scenario.ContractVersion,
		Scenario:        opts.Scenario,
		AppID:           opts.AppID,
		Columns:         columns,
	}
	report, err := fetchThreadDump(ctx, opts, cfg)
	if err != nil {
		return writeErr(opts.Stderr, cerrors.New(
			cerrors.CodeLogUnreadable,
			err.Error(),
			"检查 --yarn-base-urls 是否指向可访问的 YARN gateway;若指定 --executor-id,确认该 executor 仍 active;可先跑 yarn-logs 看 tracking_url",
		))
	}
	if opts.ThreadSummaryOnly {
		report.Threads = nil
	}
	env.Data = []any{report}
	return render(opts, env)
}

func runPaimonDiagnostics(ctx context.Context, opts Options, cfg *config.Config) int {
	env := scenario.Envelope{
		ContractVersion: scenario.ContractVersion,
		Scenario:        opts.Scenario,
		AppID:           opts.AppID,
		Columns: []string{
			"base_url", "app", "ui_url", "overview_url", "thread_dump_url",
			"profiler_url", "executor_id", "overview", "thread_dump",
			"profiler", "warnings",
		},
	}
	report, err := fetchPaimonDiagnostics(ctx, opts, cfg)
	if err != nil {
		return writeErr(opts.Stderr, cerrors.New(
			cerrors.CodeLogUnreadable,
			err.Error(),
			"检查 --yarn-base-urls 是否指向可访问的 YARN gateway;确认 Spark 应用启用了 PaimonProfilerPlugin 并能访问 Spark UI",
		))
	}
	env.Data = []any{report}
	return render(opts, env)
}

func parseApp(fsys fs.FS, src eventlog.LogSource, appID string) (*model.Application, int64, error) {
	r, err := eventlog.Open(src, fsys)
	if err != nil {
		return nil, 0, err
	}
	defer r.Close()
	app := model.NewApplication()
	app.ID = appID
	agg := model.NewAggregator(app)
	count, err := eventlog.DecodeWithOptions(r, agg, eventlog.DecodeOptions{
		AllowTruncatedTail: src.Incomplete,
	})
	if err != nil {
		return nil, int64(count), err
	}
	return app, int64(count), nil
}

// buildCache resolves the effective cache directory (config / env / flag chain
// already merged into cfg.Cache.Dir) and returns a *cache.Cache. NoCache forces
// Disabled; an empty cfg.Cache.Dir falls back to cache.DefaultDir().
func buildCache(cfg *config.Config, noCache bool, stderr io.Writer) *cache.Cache {
	if noCache {
		return cache.Disabled()
	}
	dir := cfg.Cache.Dir
	if dir == "" {
		dir = cache.DefaultDir()
	}
	return cache.NewWithWarningWriter(dir, stderr)
}

func render(opts Options, env scenario.Envelope) int {
	var err error
	switch opts.Format {
	case "", "json":
		err = output.WriteJSON(opts.Stdout, env)
	default:
		err = cerrors.New(cerrors.CodeFlagInvalid, fmt.Sprintf("unknown --format %q", opts.Format), "use json")
	}
	if err != nil {
		return writeErr(opts.Stderr, err)
	}
	return 0
}

func writeErr(w io.Writer, err error) int {
	cerrors.WriteJSON(w, err)
	return cerrors.ExitCode(err)
}

func buildConfig(opts Options) (*config.Config, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, cerrors.New(cerrors.CodeConfigMissing, err.Error(), "")
	}
	config.ApplyEnv(cfg)
	if opts.Cluster != "" {
		if err := config.ApplyCluster(cfg, opts.Cluster); err != nil {
			return nil, cerrors.New(cerrors.CodeFlagInvalid, err.Error(), "run `spark-cli config cluster list` to see configured clusters, or add one with `spark-cli config cluster add`")
		}
	}
	if len(opts.LogDirs) > 0 {
		cfg.LogDirs = opts.LogDirs
	}
	if len(opts.YARNBaseURLs) > 0 {
		cfg.YARN.BaseURLs = opts.YARNBaseURLs
	}
	if opts.HDFSUser != "" {
		cfg.HDFS.User = opts.HDFSUser
	}
	if opts.HadoopConfDir != "" {
		cfg.HDFS.ConfDir = opts.HadoopConfDir
	}
	if opts.CacheDir != "" {
		cfg.Cache.Dir = opts.CacheDir
	}
	if opts.SHSTimeout > 0 {
		cfg.SHS.Timeout = opts.SHSTimeout
	}
	if opts.TLSInsecureSkipVerify {
		cfg.TLS.InsecureSkipVerify = true
	}
	if opts.SQLDetail != "" {
		cfg.SQL.Detail = opts.SQLDetail
	}
	if opts.Timeout > 0 {
		cfg.Timeout = opts.Timeout
	}
	if opts.Scenario == "yarn-logs" || opts.Scenario == "driver-thread-dump" || opts.Scenario == "paimon-diagnostics" {
		if len(cfg.YARN.BaseURLs) == 0 {
			return nil, cerrors.New(cerrors.CodeFlagInvalid, "yarn.base_urls is empty; set --yarn-base-urls or config yarn.base_urls", "例如 --yarn-base-urls http://203.123.81.20:7765/gateway/hadoop-prod/yarn")
		}
		return cfg, nil
	}
	if opts.Guided && opts.Scenario == "diagnose" {
		return cfg, nil
	}
	if err := cfg.Validate(); err != nil {
		return nil, cerrors.New(cerrors.CodeFlagInvalid, err.Error(), validateHint(err))
	}
	return cfg, nil
}

func attachYARN(ctx context.Context, opts Options, cfg *config.Config, env *scenario.Envelope, maxLogBytes int64) {
	if len(cfg.YARN.BaseURLs) == 0 {
		return
	}
	opts.AppID = env.AppID
	report, err := fetchYARN(ctx, opts, cfg, maxLogBytes)
	if err != nil {
		env.YARN = map[string]any{"warnings": []string{err.Error()}}
		return
	}
	env.YARN = report
}

func fetchYARN(ctx context.Context, opts Options, cfg *config.Config, maxLogBytes int64) (*yarn.Report, error) {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return yarn.NewClientWithOptions(cfg.YARN.BaseURLs, timeout, yarn.ClientOptions{
		InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
	}).FetchApplicationLogs(ctx, opts.AppID, yarn.Options{
		TopContainers: opts.Top,
		LogTypes:      opts.YARNLogTypes,
		MaxLogBytes:   maxLogBytes,
		ExecutorID:    opts.ExecutorID,
	})
}

func fetchThreadDump(ctx context.Context, opts Options, cfg *config.Config) (*yarn.ThreadDumpReport, error) {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return yarn.NewClientWithOptions(cfg.YARN.BaseURLs, timeout, yarn.ClientOptions{
		InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
	}).FetchThreadDump(ctx, opts.AppID, opts.ExecutorID)
}

func fetchPaimonDiagnostics(ctx context.Context, opts Options, cfg *config.Config) (*yarn.PaimonDiagnosticsReport, error) {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return yarn.NewClientWithOptions(cfg.YARN.BaseURLs, timeout, yarn.ClientOptions{
		InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
	}).FetchPaimonDiagnostics(ctx, opts.AppID, opts.ExecutorID)
}

// validateHint 给 cfg.Validate() 错误一个可执行 hint。Validate 自身在 internal/config
// 不依赖 internal/errors,所以 hint 由 runner 层补上,免得用户看到光秃秃 message
// 还得自己猜下一步做什么。
func validateHint(err error) string {
	msg := err.Error()
	if strings.Contains(msg, "log_dirs is empty") {
		return "run `spark-cli config init` 写默认 config,或加 --log-dirs file:///path / hdfs://nn/path / shs://host:port / shs+https://host:port"
	}
	if strings.Contains(msg, "timeout must be positive") {
		return "config.yaml `timeout:` 或 --timeout 必须是正值,例如 30s"
	}
	return ""
}

func buildFS(cfg *config.Config, quiet bool, shsCacheDir string) (map[string]fs.FS, []io.Closer, error) {
	out := map[string]fs.FS{}
	var closers []io.Closer
	hdfsAddr := ""
	for _, dir := range cfg.LogDirs {
		u, err := url.Parse(dir)
		if err != nil {
			return nil, nil, cerrors.New(cerrors.CodeFlagInvalid, "bad log_dir: "+dir, "use file://, hdfs://, shs:// or shs+https://")
		}
		switch u.Scheme {
		case "file":
			if _, ok := out["file"]; !ok {
				lf := fs.NewLocal()
				out["file"] = lf
				closers = append(closers, lf)
			}
		case "hdfs":
			if hdfsAddr == "" {
				hdfsAddr = u.Host
			}
			if _, ok := out["hdfs"]; !ok {
				h, err := buildHDFS(cfg, hdfsAddr)
				if err != nil {
					return nil, closers, err
				}
				out["hdfs"] = h
				closers = append(closers, h)
			}
		case "shs", "shs+https":
			if _, ok := out[u.Scheme]; !ok {
				sh := fs.NewSHS(strings.TrimRight(dir, "/"), cfg.SHS.Timeout, fs.SHSOptions{
					Quiet:              quiet,
					CacheDir:           shsCacheDir,
					UserAgent:          "spark-cli/" + CLIVersion,
					InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
				})
				out[u.Scheme] = sh
				closers = append(closers, sh)
			}
		default:
			return nil, closers, cerrors.New(cerrors.CodeFlagInvalid, "unsupported scheme: "+u.Scheme, "use file://, hdfs://, shs:// or shs+https://")
		}
	}
	return out, closers, nil
}

// buildHDFS 优先用 Hadoop XML 配置 (cfg.HDFS.ConfDir / HADOOP_CONF_DIR / HADOOP_HOME);
// 加载到非空 Addresses 时 URI host 仅用作 List() 返回 URI 的字面前缀。
// 否则退回旧路径: 直接用 URI 的 host:port 直连 (要求形如 hdfs://host:port/...)。
func buildHDFS(cfg *config.Config, uriHost string) (*fs.HDFS, error) {
	conf, err := fs.LoadHadoopConf(cfg.HDFS.ConfDir)
	if err != nil {
		return nil, cerrors.New(cerrors.CodeConfigMissing, err.Error(), "check hadoop_conf_dir / HADOOP_CONF_DIR / HADOOP_HOME")
	}
	if len(conf) > 0 {
		opts := fs.BuildClientOptions(conf, cfg.HDFS.User)
		if len(opts.Addresses) > 0 {
			h, err := fs.NewHDFSWithOptions(uriHost, opts)
			if err != nil {
				return nil, cerrors.New(cerrors.CodeHDFSUnreachable, err.Error(), "check hadoop conf NameNode addresses and credentials")
			}
			return h, nil
		}
	}
	h, err := fs.NewHDFS(uriHost, cfg.HDFS.User)
	if err != nil {
		return nil, cerrors.New(cerrors.CodeHDFSUnreachable, err.Error(), "check hdfs:// addr and credentials, or set HADOOP_CONF_DIR for HA")
	}
	return h, nil
}

func fsForURI(fsByScheme map[string]fs.FS, uri string) (fs.FS, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, cerrors.New(cerrors.CodeInternal, "bad URI: "+uri, "")
	}
	fsys, ok := fsByScheme[u.Scheme]
	if !ok {
		return nil, cerrors.New(cerrors.CodeInternal, "no FS for scheme "+u.Scheme, "")
	}
	return fsys, nil
}

func deriveAppID(input string, src eventlog.LogSource) string {
	base := path.Base(src.URI)
	for strings.HasSuffix(base, ".inprogress") {
		base = strings.TrimSuffix(base, ".inprogress")
	}
	for _, ext := range []string{".zstd", ".lz4", ".snappy"} {
		if strings.HasSuffix(base, ext) {
			base = strings.TrimSuffix(base, ext)
			break
		}
	}
	base = strings.TrimPrefix(base, "eventlog_v2_")
	if strings.HasPrefix(base, "application_") {
		return base
	}
	if input != "" {
		return input
	}
	return base
}

const mbBytes = 1024 * 1024

func bytesToMB(b int64) float64 {
	x := float64(b) / float64(mbBytes) * 1000
	if x < 0 {
		x -= 0.5
	} else {
		x += 0.5
	}
	return float64(int64(x)) / 1000
}
