// Package scenarios glues cobra commands to the scenario pipeline.
package scenarios

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
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
	Scenario      string
	AppID         string
	LogDirs       []string
	YARNBaseURLs  []string
	YARNLogBytes  int64
	HDFSUser      string
	HadoopConfDir string
	CacheDir      string
	NoCache       bool
	SHSTimeout    time.Duration
	Timeout       time.Duration
	Format        string
	Top           int
	DryRun        bool
	// NoProgress 来自 --no-progress flag,与 SPARK_CLI_QUIET 环境变量、stdout
	// TTY 检测一并由 resolveQuiet 合成最终的 SHS 静默决定。
	NoProgress bool
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

	cfg, err := buildConfig(opts)
	if err != nil {
		return writeErr(opts.Stderr, err)
	}
	if opts.Scenario == "yarn-logs" {
		return runYARNLogs(ctx, opts, cfg)
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
		Scenario:    opts.Scenario,
		AppID:       resolvedAppID,
		LogPath:     logPath,
		LogFormat:   src.Format,
		Compression: string(src.Compression),
		Incomplete:  src.Incomplete,
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
	ch := buildCache(cfg, opts.NoCache)

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

func runYARNLogs(ctx context.Context, opts Options, cfg *config.Config) int {
	env := scenario.Envelope{
		Scenario: opts.Scenario,
		AppID:    opts.AppID,
		Columns:  []string{"base_url", "app", "containers", "warnings"},
	}
	report, err := fetchYARN(ctx, opts, cfg, opts.YARNLogBytes)
	if err != nil {
		return writeErr(opts.Stderr, err)
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
func buildCache(cfg *config.Config, noCache bool) *cache.Cache {
	if noCache {
		return cache.Disabled()
	}
	dir := cfg.Cache.Dir
	if dir == "" {
		dir = cache.DefaultDir()
	}
	return cache.New(dir)
}

func render(opts Options, env scenario.Envelope) int {
	var err error
	switch opts.Format {
	case "", "json":
		err = output.WriteJSON(opts.Stdout, env)
	case "table":
		err = output.WriteTable(opts.Stdout, env)
	case "markdown":
		err = output.WriteMarkdown(opts.Stdout, env)
	default:
		err = cerrors.New(cerrors.CodeFlagInvalid, fmt.Sprintf("unknown --format %q", opts.Format), "use json|table|markdown")
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
	if opts.SQLDetail != "" {
		cfg.SQL.Detail = opts.SQLDetail
	}
	if opts.Timeout > 0 {
		cfg.Timeout = opts.Timeout
	}
	if opts.Scenario == "yarn-logs" {
		if len(cfg.YARN.BaseURLs) == 0 {
			return nil, cerrors.New(cerrors.CodeFlagInvalid, "yarn.base_urls is empty; set --yarn-base-urls or config yarn.base_urls", "例如 --yarn-base-urls http://203.123.81.20:7765/gateway/hadoop-prod/yarn")
		}
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
	return yarn.NewClient(cfg.YARN.BaseURLs, timeout).FetchApplicationLogs(ctx, opts.AppID, yarn.Options{
		TopContainers: opts.Top,
		MaxLogBytes:   maxLogBytes,
	})
}

// validateHint 给 cfg.Validate() 错误一个可执行 hint。Validate 自身在 internal/config
// 不依赖 internal/errors,所以 hint 由 runner 层补上,免得用户看到光秃秃 message
// 还得自己猜下一步做什么。
func validateHint(err error) string {
	msg := err.Error()
	if strings.Contains(msg, "log_dirs is empty") {
		return "run `spark-cli config init` 写默认 config,或加 --log-dirs file:///path / hdfs://nn/path / shs://host:port"
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
			return nil, nil, cerrors.New(cerrors.CodeFlagInvalid, "bad log_dir: "+dir, "use file:// or hdfs://")
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
		case "shs":
			if _, ok := out["shs"]; !ok {
				sh := fs.NewSHS(strings.TrimRight(dir, "/"), cfg.SHS.Timeout, fs.SHSOptions{
					Quiet:     quiet,
					CacheDir:  shsCacheDir,
					UserAgent: "spark-cli/" + CLIVersion,
				})
				out["shs"] = sh
				closers = append(closers, sh)
			}
		default:
			return nil, closers, cerrors.New(cerrors.CodeFlagInvalid, "unsupported scheme: "+u.Scheme, "use file://, hdfs:// or shs://")
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
