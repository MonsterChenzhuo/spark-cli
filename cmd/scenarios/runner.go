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
)

type Options struct {
	Scenario      string
	AppID         string
	LogDirs       []string
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
	Stdout     io.Writer
	Stderr     io.Writer
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
		env.ParsedEvents = 0
		env.ElapsedMs = time.Since(start).Milliseconds()
		if err := buildScenarioBody(opts, cached, &env); err != nil {
			return writeErr(opts.Stderr, err)
		}
		return render(opts, env)
	}
	app, parsed, err := parseApp(fsys, src, resolvedAppID)
	if err != nil {
		return writeErr(opts.Stderr, err)
	}
	ch.Put(src, fsys, app)
	env.AppName = app.Name
	env.ParsedEvents = parsed
	env.ElapsedMs = time.Since(start).Milliseconds()

	if err := buildScenarioBody(opts, app, &env); err != nil {
		return writeErr(opts.Stderr, err)
	}
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
	count, err := eventlog.Decode(r, agg)
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
	if opts.Timeout > 0 {
		cfg.Timeout = opts.Timeout
	}
	if err := cfg.Validate(); err != nil {
		return nil, cerrors.New(cerrors.CodeFlagInvalid, err.Error(), "")
	}
	return cfg, nil
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
				sh := fs.NewSHS("shs://"+u.Host, cfg.SHS.Timeout, fs.SHSOptions{
					Quiet:    quiet,
					CacheDir: shsCacheDir,
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
