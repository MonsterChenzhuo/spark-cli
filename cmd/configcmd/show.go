package configcmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/opay-bigdata/spark-cli/internal/cache"
	"github.com/opay-bigdata/spark-cli/internal/config"
)

type sources struct {
	LogDirs       string // "flag" | "env" | "file" | "default"
	YARNBaseURLs  string
	HDFSUser      string
	HadoopConfDir string
	CacheDir      string
	SHSTimeout    string
	SQLDetail     string
	Timeout       string
}

func newShowCmd() *cobra.Command {
	var format string
	c := &cobra.Command{
		Use:   "show",
		Short: "Print the effective configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}
			src := detectSources(cfg)
			config.ApplyEnv(cfg)
			applyRootFlagOverrides(cmd, cfg, &src)
			switch format {
			case "json":
				return renderJSON(cmd.OutOrStdout(), cfg, src)
			case "", "text":
				render(cmd.OutOrStdout(), cfg, src)
				return nil
			default:
				return fmt.Errorf("unknown --format %q (use text|json)", format)
			}
		},
	}
	c.Flags().StringVar(&format, "format", "", "Output format: text (default) | json")
	return c
}

func detectSources(cfg *config.Config) sources {
	src := sources{LogDirs: "default", YARNBaseURLs: "default", HDFSUser: "default", HadoopConfDir: "default", CacheDir: "default", SHSTimeout: "default", SQLDetail: "default", Timeout: "default"}
	dir := os.Getenv("SPARK_CLI_CONFIG_DIR")
	if dir == "" {
		home, _ := os.UserHomeDir()
		dir = filepath.Join(home, ".config", "spark-cli")
	}
	path := filepath.Join(dir, "config.yaml")
	if _, err := os.Stat(path); err == nil {
		if len(cfg.LogDirs) > 0 {
			src.LogDirs = "file"
		}
		if len(cfg.YARN.BaseURLs) > 0 {
			src.YARNBaseURLs = "file"
		}
		if cfg.HDFS.User != "" {
			src.HDFSUser = "file"
		}
		if cfg.HDFS.ConfDir != "" {
			src.HadoopConfDir = "file"
		}
		if cfg.Cache.Dir != "" {
			src.CacheDir = "file"
		}
		if cfg.SHS.Timeout != 0 {
			src.SHSTimeout = "file"
		}
		if cfg.SQL.Detail != "" && cfg.SQL.Detail != "truncate" {
			src.SQLDetail = "file"
		}
		src.Timeout = "file"
	}
	if os.Getenv("SPARK_CLI_LOG_DIRS") != "" {
		src.LogDirs = "env"
	}
	if os.Getenv("SPARK_CLI_YARN_BASE_URLS") != "" {
		src.YARNBaseURLs = "env"
	}
	if os.Getenv("SPARK_CLI_HDFS_USER") != "" {
		src.HDFSUser = "env"
	}
	if os.Getenv("SPARK_CLI_HADOOP_CONF_DIR") != "" {
		src.HadoopConfDir = "env"
	}
	if os.Getenv("SPARK_CLI_CACHE_DIR") != "" {
		src.CacheDir = "env"
	}
	if os.Getenv("SPARK_CLI_SHS_TIMEOUT") != "" {
		src.SHSTimeout = "env"
	}
	if os.Getenv("SPARK_CLI_SQL_DETAIL") != "" {
		src.SQLDetail = "env"
	}
	if os.Getenv("SPARK_CLI_TIMEOUT") != "" {
		src.Timeout = "env"
	}
	return src
}

// applyRootFlagOverrides 让 root persistent flag(--log-dirs / --cache-dir /
// --hdfs-user / --hadoop-conf-dir / --shs-timeout / --sql-detail / --timeout)
// 覆盖到 cfg 与 src 上。这样 `config show --cache-dir /tmp/x` 显示的
// cache.dir 才能反映用户当下传的 flag,而不是仍报 yaml/default 的旧值,
// 让 source 标签从 "default"/"file"/"env" 升级到 "flag"。
func applyRootFlagOverrides(cmd *cobra.Command, cfg *config.Config, src *sources) {
	if cmd == nil {
		return
	}
	flags := cmd.Root().PersistentFlags()
	get := func(name string) (string, bool) {
		f := flags.Lookup(name)
		if f == nil || !f.Changed {
			return "", false
		}
		return f.Value.String(), true
	}
	if v, ok := get("log-dirs"); ok && v != "" {
		cfg.LogDirs = splitCSV(v)
		src.LogDirs = "flag"
	}
	if v, ok := get("yarn-base-urls"); ok && v != "" {
		cfg.YARN.BaseURLs = splitCSV(v)
		src.YARNBaseURLs = "flag"
	}
	if v, ok := get("cache-dir"); ok && v != "" {
		cfg.Cache.Dir = v
		src.CacheDir = "flag"
	}
	if v, ok := get("hdfs-user"); ok && v != "" {
		cfg.HDFS.User = v
		src.HDFSUser = "flag"
	}
	if v, ok := get("hadoop-conf-dir"); ok && v != "" {
		cfg.HDFS.ConfDir = v
		src.HadoopConfDir = "flag"
	}
	if v, ok := get("shs-timeout"); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.SHS.Timeout = d
			src.SHSTimeout = "flag"
		}
	}
	if v, ok := get("sql-detail"); ok && v != "" {
		cfg.SQL.Detail = v
		src.SQLDetail = "flag"
	}
	if v, ok := get("timeout"); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Timeout = d
			src.Timeout = "flag"
		}
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// renderJSON 输出 effective configuration 的结构化形态。每个字段含 value
// 与 source(file / env / default),让 agent 一次拿到"现在生效什么、来自哪"。
func renderJSON(w io.Writer, cfg *config.Config, src sources) error {
	cacheDir := cfg.Cache.Dir
	if cacheDir == "" {
		cacheDir = cache.DefaultDir()
	}
	sqlDetail := cfg.SQL.Detail
	if sqlDetail == "" {
		sqlDetail = "truncate"
	}
	type field struct {
		Source string `json:"source"`
		Value  any    `json:"value"`
	}
	out := map[string]field{
		"log_dirs":       {Source: src.LogDirs, Value: cfg.LogDirs},
		"yarn.base_urls": {Source: src.YARNBaseURLs, Value: cfg.YARN.BaseURLs},
		"hdfs.user":      {Source: src.HDFSUser, Value: cfg.HDFS.User},
		"hdfs.conf_dir":  {Source: src.HadoopConfDir, Value: cfg.HDFS.ConfDir},
		"cache.dir":      {Source: src.CacheDir, Value: cacheDir},
		"shs.timeout":    {Source: src.SHSTimeout, Value: cfg.SHS.Timeout.String()},
		"sql.detail":     {Source: src.SQLDetail, Value: sqlDetail},
		"timeout":        {Source: src.Timeout, Value: cfg.Timeout.String()},
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(out)
}

func render(w io.Writer, cfg *config.Config, src sources) {
	fmt.Fprintf(w, "log_dirs (%s):\n", src.LogDirs)
	if len(cfg.LogDirs) == 0 {
		fmt.Fprintln(w, "  (none — run `spark-cli config init`)")
	}
	for _, d := range cfg.LogDirs {
		fmt.Fprintf(w, "  - %s\n", d)
	}
	fmt.Fprintf(w, "yarn.base_urls (%s):\n", src.YARNBaseURLs)
	if len(cfg.YARN.BaseURLs) == 0 {
		fmt.Fprintln(w, "  (none)")
	}
	for _, d := range cfg.YARN.BaseURLs {
		fmt.Fprintf(w, "  - %s\n", d)
	}
	fmt.Fprintf(w, "hdfs.user (%s): %s\n", src.HDFSUser, cfg.HDFS.User)
	fmt.Fprintf(w, "hdfs.conf_dir (%s): %s\n", src.HadoopConfDir, cfg.HDFS.ConfDir)
	cacheDir := cfg.Cache.Dir
	if cacheDir == "" {
		cacheDir = cache.DefaultDir()
	}
	fmt.Fprintf(w, "cache.dir (%s): %s\n", src.CacheDir, cacheDir)
	fmt.Fprintf(w, "shs.timeout (%s): %s\n", src.SHSTimeout, cfg.SHS.Timeout)
	sqlDetail := cfg.SQL.Detail
	if sqlDetail == "" {
		sqlDetail = "truncate"
	}
	fmt.Fprintf(w, "sql.detail (%s): %s\n", src.SQLDetail, sqlDetail)
	fmt.Fprintf(w, "timeout (%s): %s\n", src.Timeout, cfg.Timeout)
}
