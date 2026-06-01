package configcmd

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/opay-bigdata/spark-cli/internal/cache"
	"github.com/opay-bigdata/spark-cli/internal/config"
	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
)

type sources struct {
	Cluster               string // "flag" | "env" | "file" | "default"
	LogDirs               string
	YARNBaseURLs          string
	HDFSUser              string
	HadoopConfDir         string
	CacheDir              string
	SHSTimeout            string
	TLSInsecureSkipVerify string
	SQLDetail             string
	Timeout               string
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
			case "", "json":
				return renderJSON(cmd.OutOrStdout(), cfg, src)
			default:
				return cerrors.New(cerrors.CodeFlagInvalid, "unknown --format "+strconv.Quote(format), "use json")
			}
		},
	}
	c.Flags().StringVar(&format, "format", "", "Output format: json")
	return c
}

func detectSources(cfg *config.Config) sources {
	src := defaultSources()
	markFileSources(cfg, &src)
	markEnvSources(&src)
	return src
}

func defaultSources() sources {
	return sources{
		Cluster: "default", LogDirs: "default", YARNBaseURLs: "default",
		HDFSUser: "default", HadoopConfDir: "default", CacheDir: "default",
		SHSTimeout: "default", TLSInsecureSkipVerify: "default",
		SQLDetail: "default", Timeout: "default",
	}
}

func markFileSources(cfg *config.Config, src *sources) {
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
		if cfg.ActiveCluster != "" || len(cfg.Clusters) > 0 {
			src.Cluster = "file"
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
		if cfg.TLS.InsecureSkipVerify {
			src.TLSInsecureSkipVerify = "file"
		}
		if cfg.SQL.Detail != "" && cfg.SQL.Detail != "truncate" {
			src.SQLDetail = "file"
		}
		src.Timeout = "file"
	}
}

func markEnvSources(src *sources) {
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
	if os.Getenv("SPARK_CLI_TLS_INSECURE_SKIP_VERIFY") != "" {
		src.TLSInsecureSkipVerify = "env"
	}
	if os.Getenv("SPARK_CLI_SQL_DETAIL") != "" {
		src.SQLDetail = "env"
	}
	if os.Getenv("SPARK_CLI_TIMEOUT") != "" {
		src.Timeout = "env"
	}
}

// applyRootFlagOverrides 让 root persistent flag(--log-dirs / --cache-dir /
// --hdfs-user / --hadoop-conf-dir / --shs-timeout / --sql-detail / --timeout /
// --cluster)覆盖到 cfg 与 src 上。这样 `config show --cache-dir /tmp/x` 显示的
// cache.dir 才能反映用户当下传的 flag,而不是仍报 yaml/default 的旧值,
// 让 source 标签从 "default"/"file"/"env" 升级到 "flag"。
func applyRootFlagOverrides(cmd *cobra.Command, cfg *config.Config, src *sources) {
	if cmd == nil {
		return
	}
	applyClusterFlagOverride(cmd, cfg, src)
	applySourceFlagOverrides(cmd, cfg, src)
	applyPathFlagOverrides(cmd, cfg, src)
	applyTuningFlagOverrides(cmd, cfg, src)
}

func changedPersistentFlag(cmd *cobra.Command, name string) (string, bool) {
	flags := cmd.Root().PersistentFlags()
	f := flags.Lookup(name)
	if f == nil || !f.Changed {
		return "", false
	}
	return f.Value.String(), true
}

func applyClusterFlagOverride(cmd *cobra.Command, cfg *config.Config, src *sources) {
	if v, ok := changedPersistentFlag(cmd, "cluster"); ok && v != "" {
		if err := config.ApplyCluster(cfg, v); err == nil {
			src.Cluster = "flag"
			src.LogDirs = "flag"
			src.YARNBaseURLs = "flag"
			if cfg.SHS.Timeout > 0 {
				src.SHSTimeout = "flag"
			}
			if cfg.TLS.InsecureSkipVerify {
				src.TLSInsecureSkipVerify = "flag"
			}
		}
	}
}

func applySourceFlagOverrides(cmd *cobra.Command, cfg *config.Config, src *sources) {
	if v, ok := changedPersistentFlag(cmd, "log-dirs"); ok && v != "" {
		cfg.LogDirs = splitCSV(v)
		src.LogDirs = "flag"
	}
	if v, ok := changedPersistentFlag(cmd, "yarn-base-urls"); ok && v != "" {
		cfg.YARN.BaseURLs = splitCSV(v)
		src.YARNBaseURLs = "flag"
	}
}

func applyPathFlagOverrides(cmd *cobra.Command, cfg *config.Config, src *sources) {
	if v, ok := changedPersistentFlag(cmd, "cache-dir"); ok && v != "" {
		cfg.Cache.Dir = v
		src.CacheDir = "flag"
	}
	if v, ok := changedPersistentFlag(cmd, "hdfs-user"); ok && v != "" {
		cfg.HDFS.User = v
		src.HDFSUser = "flag"
	}
	if v, ok := changedPersistentFlag(cmd, "hadoop-conf-dir"); ok && v != "" {
		cfg.HDFS.ConfDir = v
		src.HadoopConfDir = "flag"
	}
}

func applyTuningFlagOverrides(cmd *cobra.Command, cfg *config.Config, src *sources) {
	if v, ok := changedPersistentFlag(cmd, "shs-timeout"); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.SHS.Timeout = d
			src.SHSTimeout = "flag"
		}
	}
	if v, ok := changedPersistentFlag(cmd, "sql-detail"); ok && v != "" {
		cfg.SQL.Detail = v
		src.SQLDetail = "flag"
	}
	if v, ok := changedPersistentFlag(cmd, "tls-insecure-skip-verify"); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.TLS.InsecureSkipVerify = b
			src.TLSInsecureSkipVerify = "flag"
		}
	}
	if v, ok := changedPersistentFlag(cmd, "timeout"); ok && v != "" {
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
		"active_cluster":           {Source: src.Cluster, Value: cfg.ActiveCluster},
		"selected_cluster":         {Source: src.Cluster, Value: cfg.SelectedCluster},
		"clusters":                 {Source: src.Cluster, Value: clustersForShow(cfg.Clusters)},
		"log_dirs":                 {Source: src.LogDirs, Value: cfg.LogDirs},
		"yarn.base_urls":           {Source: src.YARNBaseURLs, Value: cfg.YARN.BaseURLs},
		"hdfs.user":                {Source: src.HDFSUser, Value: cfg.HDFS.User},
		"hdfs.conf_dir":            {Source: src.HadoopConfDir, Value: cfg.HDFS.ConfDir},
		"cache.dir":                {Source: src.CacheDir, Value: cacheDir},
		"shs.timeout":              {Source: src.SHSTimeout, Value: cfg.SHS.Timeout.String()},
		"tls.insecure_skip_verify": {Source: src.TLSInsecureSkipVerify, Value: cfg.TLS.InsecureSkipVerify},
		"sql.detail":               {Source: src.SQLDetail, Value: sqlDetail},
		"timeout":                  {Source: src.Timeout, Value: cfg.Timeout.String()},
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(out)
}

type showCluster struct {
	LogDirs      []string `json:"log_dirs,omitempty"`
	YARNBaseURLs []string `json:"yarn_base_urls,omitempty"`
	SHSTimeout   string   `json:"shs_timeout,omitempty"`
	TLSInsecure  bool     `json:"tls_insecure_skip_verify,omitempty"`
}

func clustersForShow(clusters map[string]config.ClusterConfig) map[string]showCluster {
	if len(clusters) == 0 {
		return nil
	}
	out := make(map[string]showCluster, len(clusters))
	for name, cluster := range clusters {
		row := showCluster{
			LogDirs:      cluster.LogDirs,
			YARNBaseURLs: cluster.YARN.BaseURLs,
		}
		if cluster.SHS.Timeout > 0 {
			row.SHSTimeout = cluster.SHS.Timeout.String()
		}
		row.TLSInsecure = cluster.TLS.InsecureSkipVerify
		out[name] = row
	}
	return out
}
