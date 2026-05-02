package configcmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/opay-bigdata/spark-cli/internal/cache"
	"github.com/opay-bigdata/spark-cli/internal/config"
)

type sources struct {
	LogDirs       string // "flag" | "env" | "file" | "default"
	HDFSUser      string
	HadoopConfDir string
	CacheDir      string
	SHSTimeout    string
	SQLDetail     string
	Timeout       string
}

func newShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show",
		Short: "Print the effective configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}
			src := detectSources(cfg)
			config.ApplyEnv(cfg)
			render(cmd.OutOrStdout(), cfg, src)
			return nil
		},
	}
}

func detectSources(cfg *config.Config) sources {
	src := sources{LogDirs: "default", HDFSUser: "default", HadoopConfDir: "default", CacheDir: "default", SHSTimeout: "default", SQLDetail: "default", Timeout: "default"}
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

func render(w io.Writer, cfg *config.Config, src sources) {
	fmt.Fprintf(w, "log_dirs (%s):\n", src.LogDirs)
	if len(cfg.LogDirs) == 0 {
		fmt.Fprintln(w, "  (none — run `spark-cli config init`)")
	}
	for _, d := range cfg.LogDirs {
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
