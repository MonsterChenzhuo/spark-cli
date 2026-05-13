// Package config loads and validates spark-cli configuration.
package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type HDFSConfig struct {
	User string `yaml:"user"`
	// ConfDir 指向包含 core-site.xml / hdfs-site.xml 的目录; 留空则按
	// HADOOP_CONF_DIR -> HADOOP_HOME/etc/hadoop -> HADOOP_HOME/conf 自动搜索。
	// 仅 simple-auth + HA, 不支持 Kerberos / TLS。
	ConfDir string `yaml:"conf_dir"`
}

// CacheConfig 控制 *model.Application 持久化缓存的位置。
// Dir 为空时, runner 会在使用点退回 internal/cache.DefaultDir
// ($XDG_CACHE_HOME/spark-cli 或 ~/.cache/spark-cli)。
type CacheConfig struct {
	Dir string `yaml:"dir"`
}

// SHSConfig 控制 Spark History Server (`shs://`) 数据源的 HTTP 行为。
// 当前仅暴露 Timeout; TLS / 鉴权未支持。
type SHSConfig struct {
	Timeout time.Duration `yaml:"timeout"`
}

// YARNConfig 控制 ResourceManager / gateway REST 来源。BaseURLs 可以是原生
// RM 地址(http://rm:8088)或网关前缀(http://host/gateway/prod/yarn)。
type YARNConfig struct {
	BaseURLs []string `yaml:"base_urls"`
}

// SQLConfig 控制 SQL description 在 envelope 顶层 sql_executions map 中的呈现。
// Detail 合法值:"truncate"(默认) / "full" / "none"。空值 + 非法值由 normalize
// 落到 truncate。
type SQLConfig struct {
	Detail string `yaml:"detail"`
}

type Config struct {
	LogDirs []string      `yaml:"log_dirs"`
	HDFS    HDFSConfig    `yaml:"hdfs"`
	Cache   CacheConfig   `yaml:"cache"`
	SHS     SHSConfig     `yaml:"shs"`
	YARN    YARNConfig    `yaml:"yarn"`
	SQL     SQLConfig     `yaml:"sql"`
	Timeout time.Duration `yaml:"timeout"`
}

const (
	defaultTimeout = 30 * time.Second
	// defaultSHSTimeout 给 Spark History Server 整段 zip 下载预留时间。生产里
	// 几个 GB 的 EventLog zip 是常态;60s 默认值会让首次诊断在中型作业上直接挂掉,
	// 改为 5min 起步。仍可通过 --shs-timeout / SPARK_CLI_SHS_TIMEOUT / config.yaml
	// 覆盖,只是不再让用户先撞墙再翻文档。
	defaultSHSTimeout = 5 * time.Minute
)

func configDir() string {
	if d := os.Getenv("SPARK_CLI_CONFIG_DIR"); d != "" {
		return d
	}
	if h, err := os.UserHomeDir(); err == nil {
		return filepath.Join(h, ".config", "spark-cli")
	}
	return "."
}

func Load() (*Config, error) {
	cfg := &Config{
		Timeout: defaultTimeout,
		SHS:     SHSConfig{Timeout: defaultSHSTimeout},
		SQL:     SQLConfig{Detail: "truncate"},
	}
	path := filepath.Join(configDir(), "config.yaml")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}
	raw := struct {
		LogDirs []string    `yaml:"log_dirs"`
		HDFS    HDFSConfig  `yaml:"hdfs"`
		Cache   CacheConfig `yaml:"cache"`
		SHS     struct {
			Timeout string `yaml:"timeout"`
		} `yaml:"shs"`
		YARN    YARNConfig `yaml:"yarn"`
		SQL     SQLConfig  `yaml:"sql"`
		Timeout string     `yaml:"timeout"`
	}{}
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	cfg.LogDirs = raw.LogDirs
	cfg.HDFS = raw.HDFS
	cfg.Cache = raw.Cache
	cfg.YARN = raw.YARN
	if raw.SQL.Detail != "" {
		cfg.SQL.Detail = raw.SQL.Detail
	}
	if raw.Timeout != "" {
		d, err := time.ParseDuration(raw.Timeout)
		if err != nil {
			return nil, err
		}
		cfg.Timeout = d
	}
	if raw.SHS.Timeout != "" {
		d, err := time.ParseDuration(raw.SHS.Timeout)
		if err != nil {
			return nil, err
		}
		cfg.SHS.Timeout = d
	}
	return cfg, nil
}

func ApplyEnv(cfg *Config) {
	if v := os.Getenv("SPARK_CLI_LOG_DIRS"); v != "" {
		cfg.LogDirs = splitCSV(v)
	}
	if v := os.Getenv("SPARK_CLI_YARN_BASE_URLS"); v != "" {
		cfg.YARN.BaseURLs = splitCSV(v)
	}
	if v := os.Getenv("SPARK_CLI_HDFS_USER"); v != "" {
		cfg.HDFS.User = v
	}
	if v := os.Getenv("SPARK_CLI_HADOOP_CONF_DIR"); v != "" {
		cfg.HDFS.ConfDir = v
	}
	if v := os.Getenv("SPARK_CLI_CACHE_DIR"); v != "" {
		cfg.Cache.Dir = v
	}
	if v := os.Getenv("SPARK_CLI_SHS_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.SHS.Timeout = d
		}
	}
	if v := os.Getenv("SPARK_CLI_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Timeout = d
		}
	}
	if v := os.Getenv("SPARK_CLI_SQL_DETAIL"); v != "" {
		cfg.SQL.Detail = v
	}
}

type FlagOverrides struct {
	LogDirs       string
	YARNBaseURLs  string
	HDFSUser      string
	HadoopConfDir string
	CacheDir      string
	SHSTimeout    time.Duration
	SQLDetail     string
	Timeout       time.Duration
}

func ApplyFlags(cfg *Config, f FlagOverrides) {
	if f.LogDirs != "" {
		cfg.LogDirs = splitCSV(f.LogDirs)
	}
	if f.YARNBaseURLs != "" {
		cfg.YARN.BaseURLs = splitCSV(f.YARNBaseURLs)
	}
	if f.HDFSUser != "" {
		cfg.HDFS.User = f.HDFSUser
	}
	if f.HadoopConfDir != "" {
		cfg.HDFS.ConfDir = f.HadoopConfDir
	}
	if f.CacheDir != "" {
		cfg.Cache.Dir = f.CacheDir
	}
	if f.SHSTimeout > 0 {
		cfg.SHS.Timeout = f.SHSTimeout
	}
	if f.SQLDetail != "" {
		cfg.SQL.Detail = f.SQLDetail
	}
	if f.Timeout > 0 {
		cfg.Timeout = f.Timeout
	}
}

func (c *Config) Validate() error {
	if len(c.LogDirs) == 0 {
		return errors.New("log_dirs is empty; run `spark-cli config init` or set --log-dirs")
	}
	if c.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	return nil
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
