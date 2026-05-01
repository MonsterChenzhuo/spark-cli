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

type Config struct {
	LogDirs []string      `yaml:"log_dirs"`
	HDFS    HDFSConfig    `yaml:"hdfs"`
	Timeout time.Duration `yaml:"timeout"`
}

const defaultTimeout = 30 * time.Second

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
	cfg := &Config{Timeout: defaultTimeout}
	path := filepath.Join(configDir(), "config.yaml")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}
	raw := struct {
		LogDirs []string   `yaml:"log_dirs"`
		HDFS    HDFSConfig `yaml:"hdfs"`
		Timeout string     `yaml:"timeout"`
	}{}
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	cfg.LogDirs = raw.LogDirs
	cfg.HDFS = raw.HDFS
	if raw.Timeout != "" {
		d, err := time.ParseDuration(raw.Timeout)
		if err != nil {
			return nil, err
		}
		cfg.Timeout = d
	}
	return cfg, nil
}

func ApplyEnv(cfg *Config) {
	if v := os.Getenv("SPARK_CLI_LOG_DIRS"); v != "" {
		cfg.LogDirs = splitCSV(v)
	}
	if v := os.Getenv("SPARK_CLI_HDFS_USER"); v != "" {
		cfg.HDFS.User = v
	}
	if v := os.Getenv("SPARK_CLI_HADOOP_CONF_DIR"); v != "" {
		cfg.HDFS.ConfDir = v
	}
	if v := os.Getenv("SPARK_CLI_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Timeout = d
		}
	}
}

type FlagOverrides struct {
	LogDirs       string
	HDFSUser      string
	HadoopConfDir string
	Timeout       time.Duration
}

func ApplyFlags(cfg *Config, f FlagOverrides) {
	if f.LogDirs != "" {
		cfg.LogDirs = splitCSV(f.LogDirs)
	}
	if f.HDFSUser != "" {
		cfg.HDFS.User = f.HDFSUser
	}
	if f.HadoopConfDir != "" {
		cfg.HDFS.ConfDir = f.HadoopConfDir
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
