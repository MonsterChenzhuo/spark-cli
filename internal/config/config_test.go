package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaultsWhenNoFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Timeout != 30*time.Second {
		t.Errorf("default timeout = %v, want 30s", cfg.Timeout)
	}
	if len(cfg.LogDirs) != 0 {
		t.Errorf("default log_dirs not empty: %v", cfg.LogDirs)
	}
}

func TestLoadParsesYAML(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	body := `
log_dirs:
  - file:///tmp/spark-events
  - hdfs://nn:8020/spark-history
hdfs:
  user: alice
  conf_dir: /etc/hadoop/conf
yarn:
  base_urls:
    - http://gateway.example.com/gateway/prod/yarn
timeout: 45s
`
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.LogDirs) != 2 || cfg.LogDirs[0] != "file:///tmp/spark-events" {
		t.Errorf("log_dirs = %v", cfg.LogDirs)
	}
	if cfg.HDFS.User != "alice" {
		t.Errorf("hdfs.user = %q", cfg.HDFS.User)
	}
	if cfg.HDFS.ConfDir != "/etc/hadoop/conf" {
		t.Errorf("hdfs.conf_dir = %q", cfg.HDFS.ConfDir)
	}
	if cfg.Timeout != 45*time.Second {
		t.Errorf("timeout = %v", cfg.Timeout)
	}
	if len(cfg.YARN.BaseURLs) != 1 || cfg.YARN.BaseURLs[0] != "http://gateway.example.com/gateway/prod/yarn" {
		t.Errorf("yarn.base_urls = %v", cfg.YARN.BaseURLs)
	}
}

func TestApplyEnvOverrides(t *testing.T) {
	cfg := &Config{Timeout: 30 * time.Second}
	t.Setenv("SPARK_CLI_LOG_DIRS", "file:///a,file:///b")
	t.Setenv("SPARK_CLI_YARN_BASE_URLS", "http://rm-a:8088,http://rm-b:8088")
	t.Setenv("SPARK_CLI_HDFS_USER", "bob")
	t.Setenv("SPARK_CLI_HADOOP_CONF_DIR", "/opt/hadoop/conf")
	t.Setenv("SPARK_CLI_TIMEOUT", "10s")
	ApplyEnv(cfg)
	if len(cfg.LogDirs) != 2 || cfg.LogDirs[1] != "file:///b" {
		t.Errorf("log_dirs = %v", cfg.LogDirs)
	}
	if cfg.HDFS.User != "bob" {
		t.Errorf("user = %q", cfg.HDFS.User)
	}
	if cfg.HDFS.ConfDir != "/opt/hadoop/conf" {
		t.Errorf("conf_dir = %q", cfg.HDFS.ConfDir)
	}
	if len(cfg.YARN.BaseURLs) != 2 || cfg.YARN.BaseURLs[1] != "http://rm-b:8088" {
		t.Errorf("YARN.BaseURLs = %v", cfg.YARN.BaseURLs)
	}
	if cfg.Timeout != 10*time.Second {
		t.Errorf("timeout = %v", cfg.Timeout)
	}
}

func TestApplyFlagsTakesPrecedence(t *testing.T) {
	cfg := &Config{LogDirs: []string{"file:///a"}}
	ApplyFlags(cfg, FlagOverrides{
		LogDirs:       "file:///b,file:///c",
		HDFSUser:      "carol",
		HadoopConfDir: "/etc/hadoop/conf",
		Timeout:       5 * time.Second,
	})
	if cfg.LogDirs[0] != "file:///b" || cfg.HDFS.User != "carol" || cfg.HDFS.ConfDir != "/etc/hadoop/conf" || cfg.Timeout != 5*time.Second {
		t.Errorf("override failed: %+v", cfg)
	}
}

func TestValidateRequiresLogDirs(t *testing.T) {
	cfg := &Config{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("want validation error")
	}
}

func TestApplyEnvCacheDir(t *testing.T) {
	cfg := &Config{}
	t.Setenv("SPARK_CLI_CACHE_DIR", "/tmp/spark-cli-cache")
	ApplyEnv(cfg)
	if cfg.Cache.Dir != "/tmp/spark-cli-cache" {
		t.Errorf("Cache.Dir=%q want /tmp/spark-cli-cache", cfg.Cache.Dir)
	}
}

func TestApplyFlagsCacheDir(t *testing.T) {
	cfg := &Config{Cache: CacheConfig{Dir: "/old"}}
	ApplyFlags(cfg, FlagOverrides{CacheDir: "/new"})
	if cfg.Cache.Dir != "/new" {
		t.Errorf("Cache.Dir=%q want /new", cfg.Cache.Dir)
	}
}

func TestApplyEnvSHSTimeout(t *testing.T) {
	cfg := &Config{}
	t.Setenv("SPARK_CLI_SHS_TIMEOUT", "12s")
	ApplyEnv(cfg)
	if cfg.SHS.Timeout != 12*time.Second {
		t.Errorf("SHS.Timeout=%v want 12s", cfg.SHS.Timeout)
	}
}

func TestApplyFlagsSHSTimeout(t *testing.T) {
	cfg := &Config{SHS: SHSConfig{Timeout: 30 * time.Second}}
	ApplyFlags(cfg, FlagOverrides{SHSTimeout: 90 * time.Second})
	if cfg.SHS.Timeout != 90*time.Second {
		t.Errorf("SHS.Timeout=%v want 90s", cfg.SHS.Timeout)
	}
}

func TestLoadParsesSHSTimeout(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	body := "log_dirs:\n  - shs://h:18081\nshs:\n  timeout: 90s\n"
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SHS.Timeout != 90*time.Second {
		t.Errorf("SHS.Timeout=%v want 90s", cfg.SHS.Timeout)
	}
}

func TestLoadDefaultSHSTimeout(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SHS.Timeout != 5*time.Minute {
		t.Errorf("default SHS.Timeout=%v want 5m (生产 zip 几 GB 是常态,60s 默认会让首诊撞墙)", cfg.SHS.Timeout)
	}
}

func TestLoadParsesCacheDir(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", dir)
	body := "log_dirs:\n  - file:///x\ncache:\n  dir: /var/cache/spark-cli\n"
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cache.Dir != "/var/cache/spark-cli" {
		t.Errorf("Cache.Dir=%q want /var/cache/spark-cli", cfg.Cache.Dir)
	}
}
