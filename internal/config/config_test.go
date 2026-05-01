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
}

func TestApplyEnvOverrides(t *testing.T) {
	cfg := &Config{Timeout: 30 * time.Second}
	t.Setenv("SPARK_CLI_LOG_DIRS", "file:///a,file:///b")
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
