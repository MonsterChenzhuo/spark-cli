package fs

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

const coreSiteHA = `<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://mycluster</value></property>
</configuration>
`

const hdfsSiteHA = `<?xml version="1.0"?>
<configuration>
  <property><name>dfs.nameservices</name><value>mycluster</value></property>
  <property><name>dfs.ha.namenodes.mycluster</name><value>nn1,nn2</value></property>
  <property><name>dfs.namenode.rpc-address.mycluster.nn1</name><value>nn1.example.com:8020</value></property>
  <property><name>dfs.namenode.rpc-address.mycluster.nn2</name><value>nn2.example.com:8020</value></property>
</configuration>
`

func writeHadoopConfDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "core-site.xml"), []byte(coreSiteHA), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "hdfs-site.xml"), []byte(hdfsSiteHA), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestLoadHadoopConfFromExplicitPath(t *testing.T) {
	dir := writeHadoopConfDir(t)
	conf, err := LoadHadoopConf(dir)
	if err != nil {
		t.Fatalf("LoadHadoopConf(%q): %v", dir, err)
	}
	if conf == nil {
		t.Fatalf("LoadHadoopConf returned nil conf")
	}
	if got := conf["dfs.nameservices"]; got != "mycluster" {
		t.Errorf("dfs.nameservices = %q want mycluster", got)
	}
}

func TestLoadHadoopConfFromHADOOPCONFDIR(t *testing.T) {
	dir := writeHadoopConfDir(t)
	t.Setenv("HADOOP_CONF_DIR", dir)
	t.Setenv("HADOOP_HOME", "")
	conf, err := LoadHadoopConf("")
	if err != nil {
		t.Fatalf("LoadHadoopConf(\"\"): %v", err)
	}
	if conf["dfs.nameservices"] != "mycluster" {
		t.Errorf("dfs.nameservices = %q", conf["dfs.nameservices"])
	}
}

func TestLoadHadoopConfFromHADOOPHOME(t *testing.T) {
	root := t.TempDir()
	confDir := filepath.Join(root, "etc", "hadoop")
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(confDir, "hdfs-site.xml"), []byte(hdfsSiteHA), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HADOOP_CONF_DIR", "")
	t.Setenv("HADOOP_HOME", root)
	conf, err := LoadHadoopConf("")
	if err != nil {
		t.Fatalf("LoadHadoopConf(\"\"): %v", err)
	}
	if conf["dfs.nameservices"] != "mycluster" {
		t.Errorf("dfs.nameservices = %q", conf["dfs.nameservices"])
	}
}

func TestLoadHadoopConfNoEnvReturnsEmpty(t *testing.T) {
	t.Setenv("HADOOP_CONF_DIR", "")
	t.Setenv("HADOOP_HOME", "")
	conf, err := LoadHadoopConf("")
	if err != nil {
		t.Fatalf("LoadHadoopConf(\"\"): %v", err)
	}
	if len(conf) != 0 {
		t.Errorf("expected empty conf without env, got %d keys", len(conf))
	}
}

func TestLoadHadoopConfMissingPathErrors(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if _, err := LoadHadoopConf(missing); err == nil {
		t.Fatalf("expected error for missing dir %q", missing)
	}
}

func TestBuildClientOptionsResolvesHAAddresses(t *testing.T) {
	dir := writeHadoopConfDir(t)
	conf, err := LoadHadoopConf(dir)
	if err != nil {
		t.Fatal(err)
	}
	opts := BuildClientOptions(conf, "alice")
	if opts.User != "alice" {
		t.Errorf("opts.User = %q want alice", opts.User)
	}
	got := append([]string(nil), opts.Addresses...)
	sort.Strings(got)
	want := []string{"nn1.example.com:8020", "nn2.example.com:8020"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("Addresses = %v want %v", got, want)
	}
}

func TestBuildClientOptionsUserFallsBackToOSUSER(t *testing.T) {
	dir := writeHadoopConfDir(t)
	conf, err := LoadHadoopConf(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("USER", "fallback-user")
	opts := BuildClientOptions(conf, "")
	if opts.User != "fallback-user" {
		t.Errorf("opts.User = %q want fallback-user", opts.User)
	}
}
