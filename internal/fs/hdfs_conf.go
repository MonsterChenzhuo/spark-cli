package fs

import (
	"fmt"
	"os"
	"path/filepath"

	hdfs "github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
)

// LoadHadoopConf 按以下优先级加载 Hadoop XML 配置:
//  1. path 非空 -> hadoopconf.Load(path)
//  2. HADOOP_CONF_DIR -> hadoopconf.Load($HADOOP_CONF_DIR)
//  3. HADOOP_HOME -> hadoopconf.Load($HADOOP_HOME/etc/hadoop) 或 $HADOOP_HOME/conf
//  4. 否则返回空 map (不报错), 由调用方决定是否退回 URI 字面 host:port
//
// path 显式指定但加载失败时返回错误; 自动发现路径找不到则返回空 map。
func LoadHadoopConf(path string) (hadoopconf.HadoopConf, error) {
	if path != "" {
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("hadoop conf dir %q: %w", path, err)
		}
		conf, err := hadoopconf.Load(path)
		if err != nil {
			return nil, fmt.Errorf("hadoop conf dir %q: %w", path, err)
		}
		return conf, nil
	}
	if d := os.Getenv("HADOOP_CONF_DIR"); d != "" {
		conf, err := hadoopconf.Load(d)
		if err != nil {
			return nil, fmt.Errorf("HADOOP_CONF_DIR=%q: %w", d, err)
		}
		return conf, nil
	}
	if h := os.Getenv("HADOOP_HOME"); h != "" {
		for _, sub := range []string{filepath.Join(h, "etc", "hadoop"), filepath.Join(h, "conf")} {
			if _, err := os.Stat(sub); err == nil {
				conf, err := hadoopconf.Load(sub)
				if err != nil {
					return nil, fmt.Errorf("HADOOP_HOME=%q: %w", h, err)
				}
				return conf, nil
			}
		}
	}
	return hadoopconf.HadoopConf{}, nil
}

// BuildClientOptions 把 HadoopConf 转成 hdfs.ClientOptions; user 非空则覆盖 conf 里的 hadoop.job.ugi。
// user 为空时退回 $USER, 与 NewHDFS 现有行为一致。仅支持 simple auth, 不引入 Kerberos。
func BuildClientOptions(conf hadoopconf.HadoopConf, user string) hdfs.ClientOptions {
	opts := hdfs.ClientOptionsFromConf(conf)
	if user != "" {
		opts.User = user
	}
	if opts.User == "" {
		opts.User = os.Getenv("USER")
	}
	return opts
}
