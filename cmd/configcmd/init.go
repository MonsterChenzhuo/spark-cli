package configcmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

func newInitCmd() *cobra.Command {
	var localLogDir, hdfsNameNode, hdfsHistoryDir, hdfsUser, hadoopConfDir, timeout string
	c := &cobra.Command{
		Use:   "init",
		Short: "Create ~/.config/spark-cli/config.yaml",
		RunE: func(cmd *cobra.Command, args []string) error {
			home, err := os.UserHomeDir()
			if err != nil {
				return err
			}
			dir := filepath.Join(home, ".config", "spark-cli")
			if v := os.Getenv("SPARK_CLI_CONFIG_DIR"); v != "" {
				dir = v
			}
			if err := os.MkdirAll(dir, 0755); err != nil {
				return err
			}

			logDirs := initLogDirs(localLogDir, hdfsNameNode, hdfsHistoryDir)
			var b strings.Builder
			b.WriteString("log_dirs:\n")
			for _, d := range logDirs {
				fmt.Fprintf(&b, "  - %s\n", d)
			}
			b.WriteString("# 多个 log_dirs 按顺序探测,支持的 scheme:file:// / hdfs://nn:port/path / shs://host:port / shs+https://host:port\n")
			b.WriteString("\n")
			b.WriteString("# 推荐用命名集群把 Spark History Server 和 YARN gateway 绑定在一起:\n")
			b.WriteString("# active_cluster: prod\n")
			b.WriteString("# clusters:\n")
			b.WriteString("#   prod:\n")
			b.WriteString("#     log_dirs:\n")
			b.WriteString("#       - shs://history-server:18081\n")
			b.WriteString("#     yarn:\n")
			b.WriteString("#       base_urls:\n")
			b.WriteString("#         - http://gateway/prod/yarn\n")
			b.WriteString("# 也可以用 `spark-cli config cluster add prod --log-dirs shs://history-server:18081 --yarn-base-urls http://gateway/prod/yarn --activate` 录入。\n")
			b.WriteString("\n")
			b.WriteString("hdfs:\n")
			fmt.Fprintf(&b, "  user: %s\n", hdfsUser)
			if hadoopConfDir != "" {
				fmt.Fprintf(&b, "  conf_dir: %s\n", hadoopConfDir)
			}
			b.WriteString("\n")
			fmt.Fprintf(&b, "timeout: %s\n", timeout)
			b.WriteString("\n")
			b.WriteString("# 可选段(都有默认值,留空即可,这里列出来方便用户改):\n")
			b.WriteString("# shs:\n")
			b.WriteString("#   timeout: 5m       # SHS HTTP 拉 zip 的总超时(生产 zip 几个 GB 是常态)\n")
			b.WriteString("# tls:\n")
			b.WriteString("#   insecure_skip_verify: false # HTTPS SHS/YARN gateway 使用自签证书时才改 true\n")
			b.WriteString("# cache:\n")
			b.WriteString("#   dir: \"\"           # 应用缓存路径,留空走 $XDG_CACHE_HOME/spark-cli 或 ~/.cache/spark-cli\n")
			b.WriteString("# yarn:\n")
			b.WriteString("#   base_urls: []      # YARN RM/gateway,如 http://host/gateway/prod/yarn\n")
			b.WriteString("# sql:\n")
			b.WriteString("#   detail: truncate  # sql_executions 字段呈现:truncate(默认 ~500 rune) | full | none\n")

			path := filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(path, []byte(b.String()), 0644); err != nil {
				return err
			}
			resp := initResponse{
				Command: "config init",
				Path:    path,
				Written: true,
				Config: initConfigResponse{
					LogDirs: logDirs,
					HDFS: initHDFSResponse{
						User:    hdfsUser,
						ConfDir: hadoopConfDir,
					},
					Timeout: timeout,
				},
			}
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetEscapeHTML(false)
			return enc.Encode(resp)
		},
	}
	c.Flags().StringVar(&localLogDir, "local-log-dir", "/tmp/spark-events", "Local EventLog directory to include as file:// source; empty to omit")
	c.Flags().StringVar(&hdfsNameNode, "hdfs-namenode", "", "HDFS NameNode host:port to include; empty to omit")
	c.Flags().StringVar(&hdfsHistoryDir, "hdfs-history-dir", "/spark-history", "HDFS Spark history directory used with --hdfs-namenode")
	c.Flags().StringVar(&hdfsUser, "hdfs-user", os.Getenv("USER"), "HDFS simple-auth user")
	c.Flags().StringVar(&hadoopConfDir, "hadoop-conf-dir", "", "Path to Hadoop XML config dir")
	c.Flags().StringVar(&timeout, "timeout", "30s", "Parse timeout")
	return c
}

type initResponse struct {
	Command string             `json:"command"`
	Path    string             `json:"path"`
	Written bool               `json:"written"`
	Config  initConfigResponse `json:"config"`
}

type initConfigResponse struct {
	LogDirs []string         `json:"log_dirs"`
	HDFS    initHDFSResponse `json:"hdfs"`
	Timeout string           `json:"timeout"`
}

type initHDFSResponse struct {
	User    string `json:"user"`
	ConfDir string `json:"conf_dir,omitempty"`
}

func initLogDirs(localLogDir, hdfsNameNode, hdfsHistoryDir string) []string {
	var out []string
	if localLogDir = strings.TrimSpace(localLogDir); localLogDir != "" {
		out = append(out, "file://"+localLogDir)
	}
	if hdfsNameNode = strings.TrimSpace(hdfsNameNode); hdfsNameNode != "" {
		if !strings.HasPrefix(hdfsHistoryDir, "/") {
			hdfsHistoryDir = "/" + hdfsHistoryDir
		}
		out = append(out, "hdfs://"+hdfsNameNode+hdfsHistoryDir)
	}
	return out
}
