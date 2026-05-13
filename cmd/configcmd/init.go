package configcmd

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

func newInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Interactively create ~/.config/spark-cli/config.yaml",
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
			r := bufio.NewReader(cmd.InOrStdin())
			out := cmd.OutOrStdout()

			fmt.Fprint(out, "Local EventLog dir [/tmp/spark-events]: ")
			local := readLine(r, "/tmp/spark-events")
			fmt.Fprint(out, "HDFS NameNode (host:port, blank to skip): ")
			nn := readLine(r, "")
			fmt.Fprint(out, "HDFS history dir [/spark-history]: ")
			hpath := readLine(r, "/spark-history")
			fmt.Fprint(out, "HDFS user [$USER]: ")
			huser := readLine(r, os.Getenv("USER"))
			fmt.Fprint(out, "Hadoop conf dir (path to core-site.xml/hdfs-site.xml, blank to use $HADOOP_CONF_DIR/$HADOOP_HOME): ")
			hconf := readLine(r, "")
			fmt.Fprint(out, "Parse timeout [30s]: ")
			to := readLine(r, "30s")

			var b strings.Builder
			b.WriteString("log_dirs:\n")
			if local != "" {
				fmt.Fprintf(&b, "  - file://%s\n", local)
			}
			if nn != "" {
				fmt.Fprintf(&b, "  - hdfs://%s%s\n", nn, hpath)
			}
			b.WriteString("# 多个 log_dirs 按顺序探测,支持的 scheme:file:// / hdfs://nn:port/path / shs://host:port\n")
			b.WriteString("\n")
			b.WriteString("hdfs:\n")
			fmt.Fprintf(&b, "  user: %s\n", huser)
			if hconf != "" {
				fmt.Fprintf(&b, "  conf_dir: %s\n", hconf)
			}
			b.WriteString("\n")
			fmt.Fprintf(&b, "timeout: %s\n", to)
			b.WriteString("\n")
			b.WriteString("# 可选段(都有默认值,留空即可,这里列出来方便用户改):\n")
			b.WriteString("# shs:\n")
			b.WriteString("#   timeout: 5m       # SHS HTTP 拉 zip 的总超时(生产 zip 几个 GB 是常态)\n")
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
			fmt.Fprintf(out, "Wrote %s\n", path)
			return nil
		},
	}
}

func readLine(r *bufio.Reader, def string) string {
	line, _ := r.ReadString('\n')
	line = strings.TrimSpace(line)
	if line == "" {
		return def
	}
	return line
}
