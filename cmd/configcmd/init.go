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
			b.WriteString("hdfs:\n")
			fmt.Fprintf(&b, "  user: %s\n", huser)
			fmt.Fprintf(&b, "timeout: %s\n", to)

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
