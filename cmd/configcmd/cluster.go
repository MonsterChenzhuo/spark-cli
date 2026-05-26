package configcmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/opay-bigdata/spark-cli/internal/config"
)

type clusterFile struct {
	LogDirs       []string                      `yaml:"log_dirs,omitempty"`
	ActiveCluster string                        `yaml:"active_cluster,omitempty"`
	Clusters      map[string]clusterFileProfile `yaml:"clusters,omitempty"`
	HDFS          config.HDFSConfig             `yaml:"hdfs,omitempty"`
	Cache         config.CacheConfig            `yaml:"cache,omitempty"`
	SHS           timeoutFileConfig             `yaml:"shs,omitempty"`
	YARN          config.YARNConfig             `yaml:"yarn,omitempty"`
	SQL           config.SQLConfig              `yaml:"sql,omitempty"`
	Timeout       string                        `yaml:"timeout,omitempty"`
}

type clusterFileProfile struct {
	LogDirs []string          `yaml:"log_dirs,omitempty"`
	YARN    config.YARNConfig `yaml:"yarn,omitempty"`
	SHS     timeoutFileConfig `yaml:"shs,omitempty"`
}

type timeoutFileConfig struct {
	Timeout string `yaml:"timeout,omitempty"`
}

func newClusterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Manage named cluster profiles",
	}
	cmd.AddCommand(newClusterAddCmd())
	cmd.AddCommand(newClusterListCmd())
	return cmd
}

func newClusterAddCmd() *cobra.Command {
	var logDirs, yarnBaseURLs, shsTimeout string
	var activate bool
	c := &cobra.Command{
		Use:   "add <name>",
		Short: "Add or update a named cluster profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := strings.TrimSpace(args[0])
			if name == "" {
				return fmt.Errorf("cluster name cannot be empty")
			}
			if strings.TrimSpace(logDirs) == "" {
				return fmt.Errorf("--log-dirs is required")
			}
			if shsTimeout != "" {
				if _, err := time.ParseDuration(shsTimeout); err != nil {
					return fmt.Errorf("invalid --shs-timeout %q: %w", shsTimeout, err)
				}
			}
			file, path, err := readClusterFile()
			if err != nil {
				return err
			}
			if file.Clusters == nil {
				file.Clusters = map[string]clusterFileProfile{}
			}
			file.Clusters[name] = clusterFileProfile{
				LogDirs: splitCSV(logDirs),
				YARN:    config.YARNConfig{BaseURLs: splitCSV(yarnBaseURLs)},
				SHS:     timeoutFileConfig{Timeout: shsTimeout},
			}
			if activate || file.ActiveCluster == "" {
				file.ActiveCluster = name
			}
			if err := writeClusterFile(path, file); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Wrote cluster %q to %s\n", name, path)
			return nil
		},
	}
	c.Flags().StringVar(&logDirs, "log-dirs", "", "Comma-separated EventLog sources for this cluster")
	c.Flags().StringVar(&yarnBaseURLs, "yarn-base-urls", "", "Comma-separated YARN RM/gateway URLs for this cluster")
	c.Flags().StringVar(&shsTimeout, "shs-timeout", "", "Optional SHS HTTP timeout for this cluster, e.g. 5m")
	c.Flags().BoolVar(&activate, "activate", false, "Set this cluster as active_cluster")
	return c
}

func newClusterListCmd() *cobra.Command {
	var format string
	c := &cobra.Command{
		Use:   "list",
		Short: "List configured cluster profiles",
		RunE: func(cmd *cobra.Command, args []string) error {
			file, _, err := readClusterFile()
			if err != nil {
				return err
			}
			switch format {
			case "json":
				return renderClusterListJSON(cmd.OutOrStdout(), file)
			case "", "text":
				renderClusterListText(cmd.OutOrStdout(), file)
				return nil
			default:
				return fmt.Errorf("unknown --format %q (use text|json)", format)
			}
		},
	}
	c.Flags().StringVar(&format, "format", "", "Output format: text (default) | json")
	return c
}

func readClusterFile() (clusterFile, string, error) {
	path, err := configFilePath()
	if err != nil {
		return clusterFile{}, "", err
	}
	var file clusterFile
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return file, path, nil
		}
		return clusterFile{}, "", err
	}
	if err := yaml.Unmarshal(b, &file); err != nil {
		return clusterFile{}, "", err
	}
	return file, path, nil
}

func writeClusterFile(path string, file clusterFile) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := yaml.Marshal(file)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func configFilePath() (string, error) {
	dir := os.Getenv("SPARK_CLI_CONFIG_DIR")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		dir = filepath.Join(home, ".config", "spark-cli")
	}
	return filepath.Join(dir, "config.yaml"), nil
}

type clusterListRow struct {
	Name         string   `json:"name"`
	Active       bool     `json:"active"`
	LogDirs      []string `json:"log_dirs,omitempty"`
	YARNBaseURLs []string `json:"yarn_base_urls,omitempty"`
	SHSTimeout   string   `json:"shs_timeout,omitempty"`
}

func clusterRows(file clusterFile) []clusterListRow {
	names := make([]string, 0, len(file.Clusters))
	for name := range file.Clusters {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([]clusterListRow, 0, len(names))
	for _, name := range names {
		cluster := file.Clusters[name]
		rows = append(rows, clusterListRow{
			Name:         name,
			Active:       name == file.ActiveCluster,
			LogDirs:      cluster.LogDirs,
			YARNBaseURLs: cluster.YARN.BaseURLs,
			SHSTimeout:   cluster.SHS.Timeout,
		})
	}
	return rows
}

func renderClusterListJSON(w io.Writer, file clusterFile) error {
	out := struct {
		ActiveCluster string           `json:"active_cluster,omitempty"`
		Clusters      []clusterListRow `json:"clusters"`
	}{
		ActiveCluster: file.ActiveCluster,
		Clusters:      clusterRows(file),
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(out)
}

func renderClusterListText(w io.Writer, file clusterFile) {
	fmt.Fprintf(w, "active_cluster: %s\n", file.ActiveCluster)
	rows := clusterRows(file)
	if len(rows) == 0 {
		fmt.Fprintln(w, "clusters: (none)")
		return
	}
	fmt.Fprintln(w, "clusters:")
	for _, row := range rows {
		marker := " "
		if row.Active {
			marker = "*"
		}
		fmt.Fprintf(w, "%s %s\n", marker, row.Name)
		for _, d := range row.LogDirs {
			fmt.Fprintf(w, "    log_dir: %s\n", d)
		}
		for _, u := range row.YARNBaseURLs {
			fmt.Fprintf(w, "    yarn: %s\n", u)
		}
		if row.SHSTimeout != "" {
			fmt.Fprintf(w, "    shs.timeout: %s\n", row.SHSTimeout)
		}
	}
}
