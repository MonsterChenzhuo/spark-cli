package cachecmd

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/opay-bigdata/spark-cli/internal/cache"
)

type cacheEntry struct {
	Kind    string  `json:"kind"` // "application" | "shs_zip"
	Name    string  `json:"name"`
	Path    string  `json:"path"`
	SizeMiB float64 `json:"size_mib"`
}

type listOutput struct {
	Dir      string       `json:"dir"`
	Total    int          `json:"total"`
	TotalMiB float64      `json:"total_mib"`
	Entries  []cacheEntry `json:"entries"`
}

func newListCmd() *cobra.Command {
	var format string
	c := &cobra.Command{
		Use:   "list",
		Short: "List cached parsed applications + SHS zip files with sizes",
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := cacheDir()
			out := scanCache(dir)
			switch format {
			case "json":
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetEscapeHTML(false)
				return enc.Encode(out)
			case "", "text":
				renderListText(cmd.OutOrStdout(), out)
				return nil
			default:
				return fmt.Errorf("unknown --format %q (use text|json)", format)
			}
		},
	}
	c.Flags().StringVar(&format, "format", "", "Output format: text (default) | json")
	return c
}

// cacheDir 解析出当前生效的 cache 路径。SPARK_CLI_CACHE_DIR > internal/cache.DefaultDir。
// 这里不读 yaml(yaml 走 config.Load 链),保持 cache list 命令快速、独立。
func cacheDir() string {
	if v := os.Getenv("SPARK_CLI_CACHE_DIR"); v != "" {
		return v
	}
	return cache.DefaultDir()
}

func scanCache(dir string) listOutput {
	out := listOutput{Dir: dir, Entries: []cacheEntry{}}
	if _, err := os.Stat(dir); err != nil {
		return out
	}
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		kind := "application"
		if strings.Contains(path, string(filepath.Separator)+"shs"+string(filepath.Separator)) {
			kind = "shs_zip"
		}
		mib := float64(info.Size()) / (1024 * 1024)
		out.Entries = append(out.Entries, cacheEntry{
			Kind:    kind,
			Name:    d.Name(),
			Path:    path,
			SizeMiB: round3(mib),
		})
		out.TotalMiB += mib
		return nil
	})
	out.Total = len(out.Entries)
	out.TotalMiB = round3(out.TotalMiB)
	sort.Slice(out.Entries, func(i, j int) bool {
		return out.Entries[i].SizeMiB > out.Entries[j].SizeMiB
	})
	return out
}

func renderListText(w io.Writer, out listOutput) {
	fmt.Fprintf(w, "cache_dir: %s\n", out.Dir)
	fmt.Fprintf(w, "total: %d entries · %.2f MiB\n\n", out.Total, out.TotalMiB)
	if out.Total == 0 {
		return
	}
	fmt.Fprintf(w, "%-12s  %-40s  %s\n", "kind", "name", "size_mib")
	fmt.Fprintf(w, "%-12s  %-40s  %s\n", strings.Repeat("-", 12), strings.Repeat("-", 40), strings.Repeat("-", 8))
	for _, e := range out.Entries {
		fmt.Fprintf(w, "%-12s  %-40s  %.2f\n", e.Kind, e.Name, e.SizeMiB)
	}
}

func round3(f float64) float64 {
	x := f * 1000
	if x < 0 {
		x -= 0.5
	} else {
		x += 0.5
	}
	return float64(int64(x)) / 1000
}
