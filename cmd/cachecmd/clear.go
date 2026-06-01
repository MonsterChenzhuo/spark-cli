package cachecmd

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

func newClearCmd() *cobra.Command {
	var (
		appID  string
		dryRun bool
	)
	c := &cobra.Command{
		Use:   "clear",
		Short: "Remove cached parsed-application + SHS zip files",
		Long: `Without --app, removes everything under the cache dir (application gob.zst
files and SHS zip files). With --app <id>, removes only entries whose path
contains <id> as a substring (matches both application_<id>.gob.zst and
SHS zip <appID>_<lastUpdated>.zip).

Use --dry-run to print what would be removed without actually deleting.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := resolveCacheDir(cmd)
			if _, err := os.Stat(dir); err != nil {
				return writeClearOutput(cmd, clearOutput{
					Command:  "cache clear",
					CacheDir: dir,
					AppID:    appID,
					DryRun:   dryRun,
					Entries:  []clearEntry{},
				})
			}
			out, err := clearCache(dir, appID, dryRun)
			if err != nil {
				return err
			}
			return writeClearOutput(cmd, out)
		},
	}
	c.Flags().StringVar(&appID, "app", "", "Only remove entries matching this appId (substring match)")
	c.Flags().BoolVar(&dryRun, "dry-run", false, "Print what would be removed without deleting")
	return c
}

type clearOutput struct {
	Command  string       `json:"command"`
	CacheDir string       `json:"cache_dir"`
	AppID    string       `json:"app_id,omitempty"`
	DryRun   bool         `json:"dry_run"`
	Removed  int          `json:"removed"`
	FreedMiB float64      `json:"freed_mib"`
	Entries  []clearEntry `json:"entries"`
}

type clearEntry struct {
	Path    string  `json:"path"`
	SizeMiB float64 `json:"size_mib"`
	Removed bool    `json:"removed"`
	Error   string  `json:"error,omitempty"`
}

func writeClearOutput(cmd *cobra.Command, out clearOutput) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetEscapeHTML(false)
	return enc.Encode(out)
}

// clearCache 遍历 dir,按 appID 过滤删除候选。dryRun=true 时只返回候选不删除。
// 单个文件删除失败写入 entry.error,不中断遍历。
func clearCache(dir, appID string, dryRun bool) (clearOutput, error) {
	out := clearOutput{
		Command:  "cache clear",
		CacheDir: dir,
		AppID:    appID,
		DryRun:   dryRun,
		Entries:  []clearEntry{},
	}
	var freedBytes int64
	var paths []string
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if appID != "" && !strings.Contains(d.Name(), appID) {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			continue
		}
		entry := clearEntry{
			Path:    p,
			SizeMiB: round3(float64(info.Size()) / (1024 * 1024)),
			Removed: false,
		}
		if !dryRun {
			if err := os.Remove(p); err != nil {
				entry.Removed = false
				entry.Error = err.Error()
				out.Entries = append(out.Entries, entry)
				continue
			}
			entry.Removed = true
		}
		out.Entries = append(out.Entries, entry)
		out.Removed++
		freedBytes += info.Size()
	}
	// sweep 空目录(只在非 dry-run 且没指定 appID 时;有 --app 时保留可能含其他 entries 的目录)
	if !dryRun && appID == "" {
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || !d.IsDir() || path == dir {
				return nil
			}
			entries, _ := os.ReadDir(path)
			if len(entries) == 0 {
				_ = os.Remove(path)
			}
			return nil
		})
	}
	out.FreedMiB = round3(float64(freedBytes) / (1024 * 1024))
	return out, nil
}
