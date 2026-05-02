package cachecmd

import (
	"fmt"
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
			dir := cacheDir()
			if _, err := os.Stat(dir); err != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "cache dir %s does not exist; nothing to clear\n", dir)
				return nil
			}
			removed, freedMiB, err := clearCache(dir, appID, dryRun, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			verb := "removed"
			if dryRun {
				verb = "would remove"
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s %d entries · %.2f MiB freed\n", verb, removed, freedMiB)
			return nil
		},
	}
	c.Flags().StringVar(&appID, "app", "", "Only remove entries matching this appId (substring match)")
	c.Flags().BoolVar(&dryRun, "dry-run", false, "Print what would be removed without deleting")
	return c
}

// clearCache 遍历 dir,按 appID 过滤删除候选。dryRun=true 时只打印不删。
// 返回 (removed_count, freed_MiB, error)。错误一律收集但不中断遍历(尽量 best-effort)。
func clearCache(dir, appID string, dryRun bool, out interface {
	Write(p []byte) (n int, err error)
}) (int, float64, error) {
	var removedCount int
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
		fmt.Fprintf(out, "  %s (%.2f MiB)\n", p, float64(info.Size())/(1024*1024))
		if !dryRun {
			if err := os.Remove(p); err != nil {
				fmt.Fprintf(out, "    warn: %v\n", err)
				continue
			}
		}
		removedCount++
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
	return removedCount, round3(float64(freedBytes) / (1024 * 1024)), nil
}
