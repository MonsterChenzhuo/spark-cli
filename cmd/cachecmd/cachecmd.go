// Package cachecmd provides `spark-cli cache list` / `cache clear` so users
// can inspect and prune the parsed-application + SHS zip caches without
// resorting to `rm -rf ~/.cache/spark-cli`.
package cachecmd

import (
	"github.com/spf13/cobra"
)

func New() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cache",
		Short: "Manage spark-cli's parsed-application and SHS zip caches",
	}
	cmd.AddCommand(newListCmd())
	cmd.AddCommand(newClearCmd())
	return cmd
}
