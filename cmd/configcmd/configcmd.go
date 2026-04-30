// Package configcmd provides `spark-cli config init` and `spark-cli config show`.
package configcmd

import "github.com/spf13/cobra"

func New() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage spark-cli configuration",
	}
	cmd.AddCommand(newInitCmd())
	cmd.AddCommand(newShowCmd())
	return cmd
}
