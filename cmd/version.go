package cmd

import (
	"github.com/spf13/cobra"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print spark-cli version",
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Printf("spark-cli %s\n", version)
			return nil
		},
	}
}
