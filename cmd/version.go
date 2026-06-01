package cmd

import (
	"encoding/json"

	"github.com/spf13/cobra"
)

type versionResponse struct {
	Command string `json:"command"`
	Version string `json:"version"`
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print spark-cli version",
		RunE: func(cmd *cobra.Command, args []string) error {
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetEscapeHTML(false)
			return enc.Encode(versionResponse{Command: "version", Version: version})
		},
	}
}
