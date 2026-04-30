// Package cmd wires the cobra command tree.
package cmd

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/opay-bigdata/spark-cli/cmd/configcmd"
	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
)

var version = "dev"

type globalFlags struct {
	LogDirs    string
	HDFSUser   string
	Timeout    string
	Format     string
	Top        int
	DryRun     bool
	NoProgress bool
}

var globals globalFlags

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "spark-cli",
		Short:         "Diagnose Spark applications by parsing EventLog — designed for Claude Code.",
		Long:          `spark-cli locates a Spark EventLog by applicationId (local or HDFS), parses it once, and emits structured JSON for AI agents.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&globals.LogDirs, "log-dirs", "", "Comma-separated list of EventLog dirs (file:// or hdfs:// URIs)")
	root.PersistentFlags().StringVar(&globals.HDFSUser, "hdfs-user", "", "HDFS simple-auth user (defaults to $USER)")
	root.PersistentFlags().StringVar(&globals.Timeout, "timeout", "", "Overall parse timeout, e.g. 30s")
	root.PersistentFlags().StringVar(&globals.Format, "format", "json", "Output format: json | table | markdown")
	root.PersistentFlags().IntVar(&globals.Top, "top", 10, "Top N for ranked scenarios")
	root.PersistentFlags().BoolVar(&globals.DryRun, "dry-run", false, "Locate file only; do not parse events")
	root.PersistentFlags().BoolVar(&globals.NoProgress, "no-progress", false, "Disable stderr progress")
	return root
}

// Execute is the package entry point invoked by main.go.
func Execute() int {
	root := newRootCmd()
	root.AddCommand(newVersionCmd())
	root.AddCommand(configcmd.New())
	if err := root.Execute(); err != nil {
		cerrors.WriteJSON(os.Stderr, err)
		return cerrors.ExitCode(err)
	}
	return cerrors.ExitOK
}
