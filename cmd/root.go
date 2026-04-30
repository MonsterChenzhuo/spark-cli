// Package cmd wires the cobra command tree.
package cmd

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/opay-bigdata/spark-cli/cmd/configcmd"
	"github.com/opay-bigdata/spark-cli/cmd/scenarios"
	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
)

var version = "dev"

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "spark-cli",
		Short:         "Diagnose Spark applications by parsing EventLog — designed for Claude Code.",
		Long:          `spark-cli locates a Spark EventLog by applicationId (local or HDFS), parses it once, and emits structured JSON for AI agents.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	return root
}

func buildRoot() *cobra.Command {
	root := newRootCmd()
	scenarios.Register(root)
	root.AddCommand(newVersionCmd())
	root.AddCommand(configcmd.New())
	return root
}

// Execute runs the root command using os stdio and returns the process exit code.
func Execute() int {
	root := buildRoot()
	if err := root.ExecuteContext(context.Background()); err != nil {
		cerrors.WriteJSON(os.Stderr, err)
		if rc := cerrors.ExitCode(err); rc != cerrors.ExitOK {
			return rc
		}
		return cerrors.ExitInternal
	}
	return scenarios.ExitCode()
}

// RunWith executes the root with custom args/writers, used by E2E tests.
func RunWith(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	scenarios.ResetForTest()
	root := buildRoot()
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	if err := root.ExecuteContext(ctx); err != nil {
		cerrors.WriteJSON(stderr, err)
		if rc := cerrors.ExitCode(err); rc != cerrors.ExitOK {
			return rc
		}
		return cerrors.ExitInternal
	}
	return scenarios.ExitCode()
}

// ResetForTest clears global state between table-driven tests.
func ResetForTest() { scenarios.ResetForTest() }

// ExitCode returns the most recent scenario exit code.
func ExitCode() int { return scenarios.ExitCode() }
