package scenarios

import (
	"github.com/spf13/cobra"
)

// Register attaches the persistent flags and 5 scenario commands to root.
func Register(root *cobra.Command) {
	RegisterFlags(root)
	root.AddCommand(
		newScenarioCmd("app-summary", "Application-level overview"),
		newScenarioCmd("slow-stages", "Stages ranked by wall time"),
		newScenarioCmd("data-skew", "Stages with task long-tail / input skew"),
		newScenarioCmd("gc-pressure", "Executors ranked by GC ratio"),
		newScenarioCmd("diagnose", "Run all rules and emit findings"),
	)
}

func newScenarioCmd(name, short string) *cobra.Command {
	scenario := name
	c := &cobra.Command{
		Use:   name + " <appId>",
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cc *cobra.Command, args []string) error {
			rc := Run(cc.Context(), buildOpts(scenario, args[0], cc))
			SetExitCode(rc)
			return nil
		},
	}
	return c
}

func buildOpts(scenario, appID string, cc *cobra.Command) Options {
	return Options{
		Scenario: scenario,
		AppID:    appID,
		LogDirs:  splitLogDirs(state.LogDirs),
		HDFSUser: state.HDFSUser,
		Timeout:  parseTimeoutFlag(state.Timeout),
		Format:   state.Format,
		Top:      state.Top,
		DryRun:   state.DryRun,
		Stdout:   cc.OutOrStdout(),
		Stderr:   cc.ErrOrStderr(),
	}
}
