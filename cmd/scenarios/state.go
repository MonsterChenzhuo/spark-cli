package scenarios

import (
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type globalState struct {
	LogDirs    string
	HDFSUser   string
	Timeout    string
	Format     string
	Top        int
	DryRun     bool
	NoProgress bool
	ExitCode   int
}

var state = defaultState()

func defaultState() globalState {
	return globalState{Format: "json", Top: 10}
}

// ExitCode reports the most recent scenario exit code.
func ExitCode() int { return state.ExitCode }

// SetExitCode records the scenario exit code without aborting cobra.
func SetExitCode(rc int) { state.ExitCode = rc }

// ResetForTest restores defaults so tests can reuse the package-level root.
func ResetForTest() { state = defaultState() }

// RegisterFlags binds the persistent flags spark-cli scenarios need on root.
func RegisterFlags(root *cobra.Command) {
	root.PersistentFlags().StringVar(&state.LogDirs, "log-dirs", state.LogDirs, "Comma-separated list of EventLog dirs (file:// or hdfs:// URIs)")
	root.PersistentFlags().StringVar(&state.HDFSUser, "hdfs-user", state.HDFSUser, "HDFS simple-auth user (defaults to $USER)")
	root.PersistentFlags().StringVar(&state.Timeout, "timeout", state.Timeout, "Overall parse timeout, e.g. 30s")
	root.PersistentFlags().StringVar(&state.Format, "format", state.Format, "Output format: json | table | markdown")
	root.PersistentFlags().IntVar(&state.Top, "top", state.Top, "Top N for ranked scenarios")
	root.PersistentFlags().BoolVar(&state.DryRun, "dry-run", state.DryRun, "Locate file only; do not parse events")
	root.PersistentFlags().BoolVar(&state.NoProgress, "no-progress", state.NoProgress, "Disable stderr progress")
}

func splitLogDirs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func parseTimeoutFlag(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}
