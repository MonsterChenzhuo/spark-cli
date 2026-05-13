package scenarios

import (
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type globalState struct {
	LogDirs       string
	YARNBaseURLs  string
	YARNLogBytes  int64
	HDFSUser      string
	HadoopConfDir string
	CacheDir      string
	NoCache       bool
	SHSTimeout    string
	SQLDetail     string
	Timeout       string
	Format        string
	Top           int
	DryRun        bool
	NoProgress    bool
	ExitCode      int
}

// CLIVersion 由 cmd 包在 init 时注入(走 -X github.com/.../cmd.version 链)。
// scenarios 包用它给 SHS HTTP User-Agent 拼成 "spark-cli/<version>",让 SHS
// 运维能从访问日志识别流量来源。空 / "dev" 时不影响正常诊断。
var CLIVersion = "dev"

var state = defaultState()

func defaultState() globalState {
	return globalState{Format: "json", Top: 10, SQLDetail: "truncate", YARNLogBytes: 65536}
}

// ExitCode reports the most recent scenario exit code.
func ExitCode() int { return state.ExitCode }

// SetExitCode records the scenario exit code without aborting cobra.
func SetExitCode(rc int) { state.ExitCode = rc }

// ResetForTest restores defaults so tests can reuse the package-level root.
func ResetForTest() { state = defaultState() }

// RegisterFlags binds the persistent flags spark-cli scenarios need on root.
func RegisterFlags(root *cobra.Command) {
	root.PersistentFlags().StringVar(&state.LogDirs, "log-dirs", state.LogDirs, "Comma-separated EventLog sources: file:///path | hdfs://nn:port/path | shs://host:port (Spark History Server REST endpoint)")
	root.PersistentFlags().StringVar(&state.YARNBaseURLs, "yarn-base-urls", state.YARNBaseURLs, "Comma-separated YARN RM/gateway base URLs, e.g. http://rm:8088 or http://host/gateway/prod/yarn")
	root.PersistentFlags().Int64Var(&state.YARNLogBytes, "yarn-log-bytes", state.YARNLogBytes, "Max bytes per YARN container log type for yarn-logs; diagnose only emits log URLs")
	root.PersistentFlags().StringVar(&state.HDFSUser, "hdfs-user", state.HDFSUser, "HDFS simple-auth user (defaults to $USER)")
	root.PersistentFlags().StringVar(&state.HadoopConfDir, "hadoop-conf-dir", state.HadoopConfDir, "Path to Hadoop XML config dir; falls back to HADOOP_CONF_DIR / HADOOP_HOME")
	root.PersistentFlags().StringVar(&state.Timeout, "timeout", state.Timeout, "Overall parse timeout, e.g. 30s")
	root.PersistentFlags().StringVar(&state.Format, "format", state.Format, "Output format: json | table | markdown")
	root.PersistentFlags().IntVar(&state.Top, "top", state.Top, "Top N for ranked scenarios")
	root.PersistentFlags().BoolVar(&state.DryRun, "dry-run", state.DryRun, "Locate file only; do not parse events")
	root.PersistentFlags().BoolVar(&state.NoProgress, "no-progress", state.NoProgress, "Force-silence SHS progress lines (overrides SPARK_CLI_QUIET env and stdout-TTY auto-detect)")
	root.PersistentFlags().StringVar(&state.CacheDir, "cache-dir", state.CacheDir, "Directory for the parsed-application cache (defaults to $XDG_CACHE_HOME/spark-cli or ~/.cache/spark-cli)")
	root.PersistentFlags().BoolVar(&state.NoCache, "no-cache", state.NoCache, "Bypass the parsed-application cache for this invocation (do not read or write)")
	root.PersistentFlags().StringVar(&state.SHSTimeout, "shs-timeout", state.SHSTimeout, "HTTP timeout for shs:// requests (default 5m;生产 zip 几 GB 是常态,设小了会撞墙)")
	root.PersistentFlags().StringVar(&state.SQLDetail, "sql-detail", state.SQLDetail, "SQL description detail in sql_executions: truncate(default ~500 runes) | full | none")
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
