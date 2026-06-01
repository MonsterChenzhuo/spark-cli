// Package cmd wires the cobra command tree.
package cmd

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/opay-bigdata/spark-cli/cmd/cachecmd"
	"github.com/opay-bigdata/spark-cli/cmd/configcmd"
	"github.com/opay-bigdata/spark-cli/cmd/scenarios"
	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
)

var version = "dev"

func init() {
	// 把 ldflag 注入的 version 同步给 scenarios 包,SHS HTTP User-Agent 用它拼
	// "spark-cli/<version>" 让 SHS 运维能从访问日志识别流量来源。
	scenarios.CLIVersion = version
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "spark-cli",
		Short: "Diagnose Spark applications by parsing EventLog for AI agents.",
		Long:  `spark-cli locates a Spark EventLog by applicationId (local or HDFS), parses it once, and emits structured JSON for AI agents.`,
		// Version 让 `spark-cli --version` 自动 work(原本只有 version subcommand);
		// 跟 version subcommand 用同一字符串避免漂移。
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.CompletionOptions.DisableDefaultCmd = true
	// 默认 template "<name> version <ver>\n",改成跟 version subcommand 一致的
	// JSON 对象,保证 --version 也满足 AI-only stdout 契约。
	root.SetVersionTemplate("{\"command\":\"version\",\"version\":\"{{.Version}}\"}\n")
	return root
}

func buildRoot() *cobra.Command {
	root := newRootCmd()
	scenarios.Register(root)
	root.AddCommand(newVersionCmd())
	root.AddCommand(newSelfUpdateCmd())
	root.AddCommand(configcmd.New())
	root.AddCommand(cachecmd.New())
	configureJSONHelp(root)
	return root
}

// Execute runs the root command using os stdio and returns the process exit code.
func Execute() int {
	if rc := rejectCompletion(os.Args[1:], os.Stderr); rc != cerrors.ExitOK {
		return rc
	}
	root := buildRoot()
	if err := root.ExecuteContext(context.Background()); err != nil {
		err = wrapCobraError(err)
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
	if rc := rejectCompletion(args, stderr); rc != cerrors.ExitOK {
		return rc
	}
	root := buildRoot()
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	if err := root.ExecuteContext(ctx); err != nil {
		err = wrapCobraError(err)
		cerrors.WriteJSON(stderr, err)
		if rc := cerrors.ExitCode(err); rc != cerrors.ExitOK {
			return rc
		}
		return cerrors.ExitInternal
	}
	return scenarios.ExitCode()
}

// wrapCobraError 把 cobra 的"未知命令 / 未知 flag / required flag missing"等
// 错误包装成 cerrors.Error{FLAG_INVALID},归到 USER_ERR (rc=2),不再被默认
// 当成 INTERNAL (rc=1) 让用户翻文档以为撞了 bug。已经是 *cerrors.Error 的
// 错误(scenarios runner / parseConfig 自己抛的)原样返回。
func wrapCobraError(err error) error {
	var ce *cerrors.Error
	if stderrors.As(err, &ce) {
		return err
	}
	return cerrors.New(cerrors.CodeFlagInvalid, err.Error(), "see `spark-cli --help`")
}

func rejectCompletion(args []string, stderr io.Writer) int {
	for _, arg := range args {
		switch arg {
		case "__complete", "__completeNoDesc":
			cerrors.WriteJSON(stderr, cerrors.New(cerrors.CodeFlagInvalid, "shell completion is disabled", "use `spark-cli --help` for JSON command metadata"))
			return cerrors.ExitUserErr
		}
	}
	return cerrors.ExitOK
}

type helpResponse struct {
	Command  string        `json:"command"`
	Name     string        `json:"name"`
	Use      string        `json:"use"`
	Short    string        `json:"short,omitempty"`
	Long     string        `json:"long,omitempty"`
	Commands []helpCommand `json:"commands,omitempty"`
	Flags    []helpFlag    `json:"flags,omitempty"`
}

type helpCommand struct {
	Name  string `json:"name"`
	Use   string `json:"use"`
	Short string `json:"short,omitempty"`
}

type helpFlag struct {
	Name      string `json:"name"`
	Shorthand string `json:"shorthand,omitempty"`
	Usage     string `json:"usage,omitempty"`
	Default   string `json:"default,omitempty"`
}

func configureJSONHelp(root *cobra.Command) {
	helpFunc := func(cmd *cobra.Command, _ []string) {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetEscapeHTML(false)
		_ = enc.Encode(buildHelpResponse(cmd))
	}
	var walk func(*cobra.Command)
	walk = func(cmd *cobra.Command) {
		cmd.SetHelpFunc(helpFunc)
		for _, child := range cmd.Commands() {
			walk(child)
		}
	}
	walk(root)
}

func buildHelpResponse(cmd *cobra.Command) helpResponse {
	return helpResponse{
		Command:  "help",
		Name:     cmd.Name(),
		Use:      cmd.UseLine(),
		Short:    cmd.Short,
		Long:     cmd.Long,
		Commands: helpCommands(cmd),
		Flags:    helpFlags(cmd),
	}
}

func helpCommands(cmd *cobra.Command) []helpCommand {
	children := make([]helpCommand, 0, len(cmd.Commands()))
	for _, child := range cmd.Commands() {
		if !child.IsAvailableCommand() && child.Name() != "help" {
			continue
		}
		if child.Hidden || child.Name() == "completion" {
			continue
		}
		children = append(children, helpCommand{
			Name:  child.Name(),
			Use:   child.UseLine(),
			Short: child.Short,
		})
	}
	return children
}

func helpFlags(cmd *cobra.Command) []helpFlag {
	seen := make(map[string]struct{})
	flags := make([]helpFlag, 0)
	add := func(fs *pflag.FlagSet) {
		if fs == nil {
			return
		}
		fs.VisitAll(func(flag *pflag.Flag) {
			if flag.Hidden {
				return
			}
			if _, ok := seen[flag.Name]; ok {
				return
			}
			seen[flag.Name] = struct{}{}
			flags = append(flags, helpFlag{
				Name:      flag.Name,
				Shorthand: flag.Shorthand,
				Usage:     flag.Usage,
				Default:   flag.DefValue,
			})
		})
	}
	add(cmd.Flags())
	add(cmd.InheritedFlags())
	return flags
}

// ResetForTest clears global state between table-driven tests.
func ResetForTest() { scenarios.ResetForTest() }

// ExitCode returns the most recent scenario exit code.
func ExitCode() int { return scenarios.ExitCode() }
