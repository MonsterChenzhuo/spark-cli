// Package cmd wires the cobra command tree.
package cmd

import (
	"context"
	stderrors "errors"
	"io"
	"os"

	"github.com/spf13/cobra"

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
		Short: "Diagnose Spark applications by parsing EventLog — designed for Claude Code.",
		Long:  `spark-cli locates a Spark EventLog by applicationId (local or HDFS), parses it once, and emits structured JSON for AI agents.`,
		// Version 让 `spark-cli --version` 自动 work(原本只有 version subcommand);
		// 跟 version subcommand 用同一字符串避免漂移。
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	// 默认 template "<name> version <ver>\n",改成跟 version subcommand 一致的
	// "spark-cli <ver>\n",免得 --version 与 version 输出格式不同。
	root.SetVersionTemplate("spark-cli {{.Version}}\n")
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

// ResetForTest clears global state between table-driven tests.
func ResetForTest() { scenarios.ResetForTest() }

// ExitCode returns the most recent scenario exit code.
func ExitCode() int { return scenarios.ExitCode() }
