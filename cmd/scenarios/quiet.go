package scenarios

import (
	"os"
	"strings"
)

// resolveQuiet 决定 SHS 进度提示是否静默。优先级:
//
//  1. --no-progress flag 显式 → 永远静默
//  2. SPARK_CLI_QUIET 环境变量显式("1"/"true" 静默,"0"/"false" 不静默);空值视为未设
//  3. stdout 不是 TTY(管道、重定向、agent 调用)→ 静默
//  4. 否则交互终端 → 不静默
//
// 这个 helper 把"agent 自动静默"做成默认行为,免得 agent 每次都要 export
// SPARK_CLI_QUIET=1。交互终端继续看到下载进度。
func resolveQuiet(noProgressFlag bool, stdoutIsTerminal bool) bool {
	if noProgressFlag {
		return true
	}
	if v, ok := os.LookupEnv("SPARK_CLI_QUIET"); ok && v != "" {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "0", "false":
			return false
		default:
			return true
		}
	}
	return !stdoutIsTerminal
}

// stdoutIsTTY 通过文件 mode 判定 stdout 是否是字符设备(TTY)。不引入
// golang.org/x/term 依赖。管道、重定向、agent 嵌入调用时返回 false。
func stdoutIsTTY() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
