package scenarios

import (
	"os"
	"testing"
)

func TestResolveQuietNoProgressFlagWins(t *testing.T) {
	t.Setenv("SPARK_CLI_QUIET", "0")
	if !resolveQuiet(true, true) {
		t.Errorf("--no-progress should force quiet even with TTY + SPARK_CLI_QUIET=0")
	}
}

func TestResolveQuietEnv1ForcesQuiet(t *testing.T) {
	t.Setenv("SPARK_CLI_QUIET", "1")
	if !resolveQuiet(false, true) {
		t.Errorf("SPARK_CLI_QUIET=1 should force quiet even on TTY")
	}
}

func TestResolveQuietEnv0ForcesProgress(t *testing.T) {
	t.Setenv("SPARK_CLI_QUIET", "0")
	if resolveQuiet(false, false) {
		t.Errorf("SPARK_CLI_QUIET=0 should override non-TTY default and keep progress")
	}
}

func TestResolveQuietEnvTrueAndFalse(t *testing.T) {
	t.Setenv("SPARK_CLI_QUIET", "true")
	if !resolveQuiet(false, true) {
		t.Errorf("SPARK_CLI_QUIET=true should force quiet")
	}
	t.Setenv("SPARK_CLI_QUIET", "false")
	if resolveQuiet(false, false) {
		t.Errorf("SPARK_CLI_QUIET=false should disable quiet even off-TTY")
	}
}

func TestResolveQuietDefaultsByTTY(t *testing.T) {
	// 显式 unset env:t.Setenv 在 cleanup 时会自动恢复
	_ = os.Unsetenv("SPARK_CLI_QUIET")
	t.Cleanup(func() { _ = os.Unsetenv("SPARK_CLI_QUIET") })
	if !resolveQuiet(false, false) {
		t.Errorf("non-TTY without env should be quiet by default")
	}
	if resolveQuiet(false, true) {
		t.Errorf("TTY without env should NOT be quiet by default")
	}
}

func TestResolveQuietEmptyEnvIsUnset(t *testing.T) {
	// 空字符串环境变量(SPARK_CLI_QUIET=)应当视为未设,走 TTY 检测分支
	t.Setenv("SPARK_CLI_QUIET", "")
	if resolveQuiet(false, true) {
		t.Errorf("empty SPARK_CLI_QUIET on TTY should NOT force quiet")
	}
	if !resolveQuiet(false, false) {
		t.Errorf("empty SPARK_CLI_QUIET off-TTY should fall through to non-TTY default (quiet)")
	}
}
