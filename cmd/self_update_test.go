package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func TestSelfUpdateDryRunPrintsTargetAsset(t *testing.T) {
	root := newRootCmd()
	root.AddCommand(newSelfUpdateCmd())
	root.SetArgs([]string{"self-update", "--version", "v0.2.0", "--os", "linux", "--arch", "amd64", "--dry-run"})
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)

	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\n%s", err, buf.String())
	}
	out := buf.String()
	if !strings.Contains(out, "spark-cli_0.2.0_linux_amd64.tar.gz") || !strings.Contains(out, "dry-run") {
		t.Fatalf("unexpected output: %q", out)
	}
}
