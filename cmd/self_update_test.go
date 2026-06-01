package cmd

import (
	"bytes"
	"encoding/json"
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
	var got struct {
		Command string `json:"command"`
		DryRun  bool   `json:"dry_run"`
		Asset   string `json:"asset"`
		Target  string `json:"target"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("stdout should be json: %v\n%s", err, buf.String())
	}
	if got.Command != "self-update" || !got.DryRun || got.Asset != "spark-cli_0.2.0_linux_amd64.tar.gz" || got.Target == "" {
		t.Fatalf("unexpected output: %+v", got)
	}
}
