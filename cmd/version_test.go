package cmd

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestVersionCommandPrintsVersion(t *testing.T) {
	root := newRootCmd()
	root.AddCommand(newVersionCmd())
	root.SetArgs([]string{"version"})
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}
	var got struct {
		Command string `json:"command"`
		Version string `json:"version"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("stdout should be json: %v\n%s", err, buf.String())
	}
	if got.Command != "version" || got.Version != version {
		t.Fatalf("unexpected version response: %+v", got)
	}
}
