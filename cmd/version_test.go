package cmd

import (
	"bytes"
	"strings"
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
	got := buf.String()
	if !strings.Contains(got, "spark-cli") {
		t.Fatalf("missing program name; got %q", got)
	}
	if !strings.Contains(got, version) {
		t.Fatalf("missing version %q in %q", version, got)
	}
}
