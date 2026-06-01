package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGuidedDiagnoseFlagWiresThroughRootCommand(t *testing.T) {
	configDir := t.TempDir()
	t.Setenv("SPARK_CLI_CONFIG_DIR", configDir)
	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "application_1_14")
	if err := os.WriteFile(logPath, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := "clusters:\n  prod:\n    log_dirs:\n      - file://" + logDir + "\n"
	if err := os.WriteFile(filepath.Join(configDir, "config.yaml"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{
		"diagnose", "application_1_14", "--guided", "--dry-run",
	}, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	if !strings.Contains(stdout.String(), logPath) {
		t.Fatalf("stdout did not resolve through guided flag:\n%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), `selected only configured cluster "prod"`) {
		t.Fatalf("stderr missing guided selection note:\n%s", stderr.String())
	}
}

func TestHelpPrintsJSON(t *testing.T) {
	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"--help"}, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	var got struct {
		Command  string `json:"command"`
		Name     string `json:"name"`
		Commands []struct {
			Name string `json:"name"`
		} `json:"commands"`
		Flags []struct {
			Name string `json:"name"`
		} `json:"flags"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout should be json: %v\n%s", err, stdout.String())
	}
	if got.Command != "help" || got.Name != "spark-cli" {
		t.Fatalf("unexpected help response: %+v", got)
	}
	if !hasNamedCommand(got.Commands, "diagnose") {
		t.Fatalf("help commands missing diagnose: %+v", got.Commands)
	}
	if hasNamedCommand(got.Commands, "completion") {
		t.Fatalf("AI-only help should not expose shell completion: %+v", got.Commands)
	}
	if !hasNamedFlag(got.Flags, "format") {
		t.Fatalf("help flags missing format: %+v", got.Flags)
	}
}

func TestCompletionCommandsDisabled(t *testing.T) {
	for _, args := range [][]string{{"completion", "bash"}, {"__complete", ""}} {
		t.Run(args[0], func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			rc := RunWith(context.Background(), args, &stdout, &stderr)
			if rc != 2 {
				t.Fatalf("rc=%d want 2 stderr=%s stdout=%s", rc, stderr.String(), stdout.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout should be empty for disabled completion, got %s", stdout.String())
			}
			if !bytes.Contains(stderr.Bytes(), []byte(`"FLAG_INVALID"`)) {
				t.Fatalf("stderr missing FLAG_INVALID: %s", stderr.String())
			}
		})
	}
}

func hasNamedCommand(commands []struct {
	Name string `json:"name"`
}, name string) bool {
	for _, command := range commands {
		if command.Name == name {
			return true
		}
	}
	return false
}

func hasNamedFlag(flags []struct {
	Name string `json:"name"`
}, name string) bool {
	for _, flag := range flags {
		if flag.Name == name {
			return true
		}
	}
	return false
}
