package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/opay-bigdata/spark-cli/cmd"
)

func TestE2E_AllScenarios_TinyApp(t *testing.T) {
	dir := t.TempDir()
	src, err := os.ReadFile(filepath.Join("..", "testdata", "tiny_app.json"))
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "application_1_1")
	if err := os.WriteFile(logPath, src, 0o644); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		args     []string
		wantKeys []string
	}{
		{"app-summary", []string{"app-summary", "application_1_1"}, []string{"scenario", "app_id", "data"}},
		{"slow-stages", []string{"slow-stages", "application_1_1", "--top", "5"}, []string{"scenario", "data"}},
		{"data-skew", []string{"data-skew", "application_1_1"}, []string{"scenario", "data"}},
		{"gc-pressure", []string{"gc-pressure", "application_1_1"}, []string{"scenario", "data"}},
		{"diagnose", []string{"diagnose", "application_1_1"}, []string{"scenario", "data", "summary"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd.ResetForTest()
			var stdout, stderr bytes.Buffer
			args := append(append([]string{}, tc.args...), "--log-dirs", "file://"+dir, "--format", "json")
			rc := cmd.RunWith(context.Background(), args, &stdout, &stderr)
			if rc != 0 {
				t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
			}
			var m map[string]any
			if err := json.Unmarshal(stdout.Bytes(), &m); err != nil {
				t.Fatalf("not json: %v\n%s", err, stdout.String())
			}
			for _, k := range tc.wantKeys {
				if _, ok := m[k]; !ok {
					t.Errorf("missing key %q in stdout:\n%s", k, stdout.String())
				}
			}
		})
	}
}

func TestE2E_AppNotFound(t *testing.T) {
	dir := t.TempDir()
	cmd.ResetForTest()
	var stdout, stderr bytes.Buffer
	rc := cmd.RunWith(context.Background(),
		[]string{"app-summary", "application_does_not_exist", "--log-dirs", "file://" + dir},
		&stdout, &stderr)
	if rc != 2 {
		t.Errorf("rc=%d want 2 stderr=%s", rc, stderr.String())
	}
	if !bytes.Contains(stderr.Bytes(), []byte(`"APP_NOT_FOUND"`)) {
		t.Errorf("stderr missing APP_NOT_FOUND:\n%s", stderr.String())
	}
}
