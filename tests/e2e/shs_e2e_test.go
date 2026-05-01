package e2e

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opay-bigdata/spark-cli/cmd"
)

// TestE2EShsAppSummary drives the full CLI through cmd.RunWith with
// `--log-dirs shs://<httptest-host>`, asserting that an app-summary scenario
// succeeds end-to-end against a fake Spark History Server.
func TestE2EShsAppSummary(t *testing.T) {
	tinyJSON, err := os.ReadFile(filepath.Join("..", "testdata", "tiny_app.json"))
	if err != nil {
		t.Fatal(err)
	}
	const appID = "application_1_1"

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create(appID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(tinyJSON); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	zipBody := buf.Bytes()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/applications/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/api/v1/applications/")
		parts := strings.Split(rest, "/")
		if parts[0] != appID {
			http.NotFound(w, r)
			return
		}
		switch len(parts) {
		case 1:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": appID,
				"attempts": []map[string]any{
					{"attemptId": "1", "lastUpdatedEpoch": 1000},
				},
			})
		case 3:
			if parts[2] != "logs" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/zip")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(zipBody)))
			_, _ = w.Write(zipBody)
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	hostURL, _ := url.Parse(srv.URL)

	cmd.ResetForTest()
	var stdout, stderr bytes.Buffer
	args := []string{
		"app-summary", appID,
		"--log-dirs", "shs://" + hostURL.Host,
		"--format", "json",
		"--no-cache",
	}
	rc := cmd.RunWith(context.Background(), args, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	var m map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &m); err != nil {
		t.Fatalf("not json: %v\n%s", err, stdout.String())
	}
	if got, _ := m["scenario"].(string); got != "app-summary" {
		t.Errorf("scenario = %q want app-summary", got)
	}
	if got, _ := m["log_format"].(string); got != "v1" {
		t.Errorf("log_format = %q want v1", got)
	}
	logPath, _ := m["log_path"].(string)
	if !strings.HasPrefix(logPath, "shs://"+hostURL.Host+"/"+appID+"/") {
		t.Errorf("log_path = %q does not point at shs://...", logPath)
	}
	if _, ok := m["data"]; !ok {
		t.Errorf("missing data:\n%s", stdout.String())
	}
}
