package scenarios

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunnerDryRunEmitsLogSourceWithoutParsing(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "application_1_1")
	if err := os.WriteFile(logPath, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	rc := Run(context.Background(), Options{
		Scenario: "app-summary",
		AppID:    "application_1_1",
		LogDirs:  []string{"file://" + dir},
		Format:   "json",
		DryRun:   true,
		Stdout:   &stdout,
		Stderr:   &stderr,
	})
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"log_path"`) {
		t.Errorf("stdout missing log_path:\n%s", stdout.String())
	}
}

func TestRunnerYARNLogsDoesNotRequireSparkLogDirs(t *testing.T) {
	const appID = "application_1_2"
	mux := http.NewServeMux()
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID, func(w http.ResponseWriter, r *http.Request) {
		writeRunnerJSON(t, w, map[string]any{"app": map[string]any{"id": appID, "user": "airflow", "state": "FAILED"}})
	})
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID+"/appattempts", func(w http.ResponseWriter, r *http.Request) {
		writeRunnerJSON(t, w, map[string]any{"appAttempts": map[string]any{"appAttempt": []map[string]any{{"id": "attempt_1"}}}})
	})
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID+"/appattempts/attempt_1/containers", func(w http.ResponseWriter, r *http.Request) {
		writeRunnerJSON(t, w, map[string]any{"containers": map[string]any{"container": []map[string]any{{"id": "container_1", "nodeHttpAddress": "node:8042"}}}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var stdout, stderr bytes.Buffer
	rc := Run(context.Background(), Options{
		Scenario:     "yarn-logs",
		AppID:        appID,
		YARNBaseURLs: []string{srv.URL + "/yarn"},
		Format:       "json",
		Top:          1,
		Stdout:       &stdout,
		Stderr:       &stderr,
	})
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, `"scenario":"yarn-logs"`) || !strings.Contains(out, `"state":"FAILED"`) || !strings.Contains(out, `"container_1"`) {
		t.Fatalf("unexpected stdout:\n%s", out)
	}
}

func TestRunnerDriverThreadDumpDoesNotRequireSparkLogDirs(t *testing.T) {
	const appID = "application_1_3"
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID, func(w http.ResponseWriter, r *http.Request) {
		writeRunnerJSON(t, w, map[string]any{"app": map[string]any{
			"id":          appID,
			"user":        "airflow",
			"trackingUrl": srv.URL + "/yarn/proxy/" + appID + "/",
		}})
	})
	mux.HandleFunc("/yarn/proxy/"+appID+"/api/v1/applications/"+appID+"/executors/driver/threads", func(w http.ResponseWriter, r *http.Request) {
		writeRunnerJSON(t, w, []map[string]any{{
			"threadId":    1,
			"threadName":  "main",
			"threadState": "RUNNABLE",
		}})
	})

	var stdout, stderr bytes.Buffer
	rc := Run(context.Background(), Options{
		Scenario:     "driver-thread-dump",
		AppID:        appID,
		YARNBaseURLs: []string{srv.URL + "/yarn"},
		ExecutorID:   "driver",
		Format:       "json",
		Stdout:       &stdout,
		Stderr:       &stderr,
	})
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, `"scenario":"driver-thread-dump"`) || !strings.Contains(out, `"thread_count":1`) || !strings.Contains(out, `"threadName":"main"`) {
		t.Fatalf("unexpected stdout:\n%s", out)
	}
}

func TestRunnerDriverThreadDumpSummaryOnlyOmitsRawThreads(t *testing.T) {
	const appID = "application_1_4"
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID, func(w http.ResponseWriter, r *http.Request) {
		writeRunnerJSON(t, w, map[string]any{"app": map[string]any{
			"id":          appID,
			"user":        "airflow",
			"trackingUrl": srv.URL + "/yarn/proxy/" + appID + "/",
		}})
	})
	mux.HandleFunc("/yarn/proxy/"+appID+"/api/v1/applications/"+appID+"/executors/driver/threads", func(w http.ResponseWriter, r *http.Request) {
		writeRunnerJSON(t, w, []map[string]any{{
			"threadId":    1,
			"threadName":  "main",
			"threadState": "WAITING",
			"stackTrace": map[string]any{"elems": []string{
				"org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:967)",
				"org.apache.spark.rdd.RDD.collect(RDD.scala:1021)",
			}},
		}})
	})

	var stdout, stderr bytes.Buffer
	rc := Run(context.Background(), Options{
		Scenario:          "driver-thread-dump",
		AppID:             appID,
		YARNBaseURLs:      []string{srv.URL + "/yarn"},
		ExecutorID:        "driver",
		ThreadSummaryOnly: true,
		Format:            "json",
		Stdout:            &stdout,
		Stderr:            &stderr,
	})
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	var env map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("decode stdout: %v\n%s", err, stdout.String())
	}
	rows, ok := env["data"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("data = %#v", env["data"])
	}
	row, ok := rows[0].(map[string]any)
	if !ok {
		t.Fatalf("row = %#v", rows[0])
	}
	if _, ok := row["threads"]; ok {
		t.Fatalf("threads should be omitted in summary-only mode: %#v", row["threads"])
	}
	if row["diagnosis"] == nil || row["main_thread"] == nil || row["interesting_threads"] == nil {
		t.Fatalf("summary fields missing: %#v", row)
	}
}

func TestRunnerDriverThreadDumpReturnsEnvelopeWhenThreadEndpointFails(t *testing.T) {
	const appID = "application_1_5"
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID, func(w http.ResponseWriter, r *http.Request) {
		writeRunnerJSON(t, w, map[string]any{"app": map[string]any{
			"id":          appID,
			"user":        "airflow",
			"state":       "RUNNING",
			"trackingUrl": srv.URL + "/yarn/proxy/" + appID + "/",
		}})
	})
	mux.HandleFunc("/yarn/proxy/"+appID+"/api/v1/applications/"+appID+"/executors/driver/threads", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>proxy page</html>"))
	})

	var stdout, stderr bytes.Buffer
	rc := Run(context.Background(), Options{
		Scenario:          "driver-thread-dump",
		AppID:             appID,
		YARNBaseURLs:      []string{srv.URL + "/yarn"},
		ExecutorID:        "driver",
		ThreadSummaryOnly: true,
		Format:            "json",
		Stdout:            &stdout,
		Stderr:            &stderr,
	})
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, `"category":"spark_ui_thread_dump_unavailable"`) || !strings.Contains(out, `"warnings"`) {
		t.Fatalf("unexpected stdout:\n%s", out)
	}
}

func writeRunnerJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatal(err)
	}
}
