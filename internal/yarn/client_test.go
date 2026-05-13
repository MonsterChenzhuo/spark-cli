package yarn

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFetchApplicationLogsBuildsContainerLogURLsBehindGateway(t *testing.T) {
	const appID = "application_1772605260987_20682"
	mux := http.NewServeMux()
	mux.HandleFunc("/gateway/hadoop-prod/yarn/ws/v1/cluster/apps/"+appID, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"app": map[string]any{
			"id":          appID,
			"user":        "airflow",
			"state":       "FINISHED",
			"finalStatus": "FAILED",
			"diagnostics": "User class threw exception",
		}})
	})
	mux.HandleFunc("/gateway/hadoop-prod/yarn/ws/v1/cluster/apps/"+appID+"/appattempts", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"appAttempts": map[string]any{"appAttempt": []map[string]any{{
			"id": "appattempt_1772605260987_20682_000001",
		}}}})
	})
	mux.HandleFunc("/gateway/hadoop-prod/yarn/ws/v1/cluster/apps/"+appID+"/appattempts/appattempt_1772605260987_20682_000001/containers", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"containers": map[string]any{"container": []map[string]any{{
			"id":              "container_e07_1772605260987_20682_01_000001",
			"nodeHttpAddress": "10.166.23.223:8142",
			"logUrl":          "",
		}}}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	got, err := NewClient([]string{srv.URL + "/gateway/hadoop-prod/yarn"}, 5*time.Second).FetchApplicationLogs(context.Background(), appID, Options{TopContainers: 5, MaxLogBytes: 128})
	if err != nil {
		t.Fatalf("FetchApplicationLogs: %v", err)
	}
	if got.App.User != "airflow" || got.App.FinalStatus != "FAILED" {
		t.Fatalf("app summary = %+v", got.App)
	}
	if len(got.Containers) != 1 {
		t.Fatalf("containers len=%d", len(got.Containers))
	}
	url := got.Containers[0].LogURL
	if !strings.Contains(url, "/gateway/hadoop-prod/yarn/nodemanager/node/containerlogs/container_e07_1772605260987_20682_01_000001/airflow") {
		t.Fatalf("log URL %q missing gateway containerlogs path", url)
	}
	for _, want := range []string{"scheme=http", "host=10.166.23.223", "port=8142"} {
		if !strings.Contains(url, want) {
			t.Fatalf("log URL %q missing %s", url, want)
		}
	}
}

func TestFetchApplicationLogsIncludesSmallLogSnippets(t *testing.T) {
	const appID = "application_1_2"
	mux := http.NewServeMux()
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"app": map[string]any{"id": appID, "user": "alice"}})
	})
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID+"/appattempts", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"appAttempts": map[string]any{"appAttempt": []map[string]any{{"id": "attempt_1"}}}})
	})
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID+"/appattempts/attempt_1/containers", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"containers": map[string]any{"container": []map[string]any{{"id": "container_1", "nodeHttpAddress": "node:8042"}}}})
	})
	mux.HandleFunc("/yarn/nodemanager/node/containerlogs/container_1/alice/stderr", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("line one\nCaused by: boom\n"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	got, err := NewClient([]string{srv.URL + "/yarn"}, 5*time.Second).FetchApplicationLogs(context.Background(), appID, Options{
		TopContainers: 1,
		LogTypes:      []string{"stderr"},
		MaxLogBytes:   128,
	})
	if err != nil {
		t.Fatalf("FetchApplicationLogs: %v", err)
	}
	if got.Containers[0].Logs["stderr"] == "" || !strings.Contains(got.Containers[0].Logs["stderr"], "Caused by: boom") {
		t.Fatalf("stderr snippet missing: %+v", got.Containers[0].Logs)
	}
}

func TestFetchApplicationLogsAcceptsNumericAttemptID(t *testing.T) {
	const appID = "application_1772605260987_20765"
	mux := http.NewServeMux()
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"app": map[string]any{"id": appID, "user": "airflow"}})
	})
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID+"/appattempts", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"appAttempts": map[string]any{"appAttempt": []map[string]any{{"id": 1}}}})
	})
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID+"/appattempts/1/containers", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"containers": map[string]any{"container": []map[string]any{{"id": "container_1", "nodeHttpAddress": "node:8042"}}}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	got, err := NewClient([]string{srv.URL + "/yarn"}, 5*time.Second).FetchApplicationLogs(context.Background(), appID, Options{TopContainers: 1})
	if err != nil {
		t.Fatalf("FetchApplicationLogs: %v", err)
	}
	if len(got.Containers) != 1 || got.Containers[0].ID != "container_1" {
		t.Fatalf("containers = %+v", got.Containers)
	}
}

func TestFetchThreadDumpUsesYARNTrackingURL(t *testing.T) {
	const appID = "application_1_3"
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"app": map[string]any{
			"id":          appID,
			"user":        "alice",
			"trackingUrl": srv.URL + "/yarn/proxy/" + appID + "/",
		}})
	})
	mux.HandleFunc("/yarn/proxy/"+appID+"/api/v1/applications/"+appID+"/executors/driver/threads", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, []map[string]any{{
			"threadId":    1,
			"threadName":  "main",
			"threadState": "RUNNABLE",
			"stackTrace": []map[string]any{{
				"className":  "org.apache.spark.sql.execution.QueryExecution",
				"methodName": "optimizedPlan",
				"fileName":   "QueryExecution.scala",
				"lineNumber": 183,
			}},
		}})
	})

	got, err := NewClient([]string{srv.URL + "/yarn"}, 5*time.Second).FetchThreadDump(context.Background(), appID, "driver")
	if err != nil {
		t.Fatalf("FetchThreadDump: %v", err)
	}
	if got.ThreadCount != 1 || got.StateCounts["RUNNABLE"] != 1 {
		t.Fatalf("thread summary = %+v", got)
	}
	if got.Threads[0].ThreadName != "main" || got.Threads[0].StackTrace == nil {
		t.Fatalf("thread details = %+v", got.Threads)
	}
	if got.MainThread == nil || got.MainThread.ThreadName != "main" {
		t.Fatalf("main thread summary = %+v", got.MainThread)
	}
	if got.Diagnosis == nil || got.Diagnosis.Category != "spark_sql_planning" {
		t.Fatalf("diagnosis = %+v", got.Diagnosis)
	}
}

func TestFetchThreadDumpSummarizesExecutorCodegen(t *testing.T) {
	const appID = "application_1_4"
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"app": map[string]any{
			"id":          appID,
			"user":        "alice",
			"trackingUrl": srv.URL + "/yarn/proxy/" + appID + "/",
		}})
	})
	mux.HandleFunc("/yarn/proxy/"+appID+"/api/v1/applications/"+appID+"/executors/40/threads", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, []map[string]any{{
			"threadId":    99,
			"threadName":  "Executor task launch worker for task 555.0 in stage 3.0",
			"threadState": "RUNNABLE",
			"stackTrace": map[string]any{"elems": []string{
				"org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection$.create(GenerateUnsafeProjection.scala:338)",
				"org.apache.spark.sql.execution.ProjectExec.$anonfun$doExecute$1(basicPhysicalOperators.scala:95)",
				"org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)",
			}},
		}})
	})

	got, err := NewClient([]string{srv.URL + "/yarn"}, 5*time.Second).FetchThreadDump(context.Background(), appID, "40")
	if err != nil {
		t.Fatalf("FetchThreadDump: %v", err)
	}
	if got.Diagnosis == nil || got.Diagnosis.Category != "executor_projection_codegen" {
		t.Fatalf("diagnosis = %+v", got.Diagnosis)
	}
	if len(got.InterestingThreads) != 1 {
		t.Fatalf("interesting threads = %+v", got.InterestingThreads)
	}
	if !hasTestString(got.InterestingThreads[0].Tags, "spark_codegen") || !hasTestString(got.InterestingThreads[0].Tags, "shuffle_write") {
		t.Fatalf("tags = %+v", got.InterestingThreads[0].Tags)
	}
}

func TestFetchThreadDumpClassifiesCollapseProject(t *testing.T) {
	const appID = "application_1_6"
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"app": map[string]any{
			"id":          appID,
			"user":        "alice",
			"trackingUrl": srv.URL + "/yarn/proxy/" + appID + "/",
		}})
	})
	mux.HandleFunc("/yarn/proxy/"+appID+"/api/v1/applications/"+appID+"/executors/driver/threads", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, []map[string]any{{
			"threadId":    1,
			"threadName":  "main",
			"threadState": "RUNNABLE",
			"stackTrace": map[string]any{"elems": []string{
				"org.apache.spark.sql.catalyst.optimizer.CollapseProject$.canCollapseExpressions(Optimizer.scala:1047)",
				"org.apache.spark.sql.catalyst.expressions.AttributeSet.contains(AttributeSet.scala:83)",
			}},
		}})
	})

	got, err := NewClient([]string{srv.URL + "/yarn"}, 5*time.Second).FetchThreadDump(context.Background(), appID, "driver")
	if err != nil {
		t.Fatalf("FetchThreadDump: %v", err)
	}
	if got.Diagnosis == nil || got.Diagnosis.Category != "spark_collapse_project" {
		t.Fatalf("diagnosis = %+v", got.Diagnosis)
	}
	if got.MainThread == nil || !hasTestString(got.MainThread.Tags, "spark_collapse_project") {
		t.Fatalf("main thread tags = %+v", got.MainThread)
	}
}

func TestSparkUIBaseURLFallsBackWhenTrackingURLIsYARNAppPage(t *testing.T) {
	got := sparkUIBaseURL("http://gateway/yarn", "application_1_5", "http://gateway/yarn/cluster/app/application_1_5")
	want := "http://gateway/yarn/proxy/application_1_5"
	if got != want {
		t.Fatalf("sparkUIBaseURL = %q want %q", got, want)
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatal(err)
	}
}

func hasTestString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
