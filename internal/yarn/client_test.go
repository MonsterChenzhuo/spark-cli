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

func TestCanonicalAppID(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		// 带 attempt 后缀 (EventLog / SHS V2 文件名归一化产物) → 剥成裸 appID
		{"application_1765228031635_512717_1", "application_1765228031635_512717"},
		{"application_1765228031635_512717_2", "application_1765228031635_512717"},
		// 已是裸 appID → 原样
		{"application_1765228031635_512717", "application_1765228031635_512717"},
		// 多余后缀也一并剥掉,只保留前两段
		{"application_1765228031635_512717_1_extra", "application_1765228031635_512717"},
		// 前后空白
		{"  application_1765228031635_512717_1  ", "application_1765228031635_512717"},
		// 非 application_ 形态 → 原样
		{"weird_id", "weird_id"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := canonicalAppID(tc.in); got != tc.want {
			t.Errorf("canonicalAppID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestFetchApplicationLogsStripsAttemptSuffix(t *testing.T) {
	const bareAppID = "application_1772605260987_20682"
	var gotPaths []string
	mux := http.NewServeMux()
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+bareAppID, func(w http.ResponseWriter, r *http.Request) {
		gotPaths = append(gotPaths, r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{"app": map[string]any{"id": bareAppID, "state": "RUNNING"}})
	})
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+bareAppID+"/appattempts", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"appAttempts": map[string]any{"appAttempt": []any{}}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClient([]string{srv.URL + "/yarn"}, 5*time.Second)
	// 传入带 attempt 后缀的 appID,client 必须用裸 appID 调 YARN REST,否则 RM 返回 400。
	rep, err := c.FetchApplicationLogs(context.Background(), bareAppID+"_1", Options{})
	if err != nil {
		t.Fatalf("FetchApplicationLogs: %v", err)
	}
	if rep.App.ID != bareAppID {
		t.Errorf("app id = %q, want %q", rep.App.ID, bareAppID)
	}
	for _, p := range gotPaths {
		if strings.Contains(p, bareAppID+"_1") {
			t.Errorf("request path %q still contains attempt suffix", p)
		}
	}
}

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

func TestFetchApplicationLogsWithInsecureTLS(t *testing.T) {
	const appID = "application_tls_1"
	mux := http.NewServeMux()
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"app": map[string]any{"id": appID, "user": "alice"}})
	})
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID+"/appattempts", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"appAttempts": map[string]any{"appAttempt": []map[string]any{}}})
	})
	srv := httptest.NewTLSServer(mux)
	defer srv.Close()

	got, err := NewClientWithOptions(
		[]string{srv.URL + "/yarn"},
		5*time.Second,
		ClientOptions{InsecureSkipVerify: true},
	).FetchApplicationLogs(context.Background(), appID, Options{TopContainers: 1})
	if err != nil {
		t.Fatalf("FetchApplicationLogs via self-signed HTTPS gateway: %v", err)
	}
	if got.App.ID != appID || got.App.User != "alice" {
		t.Fatalf("app summary = %+v", got.App)
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
		http.Error(w, "numeric attempt id is invalid here", http.StatusBadRequest)
	})
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID+"/appattempts/appattempt_1772605260987_20765_000001/containers", func(w http.ResponseWriter, r *http.Request) {
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

func TestFetchApplicationLogsFallsBackToHTMLContainerLinks(t *testing.T) {
	const appID = "application_1772605260987_35693"
	mux := http.NewServeMux()
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"app": map[string]any{"id": appID, "user": "airflow"}})
	})
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID+"/appattempts", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"appAttempts": map[string]any{"appAttempt": []map[string]any{{"id": "appattempt_1772605260987_35693_000002"}}}})
	})
	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID+"/appattempts/appattempt_1772605260987_35693_000002/containers", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gateway rejects containers API", http.StatusBadRequest)
	})
	mux.HandleFunc("/yarn/cluster/app/"+appID, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><body>
			<a href="/yarn/nodemanager/node/containerlogs/container_e01_1772605260987_35693_02_000123/airflow?scheme=http&host=nm1&port=8042">logs</a>
		</body></html>`))
	})
	mux.HandleFunc("/yarn/nodemanager/node/containerlogs/container_e01_1772605260987_35693_02_000123/airflow/stderr", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("executor lost heartbeat\n"))
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
	if len(got.Containers) != 1 {
		t.Fatalf("containers = %+v warnings=%v", got.Containers, got.Warnings)
	}
	if got.Containers[0].ID != "container_e01_1772605260987_35693_02_000123" {
		t.Fatalf("container = %+v", got.Containers[0])
	}
	if !strings.Contains(got.Containers[0].Logs["stderr"], "heartbeat") {
		t.Fatalf("stderr snippet missing: %+v", got.Containers[0].Logs)
	}
	if len(got.Warnings) == 0 {
		t.Fatalf("expected warning about REST containers fallback")
	}
}

func TestFetchApplicationLogsFiltersExecutorAndFetchesGCLog(t *testing.T) {
	const appID = "application_1_9"
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
	mux.HandleFunc("/yarn/proxy/"+appID+"/api/v1/applications/"+appID+"/executors", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, []map[string]any{{
			"id": "7",
			"executorLogs": map[string]string{
				"stderr": srv.URL + "/yarn/nodemanager/node/containerlogs/container_executor_7/alice/stderr",
			},
		}})
	})
	mux.HandleFunc("/yarn/nodemanager/node/containerlogs/container_executor_7/alice/stderr", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Container killed by YARN for exceeding memory\n"))
	})
	mux.HandleFunc("/yarn/nodemanager/node/containerlogs/container_executor_7/alice/gc.log.0.current", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("2026-05-26T10:00:00.000+0800: 123.456: [Full GC (Ergonomics) 120000K->119000K(122000K), 65.432 secs]\n"))
	})

	got, err := NewClient([]string{srv.URL + "/yarn"}, 5*time.Second).FetchApplicationLogs(context.Background(), appID, Options{
		ExecutorID:    "7",
		TopContainers: 5,
		LogTypes:      []string{"stderr", "gc.log.0.current"},
		MaxLogBytes:   512,
	})
	if err != nil {
		t.Fatalf("FetchApplicationLogs: %v", err)
	}
	if len(got.Containers) != 1 {
		t.Fatalf("containers = %+v warnings=%v", got.Containers, got.Warnings)
	}
	c := got.Containers[0]
	if c.SparkExecutorID != "7" {
		t.Fatalf("SparkExecutorID=%q", c.SparkExecutorID)
	}
	if !strings.Contains(c.Logs["gc.log.0.current"], "Full GC") {
		t.Fatalf("gc log missing: %+v", c.Logs)
	}
	if len(c.LogFindings) != 1 || c.LogFindings[0].Type != "full_gc" {
		t.Fatalf("log findings = %+v", c.LogFindings)
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

func TestFetchPaimonDiagnosticsReadsPaimonJSONEndpoints(t *testing.T) {
	const appID = "application_1_11"
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
	mux.HandleFunc("/yarn/proxy/"+appID+"/paimon-diagnostics/json", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{
			"schema_version": 1,
			"facts": map[string]any{
				"executor_count": 2,
			},
		})
	})
	mux.HandleFunc("/yarn/proxy/"+appID+"/paimon-diagnostics/threadDump/json", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("executorId"); got != "7" {
			t.Fatalf("executorId=%q", got)
		}
		writeJSON(t, w, map[string]any{
			"executor_id": "7",
			"diagnosis": map[string]any{
				"category": "executor_paimon_or_parquet_io",
			},
		})
	})
	mux.HandleFunc("/yarn/proxy/"+appID+"/paimon-diagnostics/profiler/json", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{
			"diagnosis": map[string]any{
				"category": "profiler_artifacts_available",
			},
			"facts": map[string]any{
				"artifact_count": 1,
			},
		})
	})

	got, err := NewClient([]string{srv.URL + "/yarn"}, 5*time.Second).FetchPaimonDiagnostics(context.Background(), appID, "7")
	if err != nil {
		t.Fatalf("FetchPaimonDiagnostics: %v", err)
	}
	if got.ExecutorID != "7" || got.UIURL != srv.URL+"/yarn/proxy/"+appID {
		t.Fatalf("unexpected routing fields: %+v", got)
	}
	if len(got.Warnings) != 0 {
		t.Fatalf("warnings = %+v", got.Warnings)
	}
	if got.Overview["schema_version"].(float64) != 1 {
		t.Fatalf("overview = %+v", got.Overview)
	}
	if got.ThreadDump["executor_id"] != "7" {
		t.Fatalf("thread dump = %+v", got.ThreadDump)
	}
	if got.Profiler["diagnosis"].(map[string]any)["category"] != "profiler_artifacts_available" {
		t.Fatalf("profiler = %+v", got.Profiler)
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

func TestFetchThreadDumpClassifiesFilePruningCollapseProject(t *testing.T) {
	const appID = "application_1_7"
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
				"org.apache.spark.sql.catalyst.planning.PhysicalOperation$.unapply(patterns.scala:114)",
				"org.apache.spark.sql.execution.datasources.PruneFileSourcePartitions$$anonfun$apply$1.applyOrElse(PruneFileSourcePartitions.scala:51)",
			}},
		}})
	})

	got, err := NewClient([]string{srv.URL + "/yarn"}, 5*time.Second).FetchThreadDump(context.Background(), appID, "driver")
	if err != nil {
		t.Fatalf("FetchThreadDump: %v", err)
	}
	if got.Diagnosis == nil || got.Diagnosis.Category != "spark_file_pruning_collapse_project" {
		t.Fatalf("diagnosis = %+v", got.Diagnosis)
	}
	if got.MainThread == nil || !hasTestString(got.MainThread.Tags, "spark_file_pruning") {
		t.Fatalf("main thread tags = %+v", got.MainThread)
	}
}

func TestFetchThreadDumpReturnsStructuredReportWhenEndpointIsHTML(t *testing.T) {
	const appID = "application_1_8"
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/yarn/ws/v1/cluster/apps/"+appID, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"app": map[string]any{
			"id":          appID,
			"user":        "alice",
			"state":       "RUNNING",
			"trackingUrl": srv.URL + "/yarn/proxy/" + appID + "/",
		}})
	})
	mux.HandleFunc("/yarn/proxy/"+appID+"/api/v1/applications/"+appID+"/executors/driver/threads", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>not spark rest json</html>"))
	})

	got, err := NewClient([]string{srv.URL + "/yarn"}, 5*time.Second).FetchThreadDump(context.Background(), appID, "driver")
	if err != nil {
		t.Fatalf("FetchThreadDump should return structured partial report, got error: %v", err)
	}
	if got.ThreadCount != 0 || len(got.Warnings) != 1 {
		t.Fatalf("thread dump summary = %+v", got)
	}
	if got.ThreadDumpURL == "" {
		t.Fatalf("thread_dump_url missing: %+v", got)
	}
	if got.Diagnosis == nil || got.Diagnosis.Category != "spark_ui_thread_dump_unavailable" {
		t.Fatalf("diagnosis = %+v", got.Diagnosis)
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
