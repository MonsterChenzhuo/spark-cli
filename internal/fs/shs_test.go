package fs

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// shsAttempt mirrors the JSON shape of one element of `applications/<id>.attempts`.
type shsAttempt struct {
	AttemptID        string `json:"attemptId"`
	LastUpdated      string `json:"lastUpdated"`
	LastUpdatedEpoch int64  `json:"lastUpdatedEpoch"`
}

type shsApp struct {
	ID       string            `json:"id"`
	Attempts []shsAttempt      `json:"attempts"`
	zips     map[string][]byte // attemptID → zip body
}

// newSHSTestServer builds an httptest.Server impersonating Spark History Server.
//
// apps: appID → app metadata (and per-attempt zip bodies)
// counts: optional pointer to capture request counts (metadata + logs)
// delay: optional sleep before responding (used for timeout tests)
// chunked: if true, /logs response omits Content-Length (uses chunked transfer)
func newSHSTestServer(t *testing.T, apps map[string]*shsApp, counts *int64, delay time.Duration, chunked bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/applications/", func(w http.ResponseWriter, r *http.Request) {
		if counts != nil {
			atomic.AddInt64(counts, 1)
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		// "/api/v1/applications/<appID>" or "/api/v1/applications/<appID>/<attemptID>/logs"
		rest := strings.TrimPrefix(r.URL.Path, "/api/v1/applications/")
		parts := strings.Split(rest, "/")
		appID := parts[0]
		app, ok := apps[appID]
		if !ok {
			http.NotFound(w, r)
			return
		}
		if len(parts) == 1 {
			w.Header().Set("Content-Type", "application/json")
			meta := struct {
				ID       string       `json:"id"`
				Attempts []shsAttempt `json:"attempts"`
			}{ID: app.ID, Attempts: app.Attempts}
			_ = json.NewEncoder(w).Encode(meta)
			return
		}
		if len(parts) >= 3 && parts[2] == "logs" {
			attemptID := parts[1]
			body, ok := app.zips[attemptID]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/zip")
			if !chunked {
				w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
			if chunked {
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
			return
		}
		http.NotFound(w, r)
	})
	return httptest.NewServer(mux)
}

func buildZip(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	// stable order
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		f, err := zw.Create(n)
		if err != nil {
			t.Fatalf("zip create: %v", err)
		}
		if _, err := f.Write(files[n]); err != nil {
			t.Fatalf("zip write: %v", err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

func shsBase(srv *httptest.Server) string {
	u, _ := url.Parse(srv.URL)
	return "shs://" + u.Host
}

func TestSHSAutoPicksMaxAttempt(t *testing.T) {
	appID := "application_x"
	zip2 := buildZip(t, map[string][]byte{appID + "_2": []byte("hello")})
	zip1 := buildZip(t, map[string][]byte{appID + "_1": []byte("oldhi")})
	apps := map[string]*shsApp{
		appID: {
			ID: appID,
			Attempts: []shsAttempt{
				{AttemptID: "1", LastUpdatedEpoch: 1},
				{AttemptID: "2", LastUpdatedEpoch: 2},
			},
			zips: map[string][]byte{"1": zip1, "2": zip2},
		},
	}
	srv := newSHSTestServer(t, apps, nil, 0, false)
	defer srv.Close()

	s := NewSHS(shsBase(srv), 5*time.Second)
	defer func() { _ = s.Close() }()

	uris, err := s.List(shsBase(srv), appID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(uris) != 1 {
		t.Fatalf("got URIs %v, want 1", uris)
	}
	if !strings.HasSuffix(uris[0], "/"+appID+"/"+appID+"_2") {
		t.Errorf("URI %q does not end with /%s/%s_2", uris[0], appID, appID)
	}
	st, err := s.Stat(uris[0])
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.IsDir {
		t.Errorf("V1 entry should not be IsDir")
	}
	wantNS := int64(2) * int64(time.Millisecond)
	if st.ModTime != wantNS {
		t.Errorf("ModTime = %d, want %d", st.ModTime, wantNS)
	}
}

func TestSHSV1Open(t *testing.T) {
	appID := "application_x"
	body := []byte("hello")
	z := buildZip(t, map[string][]byte{appID + "_1": body})
	apps := map[string]*shsApp{
		appID: {
			ID:       appID,
			Attempts: []shsAttempt{{AttemptID: "1", LastUpdatedEpoch: 1000}},
			zips:     map[string][]byte{"1": z},
		},
	}
	srv := newSHSTestServer(t, apps, nil, 0, false)
	defer srv.Close()

	s := NewSHS(shsBase(srv), 5*time.Second)
	defer func() { _ = s.Close() }()

	uris, err := s.List(shsBase(srv), appID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(uris) != 1 {
		t.Fatalf("List = %v", uris)
	}
	rc, err := s.Open(uris[0])
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, body) {
		t.Errorf("got %q want %q", got, body)
	}
}

func TestSHSV2Layout(t *testing.T) {
	appID := "application_x"
	v2dir := "eventlog_v2_" + appID + "_1"
	z := buildZip(t, map[string][]byte{
		v2dir + "/appstatus_" + appID + "_1": []byte(""),
		v2dir + "/events_1_" + appID + "_1":  []byte("part1"),
		v2dir + "/events_2_" + appID + "_1":  []byte("part2"),
	})
	apps := map[string]*shsApp{
		appID: {
			ID:       appID,
			Attempts: []shsAttempt{{AttemptID: "1", LastUpdatedEpoch: 100}},
			zips:     map[string][]byte{"1": z},
		},
	}
	srv := newSHSTestServer(t, apps, nil, 0, false)
	defer srv.Close()

	s := NewSHS(shsBase(srv), 5*time.Second)
	defer func() { _ = s.Close() }()

	rootURIs, err := s.List(shsBase(srv), "eventlog_v2_"+appID)
	if err != nil {
		t.Fatalf("root List: %v", err)
	}
	if len(rootURIs) != 1 {
		t.Fatalf("root List = %v", rootURIs)
	}
	dirURI := rootURIs[0]
	if !strings.HasSuffix(dirURI, "/"+appID+"/"+v2dir) {
		t.Errorf("dir URI %q does not end with /%s/%s", dirURI, appID, v2dir)
	}
	st, err := s.Stat(dirURI)
	if err != nil {
		t.Fatalf("Stat dir: %v", err)
	}
	if !st.IsDir {
		t.Errorf("V2 root inside zip should report IsDir=true")
	}

	parts, err := s.List(dirURI, "events_")
	if err != nil {
		t.Fatalf("inner List: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("inner parts = %v", parts)
	}
	for _, p := range parts {
		if !strings.Contains(path.Base(p), "events_") {
			t.Errorf("expected events_ basename in %q", p)
		}
	}
	rc, err := s.Open(parts[0])
	if err != nil {
		t.Fatalf("Open part: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, []byte("part1")) {
		t.Errorf("got %q want part1", got)
	}
}

func TestSHSV1WithZstdSuffix(t *testing.T) {
	appID := "application_x"
	body := []byte("zstd-compressed-bytes-as-is")
	z := buildZip(t, map[string][]byte{appID + "_1.zstd": body})
	apps := map[string]*shsApp{
		appID: {
			ID:       appID,
			Attempts: []shsAttempt{{AttemptID: "1", LastUpdatedEpoch: 100}},
			zips:     map[string][]byte{"1": z},
		},
	}
	srv := newSHSTestServer(t, apps, nil, 0, false)
	defer srv.Close()

	s := NewSHS(shsBase(srv), 5*time.Second)
	defer func() { _ = s.Close() }()

	uris, err := s.List(shsBase(srv), appID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(uris) != 1 || !strings.HasSuffix(uris[0], ".zstd") {
		t.Fatalf("expected .zstd-suffixed entry, got %v", uris)
	}
	rc, err := s.Open(uris[0])
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, body) {
		t.Errorf("Open should return raw bytes (zstd undone by outer codec layer); got %q want %q", got, body)
	}
}

func TestSHSAppNotFound(t *testing.T) {
	srv := newSHSTestServer(t, map[string]*shsApp{}, nil, 0, false)
	defer srv.Close()
	s := NewSHS(shsBase(srv), 5*time.Second)
	defer func() { _ = s.Close() }()
	uris, err := s.List(shsBase(srv), "application_missing")
	if err != nil {
		t.Fatalf("List: %v (expected nil so locator falls through)", err)
	}
	if len(uris) != 0 {
		t.Fatalf("expected empty list for missing app, got %v", uris)
	}
}

func TestSHSHTTPTimeout(t *testing.T) {
	apps := map[string]*shsApp{
		"application_x": {
			ID:       "application_x",
			Attempts: []shsAttempt{{AttemptID: "1", LastUpdatedEpoch: 100}},
			zips:     map[string][]byte{"1": buildZip(t, map[string][]byte{"application_x_1": []byte("x")})},
		},
	}
	srv := newSHSTestServer(t, apps, nil, 200*time.Millisecond, false)
	defer srv.Close()
	s := NewSHS(shsBase(srv), 50*time.Millisecond)
	defer func() { _ = s.Close() }()
	if _, err := s.List(shsBase(srv), "application_x"); err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestSHSLargeZipSpillsToTempfile(t *testing.T) {
	appID := "application_x"
	z := buildZip(t, map[string][]byte{appID + "_1": []byte("hello")})
	apps := map[string]*shsApp{
		appID: {
			ID:       appID,
			Attempts: []shsAttempt{{AttemptID: "1", LastUpdatedEpoch: 100}},
			zips:     map[string][]byte{"1": z},
		},
	}
	srv := newSHSTestServer(t, apps, nil, 0, false)
	defer srv.Close()
	s := NewSHS(shsBase(srv), 5*time.Second)
	s.threshold = 1 // force tempfile path even for tiny zips
	defer func() { _ = s.Close() }()
	if _, err := s.List(shsBase(srv), appID); err != nil {
		t.Fatalf("List: %v", err)
	}
	s.mu.Lock()
	tmpCount := len(s.tmpfiles)
	bundle := s.bundles[appID]
	s.mu.Unlock()
	if tmpCount != 1 {
		t.Errorf("expected 1 tmpfile, got %d", tmpCount)
	}
	if bundle == nil || bundle.backing == nil {
		t.Errorf("bundle.backing should be non-nil for tempfile-spilled bundle")
	}
}

func TestSHSCloseRemovesTempfiles(t *testing.T) {
	appID := "application_x"
	z := buildZip(t, map[string][]byte{appID + "_1": []byte("hello")})
	apps := map[string]*shsApp{
		appID: {
			ID:       appID,
			Attempts: []shsAttempt{{AttemptID: "1", LastUpdatedEpoch: 100}},
			zips:     map[string][]byte{"1": z},
		},
	}
	srv := newSHSTestServer(t, apps, nil, 0, false)
	defer srv.Close()
	s := NewSHS(shsBase(srv), 5*time.Second)
	s.threshold = 1
	if _, err := s.List(shsBase(srv), appID); err != nil {
		t.Fatalf("List: %v", err)
	}
	s.mu.Lock()
	tmpfiles := append([]string(nil), s.tmpfiles...)
	s.mu.Unlock()
	if len(tmpfiles) == 0 {
		t.Fatal("expected at least one tmpfile recorded")
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	for _, p := range tmpfiles {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("expected tmpfile %s removed after Close, got err=%v", p, err)
		}
	}
}

func TestSHSReusesBundleAcrossCalls(t *testing.T) {
	appID := "application_x"
	v2dir := "eventlog_v2_" + appID + "_1"
	z := buildZip(t, map[string][]byte{
		v2dir + "/events_1_" + appID + "_1": []byte("p1"),
		v2dir + "/events_2_" + appID + "_1": []byte("p2"),
	})
	apps := map[string]*shsApp{
		appID: {
			ID:       appID,
			Attempts: []shsAttempt{{AttemptID: "1", LastUpdatedEpoch: 100}},
			zips:     map[string][]byte{"1": z},
		},
	}
	var count int64
	srv := newSHSTestServer(t, apps, &count, 0, false)
	defer srv.Close()
	s := NewSHS(shsBase(srv), 5*time.Second)
	defer func() { _ = s.Close() }()

	if _, err := s.List(shsBase(srv), appID); err != nil { // V1 lookup → empty
		t.Fatalf("V1 list: %v", err)
	}
	rootURIs, err := s.List(shsBase(srv), "eventlog_v2_"+appID)
	if err != nil {
		t.Fatalf("V2 root list: %v", err)
	}
	if _, err := s.Stat(rootURIs[0]); err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if _, err := s.List(rootURIs[0], "events_"); err != nil {
		t.Fatalf("inner list: %v", err)
	}
	got := atomic.LoadInt64(&count)
	// 1 metadata + 1 logs == 2 server hits, regardless of how many FS calls
	if got != 2 {
		t.Errorf("expected 2 server requests (1 meta + 1 logs); got %d", got)
	}
}
