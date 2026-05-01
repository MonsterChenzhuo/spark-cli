package fs

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var _ FS = (*SHS)(nil)

// SHS implements FS over the Spark History Server REST API
// (`GET /api/v1/applications/<id>/<attempt>/logs` returns a zip). The zip is
// fetched once per appID per process and exposed as a virtual filesystem so
// the existing eventlog Locator and reader work unchanged.
type SHS struct {
	base       string // e.g. "shs://host:port"
	httpURL    string // e.g. "http://host:port"
	httpClient *http.Client
	threshold  int64 // bytes; zips larger than this spill to tempfile

	mu       sync.Mutex
	bundles  map[string]*shsBundle // appID → fetched zip
	tmpfiles []string              // for Close cleanup
}

type shsBundle struct {
	appID       string
	attemptID   string
	lastUpdated int64 // unix nanoseconds
	reader      *zip.Reader
	backing     io.Closer
	entries     map[string]*zip.File // full inner-zip path → entry
}

// NewSHS constructs a Spark History Server FS bound to base of the form
// "shs://host:port". TLS is not supported in v1 — the transport always uses
// "http://". A non-positive timeout defaults to 60s.
func NewSHS(base string, timeout time.Duration) *SHS {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	httpURL := strings.Replace(base, "shs://", "http://", 1)
	return &SHS{
		base:       base,
		httpURL:    httpURL,
		httpClient: &http.Client{Timeout: timeout},
		threshold:  256 << 20,
		bundles:    map[string]*shsBundle{},
	}
}

func (s *SHS) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var first error
	for _, b := range s.bundles {
		if b != nil && b.backing != nil {
			if err := b.backing.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	for _, p := range s.tmpfiles {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) && first == nil {
			first = err
		}
	}
	s.bundles = map[string]*shsBundle{}
	s.tmpfiles = nil
	return first
}

// Open returns a reader over the zip entry identified by uri. Caller closes.
func (s *SHS) Open(uri string) (io.ReadCloser, error) {
	host, appID, inner, err := splitSHSURI(uri)
	if err != nil {
		return nil, err
	}
	if appID == "" || inner == "" {
		return nil, fmt.Errorf("shs: Open requires appID and inner path: %s", uri)
	}
	b, err := s.bundleFor(host, appID)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, fmt.Errorf("shs: app %s not found", appID)
	}
	f, ok := b.entries[inner]
	if !ok {
		return nil, fmt.Errorf("shs: entry %s not found in app %s", inner, appID)
	}
	return f.Open()
}

// Stat reports a FileInfo for the zip entry or synthetic directory at uri.
func (s *SHS) Stat(uri string) (FileInfo, error) {
	host, appID, inner, err := splitSHSURI(uri)
	if err != nil {
		return FileInfo{}, err
	}
	if appID == "" {
		return FileInfo{}, fmt.Errorf("shs: Stat requires appID: %s", uri)
	}
	b, err := s.bundleFor(host, appID)
	if err != nil {
		return FileInfo{}, err
	}
	if b == nil {
		return FileInfo{}, fmt.Errorf("shs: app %s not found", appID)
	}
	if inner == "" {
		return FileInfo{URI: uri, Name: appID, ModTime: b.lastUpdated, IsDir: true}, nil
	}
	if f, ok := b.entries[inner]; ok {
		size := int64(f.UncompressedSize64) // #nosec G115 -- zip entries fit int64 in practice
		return FileInfo{
			URI:     uri,
			Name:    path.Base(inner),
			Size:    size,
			ModTime: b.lastUpdated,
			IsDir:   false,
		}, nil
	}
	prefix := inner + "/"
	for name := range b.entries {
		if strings.HasPrefix(name, prefix) {
			return FileInfo{
				URI:     uri,
				Name:    path.Base(inner),
				ModTime: b.lastUpdated,
				IsDir:   true,
			}, nil
		}
	}
	return FileInfo{}, fmt.Errorf("shs: %s not found", uri)
}

// List enumerates entries inside the SHS virtual filesystem.
//
// Two callers in the locator:
//
//   - Root listing (`shs://host:port`, prefix=`application_<id>` or
//     `eventlog_v2_application_<id>`): we derive the appID from the prefix,
//     fetch the bundle, and return synthetic URIs for matching top-level zip
//     entries.
//   - Inner V2 listing (`shs://host:port/<appID>/<innerDir>`, prefix=`events_`):
//     we re-use the cached bundle and return URIs for zip entries inside that
//     directory whose basename starts with prefix.
//
// HTTP 404 on the metadata endpoint surfaces as (nil, nil) so the locator can
// fall through to APP_NOT_FOUND. Genuine I/O errors propagate.
func (s *SHS) List(dirURI, prefix string) ([]string, error) {
	host, appID, inner, err := splitSHSURI(dirURI)
	if err != nil {
		return nil, err
	}
	if appID == "" {
		appID = appIDFromListPrefix(prefix)
		if appID == "" {
			return nil, nil
		}
		b, err := s.bundleFor(host, appID)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return nil, nil
		}
		seen := map[string]bool{}
		var out []string
		for name := range b.entries {
			seg := name
			if i := strings.Index(name, "/"); i >= 0 {
				seg = name[:i]
			}
			if seen[seg] {
				continue
			}
			seen[seg] = true
			if strings.HasPrefix(seg, prefix) {
				out = append(out, fmt.Sprintf("%s/%s/%s", s.base, appID, seg))
			}
		}
		sort.Strings(out)
		return out, nil
	}

	b, err := s.bundleFor(host, appID)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	dirPrefix := inner + "/"
	var out []string
	for name := range b.entries {
		if !strings.HasPrefix(name, dirPrefix) {
			continue
		}
		base := name[len(dirPrefix):]
		if strings.Contains(base, "/") {
			continue // nested deeper, not a direct child
		}
		if strings.HasPrefix(base, prefix) {
			out = append(out, fmt.Sprintf("%s/%s/%s/%s", s.base, appID, inner, base))
		}
	}
	sort.Strings(out)
	return out, nil
}

// appIDFromListPrefix peels `eventlog_v2_` and `_<attempt>` from a Locator
// prefix to recover the bare `application_<id>`. Returns empty string when
// the prefix doesn't carry an appID we recognize.
func appIDFromListPrefix(prefix string) string {
	id := strings.TrimPrefix(prefix, "eventlog_v2_")
	if !strings.HasPrefix(id, "application_") {
		return ""
	}
	return id
}

// splitSHSURI parses a URI of shape `shs://host[:port][/appID[/inner...]]`
// into (host, appID, inner). inner preserves embedded slashes.
func splitSHSURI(uri string) (host, appID, inner string, err error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", "", fmt.Errorf("shs: bad URI %q: %w", uri, err)
	}
	if u.Scheme != "shs" {
		return "", "", "", fmt.Errorf("shs: unsupported scheme %q", u.Scheme)
	}
	host = u.Host
	p := strings.TrimPrefix(u.Path, "/")
	if p == "" {
		return host, "", "", nil
	}
	if i := strings.Index(p, "/"); i >= 0 {
		return host, p[:i], p[i+1:], nil
	}
	return host, p, "", nil
}

// bundleFor lazily fetches and caches the zip for appID.
//
// Returns (nil, nil) when the SHS reports 404 (no such app or no attempts) so
// the locator can fall through to APP_NOT_FOUND. All other failures return an
// error.
func (s *SHS) bundleFor(host, appID string) (*shsBundle, error) {
	s.mu.Lock()
	if b, ok := s.bundles[appID]; ok {
		s.mu.Unlock()
		return b, nil
	}
	s.mu.Unlock()

	_ = host // host is implicit in s.httpURL; SHS instance is per-base

	attempt, found, lastUpdated, err := s.fetchAttempt(appID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	zr, backing, tmpPath, err := s.fetchZip(appID, attempt)
	if err != nil {
		return nil, err
	}

	entries := make(map[string]*zip.File, len(zr.File))
	for _, f := range zr.File {
		// drop trailing-slash dir entries; we synthesize directories from path prefixes
		name := strings.TrimSuffix(f.Name, "/")
		if name == "" || f.FileInfo().IsDir() {
			continue
		}
		entries[name] = f
	}

	b := &shsBundle{
		appID:       appID,
		attemptID:   attempt,
		lastUpdated: lastUpdated,
		reader:      zr,
		backing:     backing,
		entries:     entries,
	}

	s.mu.Lock()
	if existing, ok := s.bundles[appID]; ok {
		// lost race; discard ours
		s.mu.Unlock()
		if backing != nil {
			_ = backing.Close()
		}
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
		return existing, nil
	}
	s.bundles[appID] = b
	if tmpPath != "" {
		s.tmpfiles = append(s.tmpfiles, tmpPath)
	}
	s.mu.Unlock()
	return b, nil
}

// fetchAttempt selects the appropriate attempt segment for the SHS logs URL.
//
// Return shape: (attempt, found, lastUpdatedNS, err).
//
//   - found=false: SHS reported 404 or attempts is empty → caller treats as
//     APP_NOT_FOUND.
//   - found=true, attempt="N": multi-attempt or numeric attempt; logs URL is
//     /api/v1/applications/<appID>/N/logs.
//   - found=true, attempt="": single-attempt apps where the attempts[] entry
//     omits attemptId (Spark 3.4+ default); logs URL is
//     /api/v1/applications/<appID>/logs (no attempt segment). SHS returns 404
//     for /<appID>/<empty>/logs in that case, so we MUST drop the segment.
func (s *SHS) fetchAttempt(appID string) (attempt string, found bool, lastUpdatedNS int64, err error) {
	metaURL := fmt.Sprintf("%s/api/v1/applications/%s", s.httpURL, appID)
	resp, err := s.httpClient.Get(metaURL)
	if err != nil {
		return "", false, 0, fmt.Errorf("shs: GET %s: %w", metaURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", false, 0, nil
	}
	if resp.StatusCode != http.StatusOK {
		return "", false, 0, fmt.Errorf("shs: GET %s: status %d", metaURL, resp.StatusCode)
	}
	var meta struct {
		ID       string `json:"id"`
		Attempts []struct {
			AttemptID        string `json:"attemptId"`
			LastUpdated      string `json:"lastUpdated"`
			LastUpdatedEpoch int64  `json:"lastUpdatedEpoch"`
		} `json:"attempts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return "", false, 0, fmt.Errorf("shs: decode metadata: %w", err)
	}
	if len(meta.Attempts) == 0 {
		return "", false, 0, nil
	}
	bestN := -1
	bestIdx := -1
	for i, a := range meta.Attempts {
		n, err := strconv.Atoi(a.AttemptID)
		if err != nil {
			continue
		}
		if n > bestN {
			bestN = n
			bestIdx = i
		}
	}
	if bestIdx < 0 {
		// no numeric attempt — fall back to last entry (typically newest)
		bestIdx = len(meta.Attempts) - 1
	}
	a := meta.Attempts[bestIdx]
	if a.LastUpdatedEpoch > 0 {
		lastUpdatedNS = a.LastUpdatedEpoch * int64(time.Millisecond)
	} else if a.LastUpdated != "" {
		lastUpdatedNS = parseSHSTimestamp(a.LastUpdated)
	}
	return a.AttemptID, true, lastUpdatedNS, nil
}

func (s *SHS) fetchZip(appID, attempt string) (*zip.Reader, io.Closer, string, error) {
	// Empty attempt means SHS returned an attempts[] entry with no attemptId,
	// in which case the logs are served at /api/v1/applications/<id>/logs;
	// inserting an empty segment yields a 404.
	var logsURL string
	if attempt == "" {
		logsURL = fmt.Sprintf("%s/api/v1/applications/%s/logs", s.httpURL, appID)
	} else {
		logsURL = fmt.Sprintf("%s/api/v1/applications/%s/%s/logs", s.httpURL, appID, attempt)
	}
	resp, err := s.httpClient.Get(logsURL)
	if err != nil {
		return nil, nil, "", fmt.Errorf("shs: GET %s: %w", logsURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, "", fmt.Errorf("shs: GET %s: status %d", logsURL, resp.StatusCode)
	}

	cl := resp.ContentLength
	if cl >= 0 && cl <= s.threshold {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, nil, "", fmt.Errorf("shs: read zip body: %w", err)
		}
		zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
		if err != nil {
			return nil, nil, "", fmt.Errorf("shs: parse zip: %w", err)
		}
		return zr, nil, "", nil
	}

	f, err := os.CreateTemp("", "spark-cli-shs-*.zip")
	if err != nil {
		return nil, nil, "", fmt.Errorf("shs: create tempfile: %w", err)
	}
	tmpPath := f.Name()
	n, err := io.Copy(f, resp.Body)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return nil, nil, "", fmt.Errorf("shs: write zip to tempfile: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return nil, nil, "", fmt.Errorf("shs: seek tempfile: %w", err)
	}
	zr, err := zip.NewReader(f, n)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return nil, nil, "", fmt.Errorf("shs: parse zip: %w", err)
	}
	return zr, f, tmpPath, nil
}

// parseSHSTimestamp accepts the various forms Spark History Server emits:
//
//	2024-01-15T03:42:11.123GMT (legacy, literal "GMT")
//	2024-01-15T03:42:11.123Z   (ISO 8601 zulu)
//
// Returns 0 on any parse failure — cache_key invalidation may then mis-fire
// across runs but stays stable within a process.
func parseSHSTimestamp(s string) int64 {
	if strings.HasSuffix(s, "GMT") {
		s = strings.TrimSuffix(s, "GMT") + "Z"
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000Z"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixNano()
		}
	}
	return 0
}
