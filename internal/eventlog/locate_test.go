package eventlog

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/fs"
)

type incompleteFS struct {
	*fs.Local
	incomplete map[string]bool
}

func (f incompleteFS) IsIncomplete(uri string) bool { return f.incomplete[uri] }

func writeFile(t *testing.T, p string) {
	t.Helper()
	if err := os.WriteFile(p, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeAppID(t *testing.T) {
	cases := map[string]string{
		"application_1735000000_0001":                            "application_1735000000_0001",
		"1735000000_0001":                                        "application_1735000000_0001",
		"application_1735000000_0001.zstd":                       "application_1735000000_0001",
		"application_1735000000_0001.inprogress":                 "application_1735000000_0001",
		"application_1735000000_0001.zstd.inprogress":            "application_1735000000_0001",
		"application_1735000000_0001.snappy":                     "application_1735000000_0001",
		"application_1735000000_0001.zstd.inprogress.inprogress": "application_1735000000_0001",
	}
	for in, want := range cases {
		if got := normalizeAppID(in); got != want {
			t.Errorf("%s: got %s want %s", in, got, want)
		}
	}
}

func TestResolveV1Plain(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "application_1_a"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if src.Format != "v1" || src.Compression != CompressionNone || src.Incomplete {
		t.Errorf("source = %+v", src)
	}
}

func TestResolveV1Zstd(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "application_1_a.zstd"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if src.Compression != CompressionZstd {
		t.Errorf("got %v", src.Compression)
	}
}

func TestResolveV1Inprogress(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "application_1_a.zstd.inprogress"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if !src.Incomplete || src.Compression != CompressionZstd {
		t.Errorf("got %+v", src)
	}
}

func TestResolveUsesBackendIncompleteReporter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "application_1_a")
	writeFile(t, path)
	local := fs.NewLocal()
	fsys := incompleteFS{
		Local:      local,
		incomplete: map[string]bool{"file://" + path: true},
	}
	loc := NewLocator(map[string]fs.FS{"file": fsys}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if !src.Incomplete {
		t.Fatalf("source = %+v, want Incomplete=true", src)
	}
}

var _ fs.FS = incompleteFS{}
var _ interface {
	Open(string) (io.ReadCloser, error)
	Stat(string) (fs.FileInfo, error)
	List(string, string) ([]string, error)
	Close() error
	IsIncomplete(string) bool
} = incompleteFS{}

func TestResolveAmbiguous(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "application_1_a"))
	writeFile(t, filepath.Join(dir, "application_1_a.zstd"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	if _, err := loc.Resolve("application_1_a"); err == nil {
		t.Fatal("want APP_AMBIGUOUS")
	}
}

func TestResolveNotFound(t *testing.T) {
	dir := t.TempDir()
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	if _, err := loc.Resolve("application_1_a"); err == nil {
		t.Fatal("want APP_NOT_FOUND")
	}
}

// Regression: List(prefix) returns substring-prefix matches, so a longer
// appId that shares the searched prefix must not be picked up.
func TestResolveDoesNotMatchPrefixSibling(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "application_1_a_other"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	if _, err := loc.Resolve("application_1_a"); err == nil {
		t.Fatal("want APP_NOT_FOUND, got match for sibling appID")
	}
}

// V1 single-file logs can also carry an `_<attempt>` suffix (e.g. when
// `spark.eventLog.rolling.enabled=false`). The bare appId must still match.
func TestResolveV1WithAttemptSuffix(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "application_1_a_1"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatalf("expected attempt-suffixed file to resolve: %v", err)
	}
	if src.Format != "v1" {
		t.Errorf("format = %s want v1", src.Format)
	}
	if !strings.HasSuffix(src.URI, "application_1_a_1") {
		t.Errorf("URI = %s, want suffix application_1_a_1", src.URI)
	}
}

func TestResolveV1PicksHighestAttempt(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"1", "3", "2"} {
		writeFile(t, filepath.Join(dir, "application_1_a_"+n))
	}
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(src.URI, "application_1_a_3") {
		t.Errorf("expected highest attempt _3, got %s", src.URI)
	}
}

// Bare-name file beats attempt-suffixed siblings: matches Spark History
// Server's behavior of preferring the un-attempted form when present.
func TestResolveV1ExactBeatsAttempt(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "application_1_a"))
	writeFile(t, filepath.Join(dir, "application_1_a_1"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(src.URI, "application_1_a") || strings.HasSuffix(src.URI, "application_1_a_1") {
		t.Errorf("expected exact match to win, got %s", src.URI)
	}
}

func TestResolveV1ExplicitAttemptInput(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "application_1_a_1"))
	writeFile(t, filepath.Join(dir, "application_1_a_2"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a_1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(src.URI, "application_1_a_1") {
		t.Errorf("explicit attempt _1 not honored, got %s", src.URI)
	}
}

func TestResolveV1AttemptWithCompression(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "application_1_a_1.zstd"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if src.Compression != CompressionZstd {
		t.Errorf("compression = %v want zstd", src.Compression)
	}
}

func TestResolveV2(t *testing.T) {
	dir := t.TempDir()
	v2dir := filepath.Join(dir, "eventlog_v2_application_1_a")
	if err := os.MkdirAll(v2dir, 0755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(v2dir, "appstatus_application_1_a"))
	writeFile(t, filepath.Join(v2dir, "events_2_application_1_a.zstd"))
	writeFile(t, filepath.Join(v2dir, "events_1_application_1_a.zstd"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if src.Format != "v2" || len(src.Parts) != 2 {
		t.Fatalf("source = %+v", src)
	}
	if !strings.HasSuffix(src.Parts[0], "events_1_application_1_a.zstd") {
		t.Fatalf("parts not sorted: %v", src.Parts)
	}
}

func TestResolveV2InprogressLastPart(t *testing.T) {
	dir := t.TempDir()
	v2dir := filepath.Join(dir, "eventlog_v2_application_1_a")
	if err := os.MkdirAll(v2dir, 0755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(v2dir, "appstatus_application_1_a"))
	writeFile(t, filepath.Join(v2dir, "events_1_application_1_a.zstd"))
	writeFile(t, filepath.Join(v2dir, "events_2_application_1_a.zstd.inprogress"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if !src.Incomplete {
		t.Errorf("expected Incomplete=true, got %+v", src)
	}
	if src.Compression != CompressionZstd {
		t.Errorf("expected zstd, got %v", src.Compression)
	}
}

func TestResolveV2MixedCodecs(t *testing.T) {
	dir := t.TempDir()
	v2dir := filepath.Join(dir, "eventlog_v2_application_1_a")
	if err := os.MkdirAll(v2dir, 0755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(v2dir, "events_1_application_1_a.zstd"))
	writeFile(t, filepath.Join(v2dir, "events_2_application_1_a.lz4"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	if _, err := loc.Resolve("application_1_a"); err == nil {
		t.Fatal("want LOG_INCOMPLETE for mixed codecs")
	}
}

func TestResolveV2WithAttemptSuffix(t *testing.T) {
	dir := t.TempDir()
	v2dir := filepath.Join(dir, "eventlog_v2_application_1_a_1")
	if err := os.MkdirAll(v2dir, 0755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(v2dir, "appstatus_application_1_a_1"))
	writeFile(t, filepath.Join(v2dir, "events_1_application_1_a_1"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatalf("expected to resolve attempt-suffixed dir: %v", err)
	}
	if src.Format != "v2" || len(src.Parts) != 1 {
		t.Fatalf("source = %+v", src)
	}
	if !strings.HasSuffix(src.URI, "eventlog_v2_application_1_a_1") {
		t.Fatalf("URI = %s", src.URI)
	}
}

func TestResolveV2PicksHighestAttempt(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"1", "3", "2"} {
		v2dir := filepath.Join(dir, "eventlog_v2_application_1_a_"+n)
		if err := os.MkdirAll(v2dir, 0755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(v2dir, "events_1_application_1_a_"+n))
	}
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(src.URI, "eventlog_v2_application_1_a_3") {
		t.Fatalf("expected highest attempt _3, got %s", src.URI)
	}
}

func TestResolveV2ExplicitAttemptInput(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"1", "2"} {
		v2dir := filepath.Join(dir, "eventlog_v2_application_1_a_"+n)
		if err := os.MkdirAll(v2dir, 0755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(v2dir, "events_1_application_1_a_"+n))
	}
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	src, err := loc.Resolve("application_1_a_1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(src.URI, "eventlog_v2_application_1_a_1") {
		t.Fatalf("explicit attempt _1 not honored, got %s", src.URI)
	}
}

func TestResolveV2DoesNotMatchPrefixSibling(t *testing.T) {
	dir := t.TempDir()
	v2dir := filepath.Join(dir, "eventlog_v2_application_1_a_other")
	if err := os.MkdirAll(v2dir, 0755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(v2dir, "events_1_application_1_a_other"))
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	if _, err := loc.Resolve("application_1_a"); err == nil {
		t.Fatal("want APP_NOT_FOUND, sibling with non-numeric suffix must not match")
	}
}

func TestResolveV2MissingParts(t *testing.T) {
	dir := t.TempDir()
	v2dir := filepath.Join(dir, "eventlog_v2_application_1_a")
	if err := os.MkdirAll(v2dir, 0755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(v2dir, "appstatus_application_1_a"))
	writeFile(t, filepath.Join(v2dir, "events_1_application_1_a.zstd"))
	writeFile(t, filepath.Join(v2dir, "events_3_application_1_a.zstd")) // skip 2
	loc := NewLocator(map[string]fs.FS{"file": fs.NewLocal()}, []string{"file://" + dir})
	if _, err := loc.Resolve("application_1_a"); err == nil {
		t.Fatal("want LOG_INCOMPLETE")
	}
}
