package eventlog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/fs"
)

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
