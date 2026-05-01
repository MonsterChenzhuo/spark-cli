package cache

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opay-bigdata/spark-cli/internal/eventlog"
	"github.com/opay-bigdata/spark-cli/internal/fs"
)

func TestComputeSourceKeyV1(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "application_1_a")
	if err := os.WriteFile(p, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	src := eventlog.LogSource{URI: "file://" + p, Format: "v1"}
	key, ok := computeSourceKey(src, fs.NewLocal())
	if !ok {
		t.Fatal("ok=false")
	}
	if key.PartCount != 1 || key.TotalSize != 5 || key.MaxMtime == 0 {
		t.Errorf("bad key: %+v", key)
	}
	if key.URI != src.URI {
		t.Errorf("URI: got %s want %s", key.URI, src.URI)
	}
}

func TestComputeSourceKeyV1Missing(t *testing.T) {
	src := eventlog.LogSource{URI: "file:///does/not/exist", Format: "v1"}
	if _, ok := computeSourceKey(src, fs.NewLocal()); ok {
		t.Fatal("ok=true for missing file")
	}
}

func TestComputeSourceKeyV2(t *testing.T) {
	dir := t.TempDir()
	mk := func(name, body string) string {
		full := filepath.Join(dir, name)
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return "file://" + full
	}
	p1 := mk("events_1", "aa")
	time.Sleep(10 * time.Millisecond)
	p2 := mk("events_2", "bbbb")
	time.Sleep(10 * time.Millisecond)
	p3 := mk("events_3", "cccccc")
	src := eventlog.LogSource{
		URI:    "file://" + dir,
		Format: "v2",
		Parts:  []string{p1, p2, p3},
	}
	key, ok := computeSourceKey(src, fs.NewLocal())
	if !ok {
		t.Fatal("ok=false")
	}
	if key.PartCount != 3 {
		t.Errorf("PartCount=%d want 3", key.PartCount)
	}
	if key.TotalSize != 12 {
		t.Errorf("TotalSize=%d want 12", key.TotalSize)
	}
	st3, _ := fs.NewLocal().Stat(p3)
	if key.MaxMtime != st3.ModTime {
		t.Errorf("MaxMtime=%d want %d", key.MaxMtime, st3.ModTime)
	}
}
