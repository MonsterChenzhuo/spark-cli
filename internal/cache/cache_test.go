package cache

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/opay-bigdata/spark-cli/internal/eventlog"
	"github.com/opay-bigdata/spark-cli/internal/fs"
	"github.com/opay-bigdata/spark-cli/internal/model"
)

func writeFile(t *testing.T, p, body string) {
	t.Helper()
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func newApp(id string) *model.Application {
	a := model.NewApplication()
	a.ID = id
	a.Name = "demo"
	a.DurationMs = 1234
	return a
}

func TestPutCreatesFile(t *testing.T) {
	srcDir := t.TempDir()
	cacheDir := t.TempDir()
	logPath := filepath.Join(srcDir, "application_1_a")
	writeFile(t, logPath, "hi")

	src := eventlog.LogSource{URI: "file://" + logPath, Format: "v1"}
	c := New(cacheDir)
	c.Put(src, fs.NewLocal(), newApp("application_1_a"))

	want := c.Path(src)
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("expected cache file at %s: %v", want, err)
	}
}

func TestPutSkipsInprogress(t *testing.T) {
	srcDir := t.TempDir()
	cacheDir := t.TempDir()
	logPath := filepath.Join(srcDir, "application_1_a.inprogress")
	writeFile(t, logPath, "hi")

	src := eventlog.LogSource{URI: "file://" + logPath, Format: "v1", Incomplete: true}
	c := New(cacheDir)
	c.Put(src, fs.NewLocal(), newApp("application_1_a"))

	entries, _ := os.ReadDir(cacheDir)
	if len(entries) != 0 {
		t.Errorf("inprogress should not write cache, got %d files", len(entries))
	}
}

func TestPutDisabledIsNoop(t *testing.T) {
	srcDir := t.TempDir()
	logPath := filepath.Join(srcDir, "application_1_a")
	writeFile(t, logPath, "hi")
	src := eventlog.LogSource{URI: "file://" + logPath, Format: "v1"}

	Disabled().Put(src, fs.NewLocal(), newApp("application_1_a"))
}

func TestPutConcurrentSurvives(t *testing.T) {
	srcDir := t.TempDir()
	cacheDir := t.TempDir()
	logPath := filepath.Join(srcDir, "application_1_a")
	writeFile(t, logPath, "hi")
	src := eventlog.LogSource{URI: "file://" + logPath, Format: "v1"}

	c := New(cacheDir)
	app := newApp("application_1_a")

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Put(src, fs.NewLocal(), app) }()
	}
	wg.Wait()

	st, err := os.Stat(c.Path(src))
	if err != nil {
		t.Fatalf("missing final cache file: %v", err)
	}
	if st.Size() == 0 {
		t.Fatal("final cache file is empty")
	}
	entries, _ := os.ReadDir(cacheDir)
	for _, e := range entries {
		if !hasSuffix(e.Name(), ".gob.zst") {
			t.Errorf("stray file in cache dir: %s", e.Name())
		}
	}
}

func TestPutThenGetHits(t *testing.T) {
	srcDir := t.TempDir()
	cacheDir := t.TempDir()
	logPath := filepath.Join(srcDir, "application_1_a")
	writeFile(t, logPath, "hi")
	src := eventlog.LogSource{URI: "file://" + logPath, Format: "v1"}

	c := New(cacheDir)
	want := newApp("application_1_a")
	want.DurationMs = 5555
	c.Put(src, fs.NewLocal(), want)

	got, hit := c.Get(src, fs.NewLocal())
	if !hit {
		t.Fatal("expected hit")
	}
	if got.ID != want.ID || got.Name != want.Name || got.DurationMs != want.DurationMs {
		t.Errorf("round-trip mismatch: got=%+v want=%+v", got, want)
	}
}

func TestGetMissOnEmptyDir(t *testing.T) {
	srcDir := t.TempDir()
	cacheDir := t.TempDir()
	logPath := filepath.Join(srcDir, "application_1_a")
	writeFile(t, logPath, "hi")
	src := eventlog.LogSource{URI: "file://" + logPath, Format: "v1"}

	if _, hit := New(cacheDir).Get(src, fs.NewLocal()); hit {
		t.Fatal("hit on empty cache dir")
	}
}

func TestGetMissOnSizeChange(t *testing.T) {
	srcDir := t.TempDir()
	cacheDir := t.TempDir()
	logPath := filepath.Join(srcDir, "application_1_a")
	writeFile(t, logPath, "hi")
	src := eventlog.LogSource{URI: "file://" + logPath, Format: "v1"}

	c := New(cacheDir)
	c.Put(src, fs.NewLocal(), newApp("application_1_a"))

	if err := os.WriteFile(logPath, []byte("hi-extra"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, hit := c.Get(src, fs.NewLocal()); hit {
		t.Fatal("hit despite size change")
	}
}

func TestGetMissOnMtimeChange(t *testing.T) {
	srcDir := t.TempDir()
	cacheDir := t.TempDir()
	logPath := filepath.Join(srcDir, "application_1_a")
	writeFile(t, logPath, "hi")
	src := eventlog.LogSource{URI: "file://" + logPath, Format: "v1"}

	c := New(cacheDir)
	c.Put(src, fs.NewLocal(), newApp("application_1_a"))

	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(logPath, future, future); err != nil {
		t.Fatal(err)
	}
	if _, hit := c.Get(src, fs.NewLocal()); hit {
		t.Fatal("hit despite mtime change")
	}
}

func TestGetMissOnSchemaVersionMismatch(t *testing.T) {
	srcDir := t.TempDir()
	cacheDir := t.TempDir()
	logPath := filepath.Join(srcDir, "application_1_a")
	writeFile(t, logPath, "hi")
	src := eventlog.LogSource{URI: "file://" + logPath, Format: "v1"}

	c := New(cacheDir)
	c.Put(src, fs.NewLocal(), newApp("application_1_a"))

	stale := cacheEnvelope{
		SchemaVersion: 0,
		CLIVersion:    "test",
		SourceKind:    "v1",
		App:           newApp("application_1_a"),
	}
	stale.SourceKey, _ = computeSourceKey(src, fs.NewLocal())
	rewriteEnvelope(t, c.Path(src), stale)

	if _, hit := c.Get(src, fs.NewLocal()); hit {
		t.Fatal("hit despite schemaVersion=0")
	}
	if _, err := os.Stat(c.Path(src)); !os.IsNotExist(err) {
		t.Errorf("schema-mismatch cache file still present: err=%v", err)
	}
}

func TestGetMissOnCorruptFile(t *testing.T) {
	srcDir := t.TempDir()
	cacheDir := t.TempDir()
	logPath := filepath.Join(srcDir, "application_1_a")
	writeFile(t, logPath, "hi")
	src := eventlog.LogSource{URI: "file://" + logPath, Format: "v1"}

	c := New(cacheDir)
	cachePath := c.Path(src)
	if err := os.WriteFile(cachePath, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, hit := c.Get(src, fs.NewLocal()); hit {
		t.Fatal("hit on garbage file")
	}
	if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
		t.Errorf("corrupt cache file still present: err=%v", err)
	}
}

func TestGetDisabledIsMiss(t *testing.T) {
	srcDir := t.TempDir()
	logPath := filepath.Join(srcDir, "application_1_a")
	writeFile(t, logPath, "hi")
	src := eventlog.LogSource{URI: "file://" + logPath, Format: "v1"}
	if _, hit := Disabled().Get(src, fs.NewLocal()); hit {
		t.Fatal("disabled cache must always miss")
	}
}

// rewriteEnvelope replaces an existing cache file with a hand-crafted envelope.
func rewriteEnvelope(t *testing.T, p string, env cacheEnvelope) {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := gob.NewEncoder(zw).Encode(env); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}
