package cache

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

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
