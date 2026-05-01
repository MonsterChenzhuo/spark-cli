package fs

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLocalOpenStatList(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "application_1_a"), []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "application_2_b.zstd"), []byte("xx"), 0644); err != nil {
		t.Fatal(err)
	}
	l := NewLocal()

	r, err := l.Open("file://" + filepath.Join(dir, "application_1_a"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	body, _ := io.ReadAll(r)
	r.Close()
	if string(body) != "hello" {
		t.Fatalf("body = %q", body)
	}

	st, err := l.Stat("file://" + filepath.Join(dir, "application_1_a"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Size != 5 {
		t.Fatalf("size = %d", st.Size)
	}

	matches, err := l.List("file://"+dir, "application_")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Fatalf("matches = %v", matches)
	}
}

func TestLocalStatReturnsModTime(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	before := time.Now().Add(-time.Second).UnixNano()
	if err := os.WriteFile(p, []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	after := time.Now().Add(time.Second).UnixNano()

	st, err := NewLocal().Stat("file://" + p)
	if err != nil {
		t.Fatal(err)
	}
	if st.ModTime < before || st.ModTime > after {
		t.Errorf("ModTime %d out of [%d, %d]", st.ModTime, before, after)
	}
}

func TestLocalRejectsNonFileScheme(t *testing.T) {
	l := NewLocal()
	if _, err := l.Open("hdfs://nn/x"); err == nil {
		t.Fatal("Open: expected error for hdfs:// scheme")
	}
	if _, err := l.Stat("s3://bucket/k"); err == nil {
		t.Fatal("Stat: expected error for s3:// scheme")
	}
	if _, err := l.List("hdfs://nn/dir", ""); err == nil {
		t.Fatal("List: expected error for hdfs:// scheme")
	}
}
