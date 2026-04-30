package fs

import (
	"io"
	"os"
	"path/filepath"
	"testing"
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
