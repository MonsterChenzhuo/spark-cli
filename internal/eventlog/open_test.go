package eventlog

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/fs"
)

func TestOpenLogSourceV1Plain(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "evt")
	_ = os.WriteFile(p, []byte("hello\nworld\n"), 0644)
	src := LogSource{URI: "file://" + p, Format: "v1", Compression: CompressionNone}
	rc, err := Open(src, fs.NewLocal())
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	if string(body) != "hello\nworld\n" {
		t.Fatalf("body = %q", body)
	}
}

func TestOpenLogSourceV2Concat(t *testing.T) {
	dir := t.TempDir()
	p1 := filepath.Join(dir, "events_1")
	p2 := filepath.Join(dir, "events_2")
	_ = os.WriteFile(p1, []byte("part1\n"), 0644)
	_ = os.WriteFile(p2, []byte("part2\n"), 0644)
	src := LogSource{
		URI:         "file://" + dir,
		Format:      "v2",
		Compression: CompressionNone,
		Parts:       []string{"file://" + p1, "file://" + p2},
	}
	rc, err := Open(src, fs.NewLocal())
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	if string(body) != "part1\npart2\n" {
		t.Fatalf("body = %q", body)
	}
}

func TestOpenV2EmptyPartsRejected(t *testing.T) {
	src := LogSource{Format: "v2", Compression: CompressionNone, Parts: nil}
	if _, err := Open(src, fs.NewLocal()); err == nil {
		t.Fatal("expected error for empty v2 Parts")
	}
}

func TestOpenV2CloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	p1 := filepath.Join(dir, "evt1")
	p2 := filepath.Join(dir, "evt2")
	_ = os.WriteFile(p1, []byte("a"), 0644)
	_ = os.WriteFile(p2, []byte("b"), 0644)
	src := LogSource{
		Format:      "v2",
		Compression: CompressionNone,
		Parts:       []string{"file://" + p1, "file://" + p2},
	}
	rc, err := Open(src, fs.NewLocal())
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("second Close should be a no-op, got: %v", err)
	}
}
