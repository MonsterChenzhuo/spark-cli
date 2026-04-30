package eventlog

import (
	"bytes"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestOpenPlain(t *testing.T) {
	r, err := openCompressed(io.NopCloser(bytes.NewReader([]byte("hello"))), CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(r)
	if string(body) != "hello" {
		t.Fatalf("body = %q", body)
	}
}

func TestOpenZstd(t *testing.T) {
	var buf bytes.Buffer
	w, _ := zstd.NewWriter(&buf)
	_, _ = w.Write([]byte("zstd payload"))
	_ = w.Close()
	r, err := openCompressed(io.NopCloser(&buf), CompressionZstd)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(r)
	if string(body) != "zstd payload" {
		t.Fatalf("body = %q", body)
	}
}

func TestDetectCompressionFromName(t *testing.T) {
	cases := map[string]Compression{
		"application_1_a":              CompressionNone,
		"application_1_a.zstd":         CompressionZstd,
		"application_1_a.lz4":          CompressionLZ4,
		"application_1_a.snappy":       CompressionSnappy,
		"application_1_a.zstd.inprogress": CompressionZstd,
		"application_1_a.inprogress":   CompressionNone,
	}
	for name, want := range cases {
		if got := DetectCompression(name); got != want {
			t.Errorf("%s: got %v want %v", name, got, want)
		}
	}
}
