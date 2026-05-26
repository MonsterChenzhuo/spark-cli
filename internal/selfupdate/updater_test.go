package selfupdate

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestUpdateDownloadsVerifiesAndInstallsBinary(t *testing.T) {
	archive := makeArchive(t, "spark-cli", []byte("#!/bin/sh\necho new\n"))
	sum := sha256.Sum256(archive)
	asset := "spark-cli_0.2.0_darwin_arm64.tar.gz"

	mux := http.NewServeMux()
	mux.HandleFunc("/"+asset, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(archive)
	})
	mux.HandleFunc("/checksums.txt", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(hex.EncodeToString(sum[:]) + "  " + asset + "\n"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	target := filepath.Join(t.TempDir(), "spark-cli")
	if err := os.WriteFile(target, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}

	res, err := Update(context.Background(), Options{
		Version:           "v0.2.0",
		GOOS:              "darwin",
		GOARCH:            "arm64",
		DownloadBaseURL:   srv.URL,
		CurrentExecutable: target,
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if res.Asset != asset || res.Target != target {
		t.Fatalf("result = %+v", res)
	}
	body, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "#!/bin/sh\necho new\n" {
		t.Fatalf("target not replaced: %q", string(body))
	}
}

func TestUpdateRejectsChecksumMismatch(t *testing.T) {
	asset := "spark-cli_0.2.0_linux_amd64.tar.gz"
	mux := http.NewServeMux()
	mux.HandleFunc("/"+asset, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(makeArchive(t, "spark-cli", []byte("new")))
	})
	mux.HandleFunc("/checksums.txt", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("0000  " + asset + "\n"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	target := filepath.Join(t.TempDir(), "spark-cli")
	if err := os.WriteFile(target, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := Update(context.Background(), Options{
		Version:           "v0.2.0",
		GOOS:              "linux",
		GOARCH:            "amd64",
		DownloadBaseURL:   srv.URL,
		CurrentExecutable: target,
	})
	if err == nil {
		t.Fatal("expected checksum error")
	}
	body, _ := os.ReadFile(target)
	if string(body) != "old" {
		t.Fatalf("target should not be replaced on checksum mismatch: %q", string(body))
	}
}

func makeArchive(t *testing.T, name string, body []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
