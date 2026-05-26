// Package selfupdate installs a released spark-cli binary over the current one.
package selfupdate

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const DefaultRepo = "MonsterChenzhuo/spark-cli"

type Options struct {
	Repo              string
	Version           string
	InstallDir        string
	CurrentExecutable string
	DownloadBaseURL   string
	GitHubBaseURL     string
	GOOS              string
	GOARCH            string
	DryRun            bool
	HTTPClient        *http.Client
}

type Result struct {
	Version string
	Asset   string
	Target  string
	DryRun  bool
}

func Update(ctx context.Context, opts Options) (Result, error) {
	if opts.Repo == "" {
		opts.Repo = DefaultRepo
	}
	if opts.GOOS == "" {
		opts.GOOS = runtime.GOOS
	}
	if opts.GOARCH == "" {
		opts.GOARCH = runtime.GOARCH
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: 5 * time.Minute}
	}
	version := strings.TrimSpace(opts.Version)
	if version == "" {
		v, err := resolveLatest(ctx, opts)
		if err != nil {
			return Result{}, err
		}
		version = v
	}
	if version == "" {
		return Result{}, errors.New("release version is empty")
	}
	asset, err := archiveName(version, opts.GOOS, opts.GOARCH)
	if err != nil {
		return Result{}, err
	}
	target, err := targetPath(opts)
	if err != nil {
		return Result{}, err
	}
	res := Result{Version: version, Asset: asset, Target: target, DryRun: opts.DryRun}
	if opts.DryRun {
		return res, nil
	}

	base := opts.DownloadBaseURL
	if base == "" {
		base = "https://github.com/" + opts.Repo + "/releases/download/" + version
	}
	base = strings.TrimRight(base, "/")
	archiveBytes, err := download(ctx, opts.HTTPClient, base+"/"+url.PathEscape(asset))
	if err != nil {
		return Result{}, err
	}
	checksumBytes, err := download(ctx, opts.HTTPClient, base+"/checksums.txt")
	if err != nil {
		return Result{}, fmt.Errorf("download checksums.txt: %w", err)
	}
	if err := verifyChecksum(asset, archiveBytes, string(checksumBytes)); err != nil {
		return Result{}, err
	}
	binary, err := extractBinary(archiveBytes)
	if err != nil {
		return Result{}, err
	}
	if err := installBinary(target, binary); err != nil {
		return Result{}, err
	}
	return res, nil
}

func archiveName(version, goos, goarch string) (string, error) {
	switch goos {
	case "darwin", "linux":
	default:
		return "", fmt.Errorf("unsupported OS %q (only linux/darwin release assets are published)", goos)
	}
	switch goarch {
	case "amd64", "arm64":
	default:
		return "", fmt.Errorf("unsupported arch %q (only amd64/arm64 release assets are published)", goarch)
	}
	return fmt.Sprintf("spark-cli_%s_%s_%s.tar.gz", strings.TrimPrefix(version, "v"), goos, goarch), nil
}

func targetPath(opts Options) (string, error) {
	if opts.InstallDir != "" {
		return filepath.Join(opts.InstallDir, "spark-cli"), nil
	}
	if opts.CurrentExecutable != "" {
		return opts.CurrentExecutable, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	return exe, nil
}

func resolveLatest(ctx context.Context, opts Options) (string, error) {
	base := strings.TrimRight(opts.GitHubBaseURL, "/")
	if base == "" {
		if tag, err := resolveLatestRedirect(ctx, opts.HTTPClient, opts.Repo); err == nil && tag != "" {
			return tag, nil
		}
		base = "https://api.github.com"
	}
	u := base + "/repos/" + opts.Repo + "/releases/latest"
	var raw struct {
		TagName string `json:"tag_name"`
	}
	b, err := download(ctx, opts.HTTPClient, u)
	if err != nil {
		return "", fmt.Errorf("resolve latest release: %w", err)
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return "", fmt.Errorf("decode latest release: %w", err)
	}
	if raw.TagName == "" {
		return "", errors.New("latest release response did not contain tag_name")
	}
	return raw.TagName, nil
}

func resolveLatestRedirect(ctx context.Context, client *http.Client, repo string) (string, error) {
	u := "https://github.com/" + repo + "/releases/latest"
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.Request == nil || resp.Request.URL == nil {
		return "", errors.New("latest redirect did not resolve")
	}
	tag := pathBase(resp.Request.URL.Path)
	if tag == "" || tag == "latest" {
		return "", errors.New("latest redirect did not include a release tag")
	}
	return tag, nil
}

func pathBase(p string) string {
	p = strings.TrimRight(p, "/")
	if p == "" {
		return ""
	}
	i := strings.LastIndex(p, "/")
	if i >= 0 {
		return p[i+1:]
	}
	return p
}

func download(ctx context.Context, client *http.Client, u string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", u, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func verifyChecksum(asset string, body []byte, checksums string) error {
	expected := ""
	for _, line := range strings.Split(checksums, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == asset {
			expected = fields[0]
			break
		}
	}
	if expected == "" {
		return fmt.Errorf("checksums.txt has no entry for %s", asset)
	}
	sum := sha256.Sum256(body)
	actual := hex.EncodeToString(sum[:])
	if !strings.EqualFold(expected, actual) {
		return fmt.Errorf("checksum mismatch for %s (expected %s, got %s)", asset, expected, actual)
	}
	return nil
}

func extractBinary(body []byte) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if filepath.Base(h.Name) != "spark-cli" || h.FileInfo().IsDir() {
			continue
		}
		return io.ReadAll(tr)
	}
	return nil, errors.New("binary spark-cli not found in archive")
}

func installBinary(target string, body []byte) error {
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".spark-cli-update-*")
	if err != nil {
		return fmt.Errorf("create temp file in %s: %w (run with sudo or pass --install-dir to a writable directory)", dir, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o755); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, target)
}
