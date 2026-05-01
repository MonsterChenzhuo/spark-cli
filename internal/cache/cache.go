package cache

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"

	"github.com/klauspost/compress/zstd"

	"github.com/opay-bigdata/spark-cli/internal/eventlog"
	"github.com/opay-bigdata/spark-cli/internal/fs"
	"github.com/opay-bigdata/spark-cli/internal/model"
)

// Cache persists parsed *model.Application snapshots to <dir>/<appId>.gob.zst.
// All errors degrade silently to "miss" — the cache must never break the CLI.
type Cache struct {
	dir     string
	enabled bool
}

// New returns a Cache that writes under dir. Empty dir or MkdirAll failure
// yields a Disabled cache (Get always misses, Put is a noop).
func New(dir string) *Cache {
	if dir == "" {
		return Disabled()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		warn("mkdir %s: %v", dir, err)
		return Disabled()
	}
	return &Cache{dir: dir, enabled: true}
}

// Disabled returns a no-op cache; Get always misses, Put always skips.
func Disabled() *Cache { return &Cache{enabled: false} }

// Enabled reports whether the cache is operational.
func (c *Cache) Enabled() bool { return c.enabled }

// Dir returns the configured cache directory ("" if disabled).
func (c *Cache) Dir() string { return c.dir }

// Path returns the absolute path of the cache file for the given LogSource.
func (c *Cache) Path(src eventlog.LogSource) string {
	if !c.enabled {
		return ""
	}
	return filepath.Join(c.dir, cacheFileName(src))
}

func cacheFileName(src eventlog.LogSource) string {
	base := uriBase(src.URI)
	for _, ext := range []string{".inprogress", ".zstd", ".lz4", ".snappy"} {
		for len(base) > len(ext) && hasSuffix(base, ext) {
			base = base[:len(base)-len(ext)]
		}
	}
	return base + ".gob.zst"
}

func hasSuffix(s, suf string) bool {
	return len(s) >= len(suf) && s[len(s)-len(suf):] == suf
}

func uriBase(uri string) string {
	if u, err := url.Parse(uri); err == nil && u.Path != "" {
		return path.Base(u.Path)
	}
	return path.Base(uri)
}

// Put writes the cache file (atomic rename). Skips .inprogress sources.
// Failures are logged once to stderr and otherwise ignored.
func (c *Cache) Put(src eventlog.LogSource, fsys fs.FS, app *model.Application) {
	if !c.enabled || src.Incomplete {
		return
	}
	key, ok := computeSourceKey(src, fsys)
	if !ok {
		warn("put: stat source failed")
		return
	}
	env := cacheEnvelope{
		SchemaVersion: currentSchemaVersion,
		CLIVersion:    cliVersion,
		SourceKind:    src.Format,
		SourceKey:     key,
		App:           app,
	}

	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		warn("put: zstd writer: %v", err)
		return
	}
	if err := gob.NewEncoder(zw).Encode(env); err != nil {
		_ = zw.Close()
		warn("put: gob encode: %v", err)
		return
	}
	if err := zw.Close(); err != nil {
		warn("put: zstd close: %v", err)
		return
	}

	final := c.Path(src)
	tmp := fmt.Sprintf("%s.tmp.%d", final, os.Getpid())
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		warn("put: write %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, final); err != nil {
		warn("put: rename %s -> %s: %v", tmp, final, err)
		_ = os.Remove(tmp)
	}
}

// Get always misses for now; the real implementation lands in Task 5.
func (c *Cache) Get(src eventlog.LogSource, fsys fs.FS) (*model.Application, bool) {
	return nil, false
}

// DefaultDir returns $XDG_CACHE_HOME/spark-cli or ~/.cache/spark-cli.
func DefaultDir() string {
	if v := os.Getenv("XDG_CACHE_HOME"); v != "" {
		return filepath.Join(v, "spark-cli")
	}
	if h, err := os.UserHomeDir(); err == nil {
		return filepath.Join(h, ".cache", "spark-cli")
	}
	return ""
}

// cliVersion is overridden via -ldflags at build time but Cache uses it only
// for debug info, so a default constant is fine for the in-tree case.
var cliVersion = "dev"

func warn(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "warn: cache "+format+"\n", args...)
}
