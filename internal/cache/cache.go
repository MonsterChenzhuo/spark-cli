package cache

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"

	"github.com/klauspost/compress/zstd"

	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
	"github.com/opay-bigdata/spark-cli/internal/eventlog"
	"github.com/opay-bigdata/spark-cli/internal/fs"
	"github.com/opay-bigdata/spark-cli/internal/model"
)

// Cache persists parsed *model.Application snapshots to <dir>/<appId>.gob.zst.
// All errors degrade silently to "miss" — the cache must never break the CLI.
type Cache struct {
	dir     string
	enabled bool
	warnW   io.Writer
}

// New returns a Cache that writes under dir. Empty dir or MkdirAll failure
// yields a Disabled cache (Get always misses, Put is a noop).
func New(dir string) *Cache {
	return NewWithWarningWriter(dir, os.Stderr)
}

// NewWithWarningWriter returns a Cache that emits JSON warning events to w.
// Empty dir or MkdirAll failure yields a Disabled cache.
func NewWithWarningWriter(dir string, w io.Writer) *Cache {
	if dir == "" {
		return Disabled()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		warn(w, "mkdir %s: %v", dir, err)
		return Disabled()
	}
	return &Cache{dir: dir, enabled: true, warnW: w}
}

// Disabled returns a no-op cache; Get always misses, Put always skips.
func Disabled() *Cache { return &Cache{enabled: false, warnW: io.Discard} }

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
		c.warn("put: stat source failed")
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
		c.warn("put: zstd writer: %v", err)
		return
	}
	if err := gob.NewEncoder(zw).Encode(env); err != nil {
		_ = zw.Close()
		c.warn("put: gob encode: %v", err)
		return
	}
	if err := zw.Close(); err != nil {
		c.warn("put: zstd close: %v", err)
		return
	}

	final := c.Path(src)
	tmpf, err := os.CreateTemp(c.dir, filepath.Base(final)+".tmp.*")
	if err != nil {
		c.warn("put: create tmp: %v", err)
		return
	}
	tmp := tmpf.Name()
	if _, err := tmpf.Write(buf.Bytes()); err != nil {
		_ = tmpf.Close()
		_ = os.Remove(tmp)
		c.warn("put: write %s: %v", tmp, err)
		return
	}
	if err := tmpf.Close(); err != nil {
		_ = os.Remove(tmp)
		c.warn("put: close %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, final); err != nil {
		c.warn("put: rename %s -> %s: %v", tmp, final, err)
		_ = os.Remove(tmp)
	}
}

// Get returns the cached *Application iff the on-disk envelope's schemaVersion
// matches and its sourceKey equals the freshly-computed key. Any failure on
// the read path degrades to (nil, false): file missing, decode error, schema
// or key mismatch all behave the same way. Corrupt or stale cache files are
// deleted so the next Put rewrites cleanly.
func (c *Cache) Get(src eventlog.LogSource, fsys fs.FS) (*model.Application, bool) {
	if !c.enabled {
		return nil, false
	}
	cur, ok := computeSourceKey(src, fsys)
	if !ok {
		return nil, false
	}
	final := c.Path(src)
	raw, err := os.ReadFile(final)
	if err != nil {
		return nil, false
	}
	zr, err := zstd.NewReader(bytes.NewReader(raw))
	if err != nil {
		c.removeCorrupt(final, "zstd reader: %v", err)
		return nil, false
	}
	defer zr.Close()

	var env cacheEnvelope
	if err := gob.NewDecoder(zr).Decode(&env); err != nil {
		c.removeCorrupt(final, "gob decode: %v", err)
		return nil, false
	}
	if env.SchemaVersion != currentSchemaVersion {
		c.removeCorrupt(final, "schemaVersion %d != %d", env.SchemaVersion, currentSchemaVersion)
		return nil, false
	}
	if env.SourceKey != cur {
		_ = os.Remove(final)
		return nil, false
	}
	return env.App, true
}

func (c *Cache) removeCorrupt(p string, format string, args ...any) {
	c.warn("get: "+format+"; removing %s", append(args, p)...)
	_ = os.Remove(p)
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

func warn(w io.Writer, format string, args ...any) {
	cerrors.WriteEventJSON(w, cerrors.Event{
		Code:    "CACHE_WARNING",
		Level:   "warn",
		Message: fmt.Sprintf("cache "+format, args...),
	})
}

func (c *Cache) warn(format string, args ...any) {
	warn(c.warnW, format, args...)
}
