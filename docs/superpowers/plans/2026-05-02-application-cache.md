# Application Cache Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a `gob+zstd` disk cache for `*model.Application` keyed by source file mtime+size+partcount so that `diagnose → drill-down` workflows skip re-parsing (target: 7s reparse → ~200ms hit).

**Architecture:** New `internal/cache` package between `eventlog.Locator` and `eventlog.Open+Decode+Aggregate` in `cmd/scenarios/runner.Run`. Cache files live at `<cache_dir>/<appId>.gob.zst`, header carries `SchemaVersion + sourceKey + Application`. Failures degrade silently to "miss + reparse". `--no-cache` flag bypasses both reads and writes.

**Tech Stack:** Go 1.22 stdlib `encoding/gob` + existing `github.com/klauspost/compress/zstd` (no new direct deps). `tdigest.Centroid` for stats round-trip.

**Reference spec:** `docs/superpowers/specs/2026-05-02-application-cache-design.md`

---

## File Structure

**Created:**
- `internal/cache/cache.go` — `Cache` type, `New`/`Disabled`, `Put`/`Get`, `Path`, `defaultDir`
- `internal/cache/envelope.go` — `cacheEnvelope`, `sourceKey`, `currentSchemaVersion` const
- `internal/cache/source_key.go` — compute `sourceKey` from `eventlog.LogSource` + `fs.FS`
- `internal/cache/cache_test.go` — unit tests for round-trip / miss / atomicity / disabled
- `tests/e2e/cache_e2e_test.go` — e2e hit/miss/no-cache flag (build tag `e2e`)

**Modified:**
- `internal/fs/fs.go` — add `ModTime int64` to `FileInfo`
- `internal/fs/local.go:42-52` — fill `ModTime` in `Local.Stat`
- `internal/fs/hdfs.go:66-76` — fill `ModTime` in `HDFS.Stat`
- `internal/fs/local_test.go` — assert `ModTime > 0` after writing fixture
- `internal/stats/tdigest.go` — add `GobEncode`/`GobDecode` to `*Digest`
- `internal/stats/tdigest_test.go` — round-trip test
- `internal/config/config.go` — add `CacheConfig{Dir string}`, env var, flag override
- `internal/config/config_test.go` — assert env + flag precedence
- `cmd/scenarios/state.go` — add `CacheDir`, `NoCache` fields + flags
- `cmd/scenarios/register.go` — pass cache fields into `Options`
- `cmd/scenarios/runner.go` — `Options.CacheDir` + `Options.NoCache`; `buildCache`; thread cache between Resolve and parseApp
- `cmd/configcmd/show.go` — print `cache.dir` + source
- `CLAUDE.md` — new "缓存层" section + bump-schema踩坑 entry
- `README.md` / `README.zh.md` — Configuration section adds cache flags
- `CHANGELOG.md` / `CHANGELOG.zh.md` — Unreleased section
- `.claude/skills/spark/SKILL.md` — add `--no-cache` to flags list

---

## Task 1: Add `ModTime` to `fs.FileInfo` and fill it in both backends

**Files:**
- Modify: `internal/fs/fs.go:6-11`
- Modify: `internal/fs/local.go:42-52`
- Modify: `internal/fs/hdfs.go:66-76`
- Modify: `internal/fs/local_test.go`

- [ ] **Step 1: Write the failing test for Local.Stat ModTime**

Append to `internal/fs/local_test.go`:

```go
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
```

If `time` not yet imported in this file, add it.

- [ ] **Step 2: Run the test, confirm it fails**

Run: `go test -run TestLocalStatReturnsModTime ./internal/fs/...`
Expected: FAIL — `st.ModTime undefined (type FileInfo has no field or method ModTime)`

- [ ] **Step 3: Add ModTime field to FileInfo**

Edit `internal/fs/fs.go`:

```go
type FileInfo struct {
	URI     string
	Name    string
	Size    int64
	ModTime int64 // unix nanoseconds; 0 if backend doesn't expose mtime
	IsDir   bool
}
```

- [ ] **Step 4: Fill ModTime in `Local.Stat`**

Edit `internal/fs/local.go` Stat function (line 42-52), replace the `return FileInfo{...}` line:

```go
func (l *Local) Stat(uri string) (FileInfo, error) {
	p, err := uriToPath(uri)
	if err != nil {
		return FileInfo{}, err
	}
	st, err := os.Stat(p)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{
		URI:     uri,
		Name:    st.Name(),
		Size:    st.Size(),
		ModTime: st.ModTime().UnixNano(),
		IsDir:   st.IsDir(),
	}, nil
}
```

- [ ] **Step 5: Fill ModTime in `HDFS.Stat`**

Edit `internal/fs/hdfs.go` Stat function (line 66-76):

```go
func (h *HDFS) Stat(uri string) (FileInfo, error) {
	p, err := uriHDFSPath(uri)
	if err != nil {
		return FileInfo{}, err
	}
	st, err := h.client.Stat(p)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{
		URI:     uri,
		Name:    st.Name(),
		Size:    st.Size(),
		ModTime: st.ModTime().UnixNano(),
		IsDir:   st.IsDir(),
	}, nil
}
```

> `colinmarc/hdfs/v2`'s `FileInfo` implements `os.FileInfo`, which exposes `ModTime() time.Time`. Hadoop returns ms precision, so the low 6 ns digits will be 0 — fine for our use.

- [ ] **Step 6: Run the test, confirm it passes**

Run: `go test -run TestLocalStatReturnsModTime ./internal/fs/...`
Expected: PASS

- [ ] **Step 7: Run all fs unit tests + vet**

Run: `go test -race -count=1 ./internal/fs/... && go vet ./...`
Expected: all PASS, no vet issues. Pre-existing `TestResolveV*` etc. should be unaffected (they don't read ModTime).

- [ ] **Step 8: Commit**

```bash
git add internal/fs/fs.go internal/fs/local.go internal/fs/hdfs.go internal/fs/local_test.go
git commit -m "$(cat <<'EOF'
feat(fs): expose ModTime on FileInfo

Both Local.Stat and HDFS.Stat now populate ModTime as unix nanoseconds.
Required for the upcoming Application cache to detect source changes.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Gob round-trip for `stats.Digest`

**Files:**
- Modify: `internal/stats/tdigest.go`
- Modify: `internal/stats/tdigest_test.go`

- [ ] **Step 1: Write the failing round-trip test**

Append to `internal/stats/tdigest_test.go`:

```go
import (
	"bytes"
	"encoding/gob"
)

func TestDigestGobRoundTrip(t *testing.T) {
	d := NewDigest()
	for i := 1; i <= 1000; i++ {
		d.Add(float64(i))
	}
	wantP50 := d.Quantile(0.5)
	wantP99 := d.Quantile(0.99)

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(d); err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got Digest
	if err := gob.NewDecoder(&buf).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Count() != d.Count() {
		t.Errorf("Count mismatch got=%d want=%d", got.Count(), d.Count())
	}
	gotP50 := got.Quantile(0.5)
	gotP99 := got.Quantile(0.99)
	if abs(gotP50-wantP50) > wantP50*0.005 {
		t.Errorf("P50 drift gotP50=%f wantP50=%f", gotP50, wantP50)
	}
	if abs(gotP99-wantP99) > wantP99*0.005 {
		t.Errorf("P99 drift gotP99=%f wantP99=%f", gotP99, wantP99)
	}
}

func TestDigestGobRoundTripEmpty(t *testing.T) {
	d := NewDigest()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(d); err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got Digest
	if err := gob.NewDecoder(&buf).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Count() != 0 {
		t.Errorf("Count = %d want 0", got.Count())
	}
	if got.Quantile(0.5) != 0 {
		t.Errorf("empty Quantile = %f want 0", got.Quantile(0.5))
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
```

- [ ] **Step 2: Run test, confirm fail**

Run: `go test -run TestDigestGob ./internal/stats/...`
Expected: FAIL — gob will refuse to encode `Digest` because all its fields are unexported.

- [ ] **Step 3: Add GobEncode / GobDecode methods**

Edit `internal/stats/tdigest.go`. Replace the entire file with:

```go
// Package stats wraps t-digest for percentile estimates.
package stats

import (
	"bytes"
	"encoding/gob"

	"github.com/influxdata/tdigest"
)

type Digest struct {
	td    *tdigest.TDigest
	count int
}

func NewDigest() *Digest {
	return &Digest{td: tdigest.NewWithCompression(100)}
}

func (d *Digest) Add(v float64) {
	d.td.Add(v, 1)
	d.count++
}

func (d *Digest) Count() int { return d.count }

func (d *Digest) Quantile(q float64) float64 {
	if d.count == 0 {
		return 0
	}
	return d.td.Quantile(q)
}

// digestWire is the on-wire form for gob: just count + flat centroid list.
// Reconstructing a t-digest from its centroids reproduces quantiles within
// numerical tolerance (asserted by TestDigestGobRoundTrip).
type digestWire struct {
	Count     int
	Centroids []tdigest.Centroid
}

// GobEncode satisfies encoding/gob's Marshaler interface so that callers can
// gob-encode an entire *model.Application that embeds *Digest values.
func (d *Digest) GobEncode() ([]byte, error) {
	w := digestWire{Count: d.count}
	if d.td != nil {
		w.Centroids = []tdigest.Centroid(d.td.Centroids())
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(w); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GobDecode rebuilds the digest from the wire form. A fresh TDigest is
// allocated even if Count == 0, so a decoded zero-Digest is safe to Add to.
func (d *Digest) GobDecode(data []byte) error {
	var w digestWire
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&w); err != nil {
		return err
	}
	d.td = tdigest.NewWithCompression(100)
	if len(w.Centroids) > 0 {
		d.td.AddCentroidList(tdigest.CentroidList(w.Centroids))
	}
	d.count = w.Count
	return nil
}
```

- [ ] **Step 4: Run round-trip tests, confirm pass**

Run: `go test -run TestDigestGob ./internal/stats/... -v`
Expected: PASS for both `TestDigestGobRoundTrip` and `TestDigestGobRoundTripEmpty`.

- [ ] **Step 5: Run all stats tests + race detector**

Run: `go test -race -count=1 ./internal/stats/...`
Expected: all PASS, including pre-existing `TestDigestEmptyReturnsZero` and `TestDigestRoughPercentile`.

- [ ] **Step 6: Commit**

```bash
git add internal/stats/tdigest.go internal/stats/tdigest_test.go
git commit -m "$(cat <<'EOF'
feat(stats): gob round-trip for Digest

Implements GobEncoder/GobDecoder on *stats.Digest by serializing the centroid
list. Required so cached *model.Application values survive a gob round-trip
without exposing tdigest internals.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Cache envelope + sourceKey types

**Files:**
- Create: `internal/cache/envelope.go`
- Create: `internal/cache/source_key.go`
- Create: `internal/cache/source_key_test.go`

- [ ] **Step 1: Write `envelope.go`**

```go
// Package cache persists parsed *model.Application snapshots so subsequent
// spark-cli invocations on the same EventLog skip the Open+Decode+Aggregate
// pipeline. See docs/superpowers/specs/2026-05-02-application-cache-design.md.
package cache

import "github.com/opay-bigdata/spark-cli/internal/model"

// currentSchemaVersion is bumped manually whenever model field TYPES change or
// fields are renamed. Adding/removing fields is gob-tolerant and does NOT
// require a bump. Mismatched versions cause cache miss + silent rebuild.
const currentSchemaVersion = 1

// cacheEnvelope is the on-disk payload, gob-encoded then zstd-compressed.
type cacheEnvelope struct {
	SchemaVersion int
	CLIVersion    string
	SourceKind    string // "v1" | "v2"
	SourceKey     sourceKey
	App           *model.Application
}

// sourceKey is the fingerprint used to detect source changes. For V1 logs
// PartCount is always 1 and MaxMtime / TotalSize describe the single file.
// For V2, MaxMtime is the maximum across all parts; TotalSize is the sum;
// PartCount is the count of events_N_* parts.
type sourceKey struct {
	URI       string
	MaxMtime  int64
	TotalSize int64
	PartCount int
}
```

- [ ] **Step 2: Write `source_key.go`**

```go
package cache

import (
	"github.com/opay-bigdata/spark-cli/internal/eventlog"
	"github.com/opay-bigdata/spark-cli/internal/fs"
)

// computeSourceKey stats the underlying source(s) and returns the fingerprint.
// For V1, it stats the single LogSource.URI. For V2, it stats every Part.
// Any Stat error returns ok=false; the caller should treat that as miss.
func computeSourceKey(src eventlog.LogSource, fsys fs.FS) (sourceKey, bool) {
	switch src.Format {
	case "v1":
		st, err := fsys.Stat(src.URI)
		if err != nil {
			return sourceKey{}, false
		}
		return sourceKey{
			URI:       src.URI,
			MaxMtime:  st.ModTime,
			TotalSize: st.Size,
			PartCount: 1,
		}, true
	case "v2":
		var maxMtime, total int64
		for _, p := range src.Parts {
			st, err := fsys.Stat(p)
			if err != nil {
				return sourceKey{}, false
			}
			if st.ModTime > maxMtime {
				maxMtime = st.ModTime
			}
			total += st.Size
		}
		return sourceKey{
			URI:       src.URI,
			MaxMtime:  maxMtime,
			TotalSize: total,
			PartCount: len(src.Parts),
		}, true
	default:
		return sourceKey{}, false
	}
}
```

- [ ] **Step 3: Write `source_key_test.go`**

```go
package cache

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opay-bigdata/spark-cli/internal/eventlog"
	"github.com/opay-bigdata/spark-cli/internal/fs"
)

func TestComputeSourceKeyV1(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "application_1_a")
	if err := os.WriteFile(p, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	src := eventlog.LogSource{URI: "file://" + p, Format: "v1"}
	key, ok := computeSourceKey(src, fs.NewLocal())
	if !ok {
		t.Fatal("ok=false")
	}
	if key.PartCount != 1 || key.TotalSize != 5 || key.MaxMtime == 0 {
		t.Errorf("bad key: %+v", key)
	}
	if key.URI != src.URI {
		t.Errorf("URI: got %s want %s", key.URI, src.URI)
	}
}

func TestComputeSourceKeyV1Missing(t *testing.T) {
	src := eventlog.LogSource{URI: "file:///does/not/exist", Format: "v1"}
	if _, ok := computeSourceKey(src, fs.NewLocal()); ok {
		t.Fatal("ok=true for missing file")
	}
}

func TestComputeSourceKeyV2(t *testing.T) {
	dir := t.TempDir()
	mk := func(name, body string) string {
		full := filepath.Join(dir, name)
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return "file://" + full
	}
	p1 := mk("events_1", "aa")    // size 2
	time.Sleep(10 * time.Millisecond)
	p2 := mk("events_2", "bbbb")  // size 4
	time.Sleep(10 * time.Millisecond)
	p3 := mk("events_3", "cccccc") // size 6
	src := eventlog.LogSource{
		URI:    "file://" + dir,
		Format: "v2",
		Parts:  []string{p1, p2, p3},
	}
	key, ok := computeSourceKey(src, fs.NewLocal())
	if !ok {
		t.Fatal("ok=false")
	}
	if key.PartCount != 3 {
		t.Errorf("PartCount=%d want 3", key.PartCount)
	}
	if key.TotalSize != 12 {
		t.Errorf("TotalSize=%d want 12", key.TotalSize)
	}
	// Max mtime should be from p3 (last written)
	st3, _ := fs.NewLocal().Stat(p3)
	if key.MaxMtime != st3.ModTime {
		t.Errorf("MaxMtime=%d want %d", key.MaxMtime, st3.ModTime)
	}
}
```

- [ ] **Step 4: Verify compile + tests pass**

Run: `go test -race -count=1 ./internal/cache/...`
Expected: all PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/cache/envelope.go internal/cache/source_key.go internal/cache/source_key_test.go
git commit -m "$(cat <<'EOF'
feat(cache): envelope + sourceKey types

Introduces internal/cache with the on-disk cacheEnvelope shape and the
sourceKey fingerprint (URI + MaxMtime + TotalSize + PartCount) covering both
V1 single-file and V2 multi-part EventLogs.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `Cache.Put` (write side)

**Files:**
- Create: `internal/cache/cache.go`
- Create: `internal/cache/cache_test.go`

- [ ] **Step 1: Write the failing Put round-trip test**

Create `internal/cache/cache_test.go`:

```go
package cache

import (
	"os"
	"path/filepath"
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
	// no panic, no file write — there's no dir to even check
}
```

- [ ] **Step 2: Run, confirm fail**

Run: `go test -run TestPut ./internal/cache/...`
Expected: FAIL — `cache.New`, `cache.Disabled`, `(*Cache).Put`, `(*Cache).Path` undefined.

- [ ] **Step 3: Implement `cache.go` (Put + helpers, Get stubbed)**

Create `internal/cache/cache.go`:

```go
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

// New returns a Cache that writes under dir. If dir is empty or MkdirAll fails
// the returned cache is disabled (Get always misses, Put is a noop).
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

// Path returns the absolute path of the cache file for the given LogSource.
// It is exported for tests / debug output.
func (c *Cache) Path(src eventlog.LogSource) string {
	if !c.enabled {
		return ""
	}
	return filepath.Join(c.dir, cacheFileName(src))
}

// cacheFileName derives a single filename per appId. We use the V1 file's
// basename or the V2 directory's basename — both are already-sanitized
// `application_<id>_<attempt>` style strings.
func cacheFileName(src eventlog.LogSource) string {
	base := uriBase(src.URI)
	// Strip codec / .inprogress so v1 .zstd and bare share one cache slot.
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
		warn("cache put: stat source failed")
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
		warn("cache put: zstd writer: %v", err)
		return
	}
	if err := gob.NewEncoder(zw).Encode(env); err != nil {
		_ = zw.Close()
		warn("cache put: gob encode: %v", err)
		return
	}
	if err := zw.Close(); err != nil {
		warn("cache put: zstd close: %v", err)
		return
	}

	final := c.Path(src)
	tmp := fmt.Sprintf("%s.tmp.%d", final, os.Getpid())
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		warn("cache put: write %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, final); err != nil {
		warn("cache put: rename %s -> %s: %v", tmp, final, err)
		_ = os.Remove(tmp)
	}
}

// Get always misses for now; real implementation lands in Task 5.
func (c *Cache) Get(src eventlog.LogSource, fsys fs.FS) (*model.Application, bool) {
	return nil, false
}

// cliVersion is overridden via -ldflags at build time but Cache uses it only
// for debug info, so a default constant is fine for the in-tree case.
var cliVersion = "dev"

func warn(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "warn: cache "+format+"\n", args...)
}
```

- [ ] **Step 4: Re-run Put tests**

Run: `go test -run TestPut ./internal/cache/... -v`
Expected: PASS for `TestPutCreatesFile`, `TestPutSkipsInprogress`, `TestPutDisabledIsNoop`.

- [ ] **Step 5: Add atomic-rename safety test**

Append to `internal/cache/cache_test.go`:

```go
import "sync"

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

	// Final file should exist + be readable as a non-empty regular file.
	st, err := os.Stat(c.Path(src))
	if err != nil {
		t.Fatalf("missing final cache file: %v", err)
	}
	if st.Size() == 0 {
		t.Fatal("final cache file is empty")
	}
	// No leftover .tmp.* files (allow a brief grace by re-listing).
	entries, _ := os.ReadDir(cacheDir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".zst" && !hasSuffix(e.Name(), ".gob.zst") {
			t.Errorf("stray file in cache dir: %s", e.Name())
		}
	}
}
```

> Note: `sync` import goes into the existing import block — don't create a duplicate one.

- [ ] **Step 6: Run concurrent test**

Run: `go test -run TestPutConcurrentSurvives ./internal/cache/... -race -count=1`
Expected: PASS, no leftover `.tmp.*` files.

- [ ] **Step 7: Commit**

```bash
git add internal/cache/cache.go internal/cache/cache_test.go
git commit -m "$(cat <<'EOF'
feat(cache): Cache.Put writes gob+zstd snapshots

Atomic rename via <file>.tmp.<pid>; skips .inprogress sources; disabled cache
is a no-op. Get is stubbed to always miss — real read lands next.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: `Cache.Get` (read side) + invalidation tests

**Files:**
- Modify: `internal/cache/cache.go`
- Modify: `internal/cache/cache_test.go`

- [ ] **Step 1: Write the failing round-trip test**

Append to `internal/cache/cache_test.go`:

```go
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

	// Append bytes -> size changes
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

	// Bump mtime forward; keep size the same
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

	// Forge a stale schemaVersion by re-writing the cache file with version 0.
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
	// Corrupt file should be removed so next Put can create cleanly.
	if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
		t.Errorf("corrupt cache file still present: err=%v", err)
	}
}

// rewriteEnvelope replaces an existing cache file with a hand-crafted envelope.
// Used to simulate stale or partial writes.
func rewriteEnvelope(t *testing.T, p string, env cacheEnvelope) {
	t.Helper()
	var buf bytes.Buffer
	zw, _ := zstd.NewWriter(&buf)
	if err := gob.NewEncoder(zw).Encode(env); err != nil {
		t.Fatal(err)
	}
	_ = zw.Close()
	if err := os.WriteFile(p, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}
```

> Add new imports needed at top of file: `"bytes"`, `"encoding/gob"`, `"github.com/klauspost/compress/zstd"`, `"time"`. Keep them in the existing import block.

- [ ] **Step 2: Run, confirm hit / miss tests fail**

Run: `go test ./internal/cache/...`
Expected: `TestPutThenGetHits` fails (Get is stubbed to return miss); `TestGetMissOnCorruptFile` fails (we never delete corrupt files); other miss tests pass coincidentally because Get always returns miss.

- [ ] **Step 3: Implement Get for real**

Replace the existing stub of `func (c *Cache) Get` in `internal/cache/cache.go` with:

```go
// Get returns the cached *Application iff the on-disk envelope's schemaVersion
// matches and its sourceKey equals the freshly-computed key. Any failure on
// the read path degrades to (nil, false): file missing, decode error, schema
// or key mismatch all behave the same way. Corrupt cache files are deleted
// so the next Put can rewrite cleanly.
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
		removeCorrupt(final, "zstd reader: %v", err)
		return nil, false
	}
	defer zr.Close()

	var env cacheEnvelope
	if err := gob.NewDecoder(zr).Decode(&env); err != nil {
		removeCorrupt(final, "gob decode: %v", err)
		return nil, false
	}
	if env.SchemaVersion != currentSchemaVersion {
		removeCorrupt(final, "schemaVersion %d != %d", env.SchemaVersion, currentSchemaVersion)
		return nil, false
	}
	if env.SourceKey != cur {
		// Not corruption — source genuinely changed. Don't warn loudly.
		_ = os.Remove(final)
		return nil, false
	}
	return env.App, true
}

func removeCorrupt(path, format string, args ...any) {
	warn("get: "+format+"; removing %s", append(args, path)...)
	_ = os.Remove(path)
}
```

> Note `zstd.Decoder` is exposed as `*zstd.Decoder`; `zstd.NewReader(r)` returns it. We call `.Close()` on a defer; underlying type implements `io.ReadCloser` since the project already wraps it in `internal/eventlog/reader.go`.

- [ ] **Step 4: Re-run all cache tests**

Run: `go test -race -count=1 ./internal/cache/...`
Expected: all PASS (including round-trip, miss-on-size, miss-on-mtime, miss-on-schema, miss-on-corrupt).

- [ ] **Step 5: Commit**

```bash
git add internal/cache/cache.go internal/cache/cache_test.go
git commit -m "$(cat <<'EOF'
feat(cache): Cache.Get with mtime+size invalidation

Reads the gob+zstd envelope; misses on schema mismatch, source mtime/size
drift, or any decode error. Corrupt files are deleted so subsequent Put
can rewrite cleanly. All errors degrade to miss; nothing is surfaced to
the caller.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Wire `--cache-dir` / `--no-cache` through config + cobra

**Files:**
- Modify: `internal/cache/cache.go` (add `DefaultDir`)
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `cmd/scenarios/state.go`
- Modify: `cmd/scenarios/register.go`
- Modify: `cmd/scenarios/runner.go`
- Modify: `cmd/configcmd/show.go`
- Modify: `cmd/configcmd/show_test.go`

- [ ] **Step 1: Add `DefaultDir` helper to `internal/cache/cache.go`**

Append to `internal/cache/cache.go`:

```go
// DefaultDir returns the default cache directory: $XDG_CACHE_HOME/spark-cli
// or ~/.cache/spark-cli. Returns "" if neither can be resolved (Cache.New("")
// then yields a Disabled cache).
func DefaultDir() string {
	if v := os.Getenv("XDG_CACHE_HOME"); v != "" {
		return filepath.Join(v, "spark-cli")
	}
	if h, err := os.UserHomeDir(); err == nil {
		return filepath.Join(h, ".cache", "spark-cli")
	}
	return ""
}
```

- [ ] **Step 2: Extend Config with cache section + env**

Edit `internal/config/config.go`. After the `HDFSConfig` block add:

```go
type CacheConfig struct {
	// Dir is the persistent cache directory. Empty falls back to
	// XDG_CACHE_HOME/spark-cli or ~/.cache/spark-cli (resolved by
	// internal/cache.DefaultDir at use time, not load time).
	Dir string `yaml:"dir"`
}
```

Inside the `Config` struct add:

```go
Cache CacheConfig `yaml:"cache"`
```

Inside the `raw` struct in `Load()` add:

```go
Cache CacheConfig `yaml:"cache"`
```

After `cfg.HDFS = raw.HDFS` add:

```go
cfg.Cache = raw.Cache
```

In `ApplyEnv` after the HDFS conf-dir block add:

```go
if v := os.Getenv("SPARK_CLI_CACHE_DIR"); v != "" {
    cfg.Cache.Dir = v
}
```

In `FlagOverrides` add field:

```go
CacheDir string
```

In `ApplyFlags` add:

```go
if f.CacheDir != "" {
    cfg.Cache.Dir = f.CacheDir
}
```

- [ ] **Step 3: Test config env precedence**

Append to `internal/config/config_test.go`:

```go
func TestApplyEnvCacheDir(t *testing.T) {
	t.Setenv("SPARK_CLI_CACHE_DIR", "/tmp/spark-cli-cache")
	cfg := &Config{}
	ApplyEnv(cfg)
	if cfg.Cache.Dir != "/tmp/spark-cli-cache" {
		t.Errorf("Cache.Dir=%q want /tmp/spark-cli-cache", cfg.Cache.Dir)
	}
}

func TestApplyFlagsCacheDir(t *testing.T) {
	cfg := &Config{Cache: CacheConfig{Dir: "/old"}}
	ApplyFlags(cfg, FlagOverrides{CacheDir: "/new"})
	if cfg.Cache.Dir != "/new" {
		t.Errorf("Cache.Dir=%q want /new", cfg.Cache.Dir)
	}
}
```

- [ ] **Step 4: Add CLI flags + thread through scenarios**

Edit `cmd/scenarios/state.go`. In `globalState` add:

```go
CacheDir string
NoCache  bool
```

In `RegisterFlags` (after the existing `--no-progress` line) add:

```go
root.PersistentFlags().StringVar(&state.CacheDir, "cache-dir", state.CacheDir, "Directory for the parsed-application cache (defaults to $XDG_CACHE_HOME/spark-cli or ~/.cache/spark-cli)")
root.PersistentFlags().BoolVar(&state.NoCache, "no-cache", state.NoCache, "Bypass the parsed-application cache for this invocation (do not read or write)")
```

Edit `cmd/scenarios/register.go`'s `buildOpts` to add:

```go
CacheDir: state.CacheDir,
NoCache:  state.NoCache,
```

Edit `cmd/scenarios/runner.go`. In `Options` struct after `HadoopConfDir` add:

```go
CacheDir string
NoCache  bool
```

Inside `buildConfig`, after the `HadoopConfDir` block add:

```go
if opts.CacheDir != "" {
    cfg.Cache.Dir = opts.CacheDir
}
```

Add a helper function below `buildHDFS`:

```go
// buildCache resolves the effective cache directory (config / env / flag chain
// already settled) and returns a Cache. NoCache forces Disabled.
func buildCache(cfg *config.Config, noCache bool) *cache.Cache {
    if noCache {
        return cache.Disabled()
    }
    dir := cfg.Cache.Dir
    if dir == "" {
        dir = cache.DefaultDir()
    }
    return cache.New(dir)
}
```

Add the import `"github.com/opay-bigdata/spark-cli/internal/cache"`.

In `Run`, after `loc.Resolve` (line ~63) and before `parseApp`, insert the cache lookup:

```go
fsys, err := fsForURI(fsByScheme, src.URI)
if err != nil {
    return writeErr(opts.Stderr, err)
}
ch := buildCache(cfg, opts.NoCache)

env := scenario.Envelope{
    Scenario:    opts.Scenario,
    AppID:       resolvedAppID,
    LogPath:     logPath,
    LogFormat:   src.Format,
    Compression: string(src.Compression),
    Incomplete:  src.Incomplete,
}

if opts.DryRun {
    // (existing dry-run block — unchanged)
}

start := time.Now()
if cached, hit := ch.Get(src, fsys); hit {
    env.AppName = cached.Name
    env.ParsedEvents = 0
    env.ElapsedMs = time.Since(start).Milliseconds()
    if err := buildScenarioBody(opts, cached, &env); err != nil {
        return writeErr(opts.Stderr, err)
    }
    return render(opts, env)
}
app, parsed, err := parseApp(fsByScheme, src, resolvedAppID)
if err != nil {
    return writeErr(opts.Stderr, err)
}
ch.Put(src, fsys, app)
env.AppName = app.Name
env.ParsedEvents = parsed
env.ElapsedMs = time.Since(start).Milliseconds()

if err := buildScenarioBody(opts, app, &env); err != nil {
    return writeErr(opts.Stderr, err)
}
return render(opts, env)
```

> Note: this restructures `Run`. Read existing `Run` carefully to keep the `DryRun` block in its original position (it should run BEFORE the cache lookup since dry-run skips parsing entirely).

The exact diff is:
1. Move the `fsys, err := fsForURI(...)` + `ch := buildCache(...)` lines so they execute after Resolve and before the existing `env := scenario.Envelope{...}` block.
2. Replace the existing block from `start := time.Now()` through `env.ElapsedMs = ...` with the if/else above (cached vs parse).
3. The post-block `if err := buildScenarioBody(...)` and `return render(...)` lines stay where they are; remove duplicates introduced above.

If unsure, read the current `runner.go` `Run` body in full first and apply each change in place rather than rewriting.

- [ ] **Step 5: Update `config show` to print cache dir**

Edit `cmd/configcmd/show.go`. In `sources` struct add `Cache string`. In `detectSources`:

```go
src := sources{LogDirs: "default", HDFSUser: "default", HadoopConfDir: "default", Timeout: "default", Cache: "default"}
```

After the `src.Timeout = "file"` block (within the `if _, err := os.Stat(path); err == nil { ... }` body) add:

```go
if cfg.Cache.Dir != "" {
    src.Cache = "file"
}
```

After the `src.HadoopConfDir = "env"` block add:

```go
if os.Getenv("SPARK_CLI_CACHE_DIR") != "" {
    src.Cache = "env"
}
```

In `render`, after the `hdfs.conf_dir` line add:

```go
dir := cfg.Cache.Dir
if dir == "" {
    dir = cache.DefaultDir()
}
fmt.Fprintf(w, "cache.dir (%s): %s\n", src.Cache, dir)
```

Add the import `"github.com/opay-bigdata/spark-cli/internal/cache"`.

- [ ] **Step 6: Update show_test.go**

Edit `cmd/configcmd/show_test.go` — wherever existing tests check for substrings in the rendered output, add a check for `cache.dir`. Use the existing test as a template; if there's no equivalent assertion file, append:

```go
func TestRenderIncludesCacheDir(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", "/tmp/xdg")
	cfg := &Config{Cache: CacheConfig{}}
	src := sources{Cache: "default", LogDirs: "default", HDFSUser: "default", HadoopConfDir: "default", Timeout: "default"}
	var buf bytes.Buffer
	render(&buf, cfg, src)
	if !strings.Contains(buf.String(), "cache.dir (default): /tmp/xdg/spark-cli") {
		t.Errorf("output missing cache.dir line:\n%s", buf.String())
	}
}
```

> If `Config` and `CacheConfig` don't import correctly inside the test, alias via `import . "github.com/opay-bigdata/spark-cli/internal/config"` consistent with existing patterns. Read `show_test.go` first to see the import style.

- [ ] **Step 7: Run all gates**

Run: `make tidy && make lint && make unit-test`
Expected: all PASS, no diff in `go.mod` / `go.sum`.

- [ ] **Step 8: Commit**

```bash
git add internal/cache/cache.go \
        internal/config/config.go internal/config/config_test.go \
        cmd/scenarios/state.go cmd/scenarios/register.go cmd/scenarios/runner.go \
        cmd/configcmd/show.go cmd/configcmd/show_test.go
git commit -m "$(cat <<'EOF'
feat(cache): wire --cache-dir / --no-cache + config plumbing

CacheConfig.Dir flows from yaml -> env (SPARK_CLI_CACHE_DIR) -> --cache-dir.
runner.Run consults the cache between Locator.Resolve and parseApp; on hit
it skips Open/Decode/Aggregate entirely and reports ParsedEvents=0. config
show prints the resolved cache dir with its source.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: e2e tests for hit / miss / no-cache

**Files:**
- Create: `tests/e2e/cache_e2e_test.go`

- [ ] **Step 1: Write the e2e test file**

Create `tests/e2e/cache_e2e_test.go`:

```go
//go:build e2e
// +build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opay-bigdata/spark-cli/cmd"
)

func runCLI(t *testing.T, args []string) (map[string]any, string) {
	t.Helper()
	cmd.ResetForTest()
	var stdout, stderr bytes.Buffer
	rc := cmd.RunWith(context.Background(), args, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr.String())
	}
	var m map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &m); err != nil {
		t.Fatalf("not json: %v\n%s", err, stdout.String())
	}
	return m, stderr.String()
}

func TestE2ECacheHitSkipsParse(t *testing.T) {
	logsDir := t.TempDir()
	cacheDir := t.TempDir()
	src, err := os.ReadFile(filepath.Join("..", "testdata", "tiny_app.json"))
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(logsDir, "application_1_1")
	if err := os.WriteFile(logPath, src, 0o644); err != nil {
		t.Fatal(err)
	}

	args := []string{"app-summary", "application_1_1",
		"--log-dirs", "file://" + logsDir,
		"--cache-dir", cacheDir,
		"--format", "json",
	}

	first, _ := runCLI(t, args)
	if pe, ok := first["parsed_events"].(float64); !ok || pe == 0 {
		t.Fatalf("first run should parse events, got parsed_events=%v", first["parsed_events"])
	}

	// Cache file must exist after the first run.
	entries, err := os.ReadDir(cacheDir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("cache dir empty after first run: err=%v entries=%v", err, entries)
	}

	second, _ := runCLI(t, args)
	if pe, ok := second["parsed_events"].(float64); !ok || pe != 0 {
		t.Errorf("cache hit should set parsed_events=0, got %v", second["parsed_events"])
	}
}

func TestE2ECacheInvalidatesOnMtimeChange(t *testing.T) {
	logsDir := t.TempDir()
	cacheDir := t.TempDir()
	src, err := os.ReadFile(filepath.Join("..", "testdata", "tiny_app.json"))
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(logsDir, "application_1_1")
	if err := os.WriteFile(logPath, src, 0o644); err != nil {
		t.Fatal(err)
	}

	args := []string{"app-summary", "application_1_1",
		"--log-dirs", "file://" + logsDir,
		"--cache-dir", cacheDir,
		"--format", "json",
	}

	_, _ = runCLI(t, args) // populate cache

	// Bump mtime; size unchanged.
	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(logPath, future, future); err != nil {
		t.Fatal(err)
	}

	second, _ := runCLI(t, args)
	if pe, ok := second["parsed_events"].(float64); !ok || pe == 0 {
		t.Errorf("mtime change should force reparse, got parsed_events=%v", second["parsed_events"])
	}
}

func TestE2ENoCacheFlagSkipsWrite(t *testing.T) {
	logsDir := t.TempDir()
	cacheDir := t.TempDir()
	src, err := os.ReadFile(filepath.Join("..", "testdata", "tiny_app.json"))
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(logsDir, "application_1_1")
	if err := os.WriteFile(logPath, src, 0o644); err != nil {
		t.Fatal(err)
	}

	args := []string{"app-summary", "application_1_1",
		"--log-dirs", "file://" + logsDir,
		"--cache-dir", cacheDir,
		"--no-cache",
		"--format", "json",
	}

	_, _ = runCLI(t, args)
	entries, _ := os.ReadDir(cacheDir)
	if len(entries) != 0 {
		t.Errorf("--no-cache should not write any file, got %d entries", len(entries))
	}
}
```

- [ ] **Step 2: Run the e2e suite**

Run: `make e2e`
Expected: all e2e tests pass — `TestE2E_AllScenarios_TinyApp`, `TestE2E_AppNotFound`, plus the 3 new cache tests.

- [ ] **Step 3: Run full unit test suite again to make sure runner refactor didn't regress**

Run: `make unit-test`
Expected: all PASS.

- [ ] **Step 4: Smoke-test by hand against the bundled fixture**

```bash
mkdir -p /tmp/spark-cli-smoke
cp tests/testdata/tiny_app.json /tmp/spark-cli-smoke/application_1_1
rm -rf /tmp/spark-cli-cache
go run . diagnose application_1_1 --log-dirs file:///tmp/spark-cli-smoke --cache-dir /tmp/spark-cli-cache | head -5
go run . diagnose application_1_1 --log-dirs file:///tmp/spark-cli-smoke --cache-dir /tmp/spark-cli-cache | head -5
ls /tmp/spark-cli-cache/
```

Expected: second run returns identical envelope shape; `ls` shows `application_1_1.gob.zst`.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/cache_e2e_test.go
git commit -m "$(cat <<'EOF'
test(e2e): cache hit, mtime invalidation, --no-cache

Verifies the cache hit path returns parsed_events=0, mtime drift forces a
reparse, and --no-cache writes nothing to the cache dir.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Documentation sync

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md`
- Modify: `README.zh.md`
- Modify: `CHANGELOG.md`
- Modify: `CHANGELOG.zh.md`
- Modify: `.claude/skills/spark/SKILL.md`

- [ ] **Step 1: CLAUDE.md — add 缓存层 section + bump 踩坑**

In `CLAUDE.md` after the "## HDFS 连接" section, add a new section:

```markdown
## Application 缓存

`internal/cache` 把首次解析得到的 `*model.Application` 用 `gob+zstd` 序列化到磁盘，让同一 appId 的后续命令绕过 `Open + Decode + Aggregate`。

**缓存目录优先级**(高 → 低)：
1. `--cache-dir <path>` flag
2. `SPARK_CLI_CACHE_DIR` 环境变量
3. `config.yaml` 的 `cache.dir` 字段
4. `$XDG_CACHE_HOME/spark-cli/`
5. `~/.cache/spark-cli/`

**失效规则**：
- V1：`{URI, mtime, size}` 任一变化触发 miss
- V2：`{URI, max(part_mtime), sum(part_size), part_count)}` 任一变化触发 miss
- `.inprogress` 日志整体跳过缓存（不读不写）
- `--no-cache` 旁路缓存（不读不写）

**Schema 版本**：`internal/cache/envelope.go` 的 `currentSchemaVersion`。**字段加/删** gob 天然容忍，**不需要** bump；**字段改类型或重命名时**必须 bump，否则用户拿到的是用零值/老结构填充的 `*Application`。bump 后旧缓存自动作废重建，对用户透明。

**关键约束**：缓存层永远不能让 CLI 失败 —— 所有错误（写盘失败、损坏文件、解码错误）都退化为 "miss + 重新解析"。损坏文件读到时直接删除，下次写盘清白。
```

In "## 已知踩坑" section append a new bullet:

```markdown
- **缓存 schema 不 bump 的隐性 bug**：`internal/cache/envelope.go` 的 `currentSchemaVersion` 是手动维护的整型常量。改 `model.Application` / `Stage` / `Executor` / `BlacklistEvent` 等的字段**类型**或**重命名**字段时**必须** bump 一档，否则旧缓存会被当成有效，反序列化结果可能字段错位（gob 对类型不匹配会报错被当成 miss，但对兼容类型（如 int → int64）可能静默错位）。**只新增字段不需要 bump**。每次走完上面"添加新场景的标准步骤"之后，自检一下是否动了字段类型，动了就 bump。
```

In "## 添加新场景的标准步骤" section, after the existing 7 steps, add step 8:

```markdown
8. 若 step 2 改了 `model` 现有字段的**类型**或**名称**，bump `internal/cache/envelope.go` 的 `currentSchemaVersion` —— 仅新增字段则不需要
```

- [ ] **Step 2: README.md — Configuration section**

Find the existing "Configuration" or "Setup" section in `README.md`. Append a subsection (or extend an existing flag table):

```markdown
### Cache

The first invocation on a new EventLog persists the parsed `*model.Application`
to `<cache_dir>/<appId>.gob.zst`. Subsequent commands on the same `appId` skip
parsing and return in <300 ms regardless of source size.

| Source | Cache dir |
|---|---|
| `--cache-dir` flag | (highest priority) |
| `SPARK_CLI_CACHE_DIR` env var | |
| `config.yaml: cache.dir` | |
| `$XDG_CACHE_HOME/spark-cli` | |
| `~/.cache/spark-cli` | (default) |

`--no-cache` bypasses both the read and the write for the current invocation.
`.inprogress` logs are never cached. The cache invalidates automatically when
the source file's mtime or size changes (V1) or when any part's mtime / total
size / part count changes (V2).
```

- [ ] **Step 3: README.zh.md — same content, Chinese**

```markdown
### 缓存

第一次解析某个 EventLog 后，`*model.Application` 会以 `gob+zstd` 形式写到 `<cache_dir>/<appId>.gob.zst`。同一 appId 的后续命令直接读缓存，无论日志多大都能 <300 ms 返回。

| 来源 | 缓存目录 |
|---|---|
| `--cache-dir` flag | (最高优先级) |
| `SPARK_CLI_CACHE_DIR` 环境变量 | |
| `config.yaml: cache.dir` | |
| `$XDG_CACHE_HOME/spark-cli` | |
| `~/.cache/spark-cli` | (默认) |

`--no-cache` 让本次执行不读不写缓存。`.inprogress` 日志永远不缓存。源文件 mtime/size（V1）或任一分片 mtime / 总 size / 分片数（V2）变化都会自动失效。
```

- [ ] **Step 4: CHANGELOG.md — Unreleased section**

In `CHANGELOG.md`, under `## Unreleased`, prepend:

```markdown
### Application cache layer

- `internal/cache` persists the parsed `*model.Application` as `gob+zstd`
  blobs under `$XDG_CACHE_HOME/spark-cli/` (or `~/.cache/spark-cli/`). The
  first command on a given `appId` parses normally; subsequent commands skip
  Open + Decode + Aggregate and return in <300 ms.
- New flags `--cache-dir <path>` and `--no-cache` (bypass read+write).
  Env var `SPARK_CLI_CACHE_DIR`. YAML key `cache.dir`.
- Cache invalidates on V1 source mtime/size change or any V2 part change.
  `.inprogress` logs are never cached. `spark-cli config show` reports the
  resolved `cache.dir` and its source (flag/env/file/default).
- Cache failures (corrupt files, schema mismatch, write errors) degrade
  silently to "miss + reparse" — never surfaced as CLI errors.
```

- [ ] **Step 5: CHANGELOG.zh.md — same in Chinese**

```markdown
### Application 缓存层

- `internal/cache` 把解析后的 `*model.Application` 用 `gob+zstd` 序列化到 `$XDG_CACHE_HOME/spark-cli/`(或 `~/.cache/spark-cli/`)。同一 `appId` 的首条命令照常解析；之后的命令绕过 Open + Decode + Aggregate，<300 ms 返回。
- 新增 flag `--cache-dir <path>`、`--no-cache`(本次执行不读不写)；环境变量 `SPARK_CLI_CACHE_DIR`；YAML 字段 `cache.dir`。
- 缓存失效条件：V1 源文件 mtime/size 改变；V2 任一分片 mtime / 总 size / 分片数变化。`.inprogress` 日志永不缓存。`spark-cli config show` 输出 `cache.dir` 及其来源(flag/env/file/default)。
- 缓存失败(损坏文件、schema 不匹配、写盘失败)静默退化为 "miss + 重新解析"，绝不向用户报错。
```

- [ ] **Step 6: SKILL.md — add `--no-cache` to flags**

Edit `.claude/skills/spark/SKILL.md`. Find the "## Useful flags" section. Append:

```markdown
- `--cache-dir <path>` — persistent cache dir (default `~/.cache/spark-cli`)
- `--no-cache` — bypass the parsed-application cache for this invocation
```

- [ ] **Step 7: Run gate one more time**

Run: `make tidy && make lint && make unit-test && make e2e`
Expected: all PASS.

- [ ] **Step 8: Commit docs**

```bash
git add CLAUDE.md README.md README.zh.md CHANGELOG.md CHANGELOG.zh.md .claude/skills/spark/SKILL.md
git commit -m "$(cat <<'EOF'
docs: application cache layer

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Push

- [ ] **Step 1: Verify clean tree**

Run: `git status`
Expected: nothing to commit, working tree clean.

- [ ] **Step 2: Confirm gates one final time**

Run: `make tidy && make lint && make unit-test && make e2e`
Expected: all PASS.

- [ ] **Step 3: Push to origin/main**

Run: `git push origin main`
Expected: 8 commits land on main; release workflow auto-bumps the patch tag.

---

## Self-review checklist (executed by the planner)

**Spec coverage:**
- §1.1 单 blob per app: Tasks 4 (Path), 5 (Get).
- §1.2 sourceKey 三/四元组: Task 3 (V1+V2 tests).
- §1.3 跳 .inprogress / --no-cache / 不可写目录: Tasks 4 (Skip inprogress), 6 (--no-cache), 4 (mkdir fail → Disabled).
- §1.4 dir 优先级: Tasks 6 (config) + 8 (docs).
- §2 文件格式 gob+zstd + envelope + schemaVersion: Tasks 3, 4, 5.
- §3 包结构 + API: Tasks 3-5.
- §3.3 stats.Digest gob: Task 2.
- §3.4 fs.FileInfo.ModTime: Task 1.
- §4 接入点: Task 6 (runner.Run + flags + show).
- §5 错误矩阵: Task 5 (Get fault tolerance) + Task 4 (Put fault tolerance).
- §6 测试矩阵: Tasks 1, 2, 3, 4, 5 (units), 7 (e2e).
- §7 性能预算: handled by smoke step in Task 7.5; no hard CI assertion (per spec §6.5).
- §8 文档: Task 8.
- §9 里程碑顺序: matches Tasks 1-8.

**Placeholder scan:** No "TBD" / "TODO" / "fill in details" / vague error-handling steps remain.

**Type consistency:** `Cache.Get` / `Cache.Put` / `Cache.Path` signatures consistent across Tasks 4, 5, 6. `cacheEnvelope` / `sourceKey` field names consistent between source_key.go (Task 3) and cache.go (Tasks 4, 5). `currentSchemaVersion` const referenced in Tasks 3 and 5. `cache.DefaultDir()` defined in Task 6 step 1, consumed in Task 6 step 4 (runner) and step 5 (show).
