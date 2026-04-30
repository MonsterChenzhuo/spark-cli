// Package eventlog locates and reads Spark EventLog files (V1 single file or V2 directory).
package eventlog

import (
	"fmt"
	"io"
	"strings"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/opay-bigdata/spark-cli/internal/fs"
	"github.com/pierrec/lz4/v4"
)

// Open returns a single io.ReadCloser combining all parts of a LogSource and
// applying decompression. Incomplete sources (.inprogress) are opened normally;
// it is the parser's job to handle truncation. Caller must Close exactly once,
// though Close is idempotent for safety.
func Open(src LogSource, fsys fs.FS) (io.ReadCloser, error) {
	if src.Format == "v2" {
		if len(src.Parts) == 0 {
			return nil, fmt.Errorf("eventlog: v2 source has no parts")
		}
		readers := make([]io.Reader, 0, len(src.Parts))
		closers := make([]io.Closer, 0, len(src.Parts))
		for _, uri := range src.Parts {
			rc, err := fsys.Open(uri)
			if err != nil {
				for _, c := range closers {
					_ = c.Close()
				}
				return nil, err
			}
			readers = append(readers, rc)
			closers = append(closers, rc)
		}
		multi := &multiCloser{r: io.MultiReader(readers...), closers: closers}
		return openCompressed(multi, src.Compression)
	}
	rc, err := fsys.Open(src.URI)
	if err != nil {
		return nil, err
	}
	return openCompressed(rc, src.Compression)
}

type multiCloser struct {
	r       io.Reader
	closers []io.Closer
	closed  bool
	err     error
}

func (m *multiCloser) Read(p []byte) (int, error) { return m.r.Read(p) }
func (m *multiCloser) Close() error {
	if m.closed {
		return m.err
	}
	m.closed = true
	var first error
	for _, c := range m.closers {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	m.err = first
	return first
}

type Compression string

const (
	CompressionNone   Compression = "none"
	CompressionZstd   Compression = "zstd"
	CompressionLZ4    Compression = "lz4"
	CompressionSnappy Compression = "snappy"
)

// DetectCompression strips trailing .inprogress (repeatedly, in case of
// staggered restarts) then matches the next extension case-insensitively.
// Files with no recognized codec extension — including V2 rolling-format
// parts like events_N_application_X — return CompressionNone, matching
// Spark's default of writing uncompressed unless EVENT_LOG_COMPRESS is set.
func DetectCompression(name string) Compression {
	n := strings.ToLower(name)
	for strings.HasSuffix(n, ".inprogress") {
		n = strings.TrimSuffix(n, ".inprogress")
	}
	switch {
	case strings.HasSuffix(n, ".zstd"):
		return CompressionZstd
	case strings.HasSuffix(n, ".lz4"):
		return CompressionLZ4
	case strings.HasSuffix(n, ".snappy"):
		return CompressionSnappy
	default:
		return CompressionNone
	}
}

// openCompressed wraps the underlying reader with the appropriate decompressor.
// On success the caller must Close the returned reader, which releases both
// the decoder and rc. On error rc has already been closed.
func openCompressed(rc io.ReadCloser, c Compression) (io.ReadCloser, error) {
	switch c {
	case CompressionNone:
		return rc, nil
	case CompressionZstd:
		zr, err := zstd.NewReader(rc)
		if err != nil {
			_ = rc.Close()
			return nil, err
		}
		return &zstdCloser{zr: zr, raw: rc}, nil
	case CompressionLZ4:
		return &readClose{r: lz4.NewReader(rc), raw: rc}, nil
	case CompressionSnappy:
		return &readClose{r: snappy.NewReader(rc), raw: rc}, nil
	}
	_ = rc.Close()
	return nil, fmt.Errorf("eventlog: unsupported compression %q", c)
}

type zstdCloser struct {
	zr  *zstd.Decoder
	raw io.Closer
}

func (z *zstdCloser) Read(p []byte) (int, error) { return z.zr.Read(p) }
func (z *zstdCloser) Close() error               { z.zr.Close(); return z.raw.Close() }

type readClose struct {
	r   io.Reader
	raw io.Closer
}

func (r *readClose) Read(p []byte) (int, error) { return r.r.Read(p) }
func (r *readClose) Close() error               { return r.raw.Close() }
