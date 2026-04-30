// Package eventlog locates and reads Spark EventLog files (V1 single file or V2 directory).
package eventlog

import (
	"io"
	"strings"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type Compression string

const (
	CompressionNone   Compression = "none"
	CompressionZstd   Compression = "zstd"
	CompressionLZ4    Compression = "lz4"
	CompressionSnappy Compression = "snappy"
)

// DetectCompression strips trailing .inprogress then matches the next extension.
func DetectCompression(name string) Compression {
	n := strings.TrimSuffix(name, ".inprogress")
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
// Caller must Close the returned reader.
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
	return rc, nil
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
func (r *readClose) Close() error                { return r.raw.Close() }
