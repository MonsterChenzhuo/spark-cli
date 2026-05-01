// Package fs abstracts local and HDFS filesystems behind a small interface.
package fs

import "io"

type FileInfo struct {
	URI     string
	Name    string
	Size    int64
	ModTime int64 // unix nanoseconds; 0 if backend does not expose mtime
	IsDir   bool
}

type FS interface {
	Open(uri string) (io.ReadCloser, error)
	Stat(uri string) (FileInfo, error)
	List(dirURI, prefix string) ([]string, error) // returns full URIs
	Close() error
}
