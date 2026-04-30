package fs

import (
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type Local struct{}

func NewLocal() *Local { return &Local{} }

func uriToPath(uri string) (string, error) {
	if !strings.HasPrefix(uri, "file://") {
		return uri, nil
	}
	u, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	return u.Path, nil
}

func (l *Local) Open(uri string) (io.ReadCloser, error) {
	p, err := uriToPath(uri)
	if err != nil {
		return nil, err
	}
	return os.Open(p)
}

func (l *Local) Stat(uri string) (FileInfo, error) {
	p, err := uriToPath(uri)
	if err != nil {
		return FileInfo{}, err
	}
	st, err := os.Stat(p)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{URI: uri, Name: st.Name(), Size: st.Size(), IsDir: st.IsDir()}, nil
}

func (l *Local) List(dirURI, prefix string) ([]string, error) {
	p, err := uriToPath(dirURI)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(p)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) {
			out = append(out, "file://"+filepath.Join(p, e.Name()))
		}
	}
	return out, nil
}
