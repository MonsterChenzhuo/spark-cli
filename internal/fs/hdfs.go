package fs

import (
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	hdfs "github.com/colinmarc/hdfs/v2"
)

type HDFS struct {
	client *hdfs.Client
	addr   string
}

func NewHDFS(addr, user string) (*HDFS, error) {
	if user == "" {
		user = os.Getenv("USER")
	}
	opts := hdfs.ClientOptions{Addresses: []string{addr}, User: user}
	c, err := hdfs.NewClient(opts)
	if err != nil {
		return nil, err
	}
	return &HDFS{client: c, addr: addr}, nil
}

func (h *HDFS) Close() error { return h.client.Close() }

func uriHDFSPath(uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	if u.Scheme != "hdfs" {
		return "", os.ErrInvalid
	}
	return u.Path, nil
}

func (h *HDFS) Open(uri string) (io.ReadCloser, error) {
	p, err := uriHDFSPath(uri)
	if err != nil {
		return nil, err
	}
	return h.client.Open(p)
}

func (h *HDFS) Stat(uri string) (FileInfo, error) {
	p, err := uriHDFSPath(uri)
	if err != nil {
		return FileInfo{}, err
	}
	st, err := h.client.Stat(p)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{URI: uri, Name: st.Name(), Size: st.Size(), IsDir: st.IsDir()}, nil
}

func (h *HDFS) List(dirURI, prefix string) ([]string, error) {
	p, err := uriHDFSPath(dirURI)
	if err != nil {
		return nil, err
	}
	entries, err := h.client.ReadDir(p)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) {
			out = append(out, "hdfs://"+h.addr+path.Join(p, e.Name()))
		}
	}
	return out, nil
}
