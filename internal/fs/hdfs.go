package fs

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	hdfs "github.com/colinmarc/hdfs/v2"
)

var _ FS = (*HDFS)(nil)

type HDFS struct {
	client *hdfs.Client
	addr   string
}

// NewHDFS 用 URI 字面 host:port 直连 (旧路径)。addr 用作 List() 返回 URI 的前缀。
func NewHDFS(addr, user string) (*HDFS, error) {
	if user == "" {
		user = os.Getenv("USER")
	}
	opts := hdfs.ClientOptions{Addresses: []string{addr}, User: user}
	return NewHDFSWithOptions(addr, opts)
}

// NewHDFSWithOptions 用调用方提供的 ClientOptions 建客户端 (典型来自 BuildClientOptions),
// uriHost 仅作 List() 返回 URI 的字面前缀, 与实际连接的 Addresses 解耦, 这样
// HA 逻辑名 (例如 "mycluster") 可以保留在用户原始 URI 里, 不破坏 eventlog.Locator
// 的 prefix matching。
func NewHDFSWithOptions(uriHost string, opts hdfs.ClientOptions) (*HDFS, error) {
	c, err := hdfs.NewClient(opts)
	if err != nil {
		return nil, err
	}
	return &HDFS{client: c, addr: uriHost}, nil
}

func (h *HDFS) Close() error { return h.client.Close() }

func uriHDFSPath(uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("HDFS: parse %q: %w", uri, err)
	}
	if u.Scheme != "hdfs" {
		return "", fmt.Errorf("HDFS: unsupported URI scheme %q", u.Scheme)
	}
	if u.Path == "" {
		return "/", nil
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
			out = append(out, "hdfs://"+h.addr+path.Join("/", p, e.Name()))
		}
	}
	return out, nil
}
