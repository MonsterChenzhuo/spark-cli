package fs

import (
	"strings"
	"testing"
)

func TestURIHDFSPath(t *testing.T) {
	cases := []struct {
		uri      string
		wantPath string
		wantErr  string
	}{
		{"hdfs://nn:8020/a/b", "/a/b", ""},
		{"hdfs://nn:8020", "/", ""},
		{"hdfs://nn:8020/", "/", ""},
		{"file:///x", "", "unsupported URI scheme"},
		{"s3://b/k", "", "unsupported URI scheme"},
	}
	for _, c := range cases {
		got, err := uriHDFSPath(c.uri)
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("%s: want err containing %q, got %v", c.uri, c.wantErr, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected err: %v", c.uri, err)
			continue
		}
		if got != c.wantPath {
			t.Errorf("%s: got %q want %q", c.uri, got, c.wantPath)
		}
	}
}
