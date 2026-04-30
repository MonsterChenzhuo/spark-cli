package configcmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/config"
)

func TestRenderShowsSources(t *testing.T) {
	cfg := &config.Config{
		LogDirs: []string{"file:///tmp/spark-events"},
		HDFS:    config.HDFSConfig{User: "alice"},
	}
	cfg.Timeout = 30_000_000_000 // 30s in ns
	var buf bytes.Buffer
	render(&buf, cfg, sources{
		LogDirs:  "file",
		HDFSUser: "default",
		Timeout:  "default",
	})
	out := buf.String()
	if !strings.Contains(out, "log_dirs") || !strings.Contains(out, "file:///tmp/spark-events") {
		t.Errorf("missing log_dirs in %q", out)
	}
	if !strings.Contains(out, "(file)") {
		t.Errorf("missing source label in %q", out)
	}
}
