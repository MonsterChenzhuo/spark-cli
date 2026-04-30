//go:build hdfs_integration

package fs

import (
	"io"
	"os"
	"testing"
)

func TestHDFSReadIfEnvSet(t *testing.T) {
	addr := os.Getenv("SPARK_CLI_TEST_HDFS_ADDR")
	if addr == "" {
		t.Skip("set SPARK_CLI_TEST_HDFS_ADDR to run")
	}
	h, err := NewHDFS(addr, "")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	rc, err := h.Open("hdfs://" + addr + "/tmp/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	if len(body) == 0 {
		t.Fatal("empty")
	}
}
