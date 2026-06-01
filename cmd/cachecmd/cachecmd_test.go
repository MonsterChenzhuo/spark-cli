package cachecmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
)

func writeTmp(t *testing.T, dir, rel string, size int) string {
	t.Helper()
	full := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, bytes.Repeat([]byte("x"), size), 0o644); err != nil {
		t.Fatal(err)
	}
	return full
}

// resolveCacheDir 优先级:--cache-dir flag > SPARK_CLI_CACHE_DIR env > DefaultDir。
// round-11 修 — list/clear 之前只读 env,忽略 root persistent flag。
func TestResolveCacheDirRespectsRootFlag(t *testing.T) {
	t.Setenv("SPARK_CLI_CACHE_DIR", "/from/env")

	root := newRootStub()
	root.PersistentFlags().String("cache-dir", "", "")
	if err := root.PersistentFlags().Set("cache-dir", "/from/flag"); err != nil {
		t.Fatal(err)
	}
	child := &cobra.Command{Use: "list"}
	root.AddCommand(child)
	got := resolveCacheDir(child)
	if got != "/from/flag" {
		t.Errorf("resolveCacheDir=%q want /from/flag (flag should win over env)", got)
	}

	// flag 没设时退回 env
	root2 := newRootStub()
	root2.PersistentFlags().String("cache-dir", "", "")
	child2 := &cobra.Command{Use: "list"}
	root2.AddCommand(child2)
	got2 := resolveCacheDir(child2)
	if got2 != "/from/env" {
		t.Errorf("resolveCacheDir=%q want /from/env (env fallback)", got2)
	}
}

func newRootStub() *cobra.Command {
	return &cobra.Command{Use: "spark-cli"}
}

// scanCache 应当列出所有 application gob.zst 与 shs/<host>/<id>_<v>.zip,
// 按 size_mib 降序排序,合计 total_mib。
func TestScanCacheClassifiesAndSorts(t *testing.T) {
	dir := t.TempDir()
	writeTmp(t, dir, "application_1.gob.zst", 100)
	writeTmp(t, dir, "shs/host_1234/application_1_999.zip", 5000)
	writeTmp(t, dir, "application_2.gob.zst", 50)

	out := scanCache(dir)
	if out.Total != 3 {
		t.Errorf("total=%d want 3", out.Total)
	}
	// 第一名应当是最大的(shs zip)
	if out.Entries[0].Kind != "shs_zip" {
		t.Errorf("first entry kind=%s want shs_zip", out.Entries[0].Kind)
	}
	if !strings.HasSuffix(out.Entries[0].Name, ".zip") {
		t.Errorf("first entry name=%s want zip", out.Entries[0].Name)
	}
	// 后两个都是 application 类型
	if out.Entries[1].Kind != "application" || out.Entries[2].Kind != "application" {
		t.Errorf("entries kind:\n%+v", out.Entries)
	}
}

func TestCacheListDefaultsToJSON(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CACHE_DIR", dir)
	writeTmp(t, dir, "application_1.gob.zst", 100)

	cmd := New()
	cmd.SetArgs([]string{"list"})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stdout)
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("cache list: %v\n%s", err, stdout.String())
	}
	var got listOutput
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("default output should be json: %v\n%s", err, stdout.String())
	}
	if got.Dir != dir || got.Total != 1 {
		t.Fatalf("unexpected output: %+v", got)
	}
}

func TestCacheListRejectsTextFormat(t *testing.T) {
	cmd := New()
	cmd.SetArgs([]string{"list", "--format", "text"})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stdout)
	err := cmd.ExecuteContext(context.Background())
	if err == nil {
		t.Fatalf("cache list --format text should fail, stdout=%s", stdout.String())
	}
	if !strings.Contains(err.Error(), `unknown --format "text"`) {
		t.Fatalf("unexpected error: %v", err)
	}
	var ce *cerrors.Error
	if !errors.As(err, &ce) || ce.Code != cerrors.CodeFlagInvalid {
		t.Fatalf("error should be FLAG_INVALID, got %#v", err)
	}
}

// clearCache --app 应当只删名字含 appId substring 的文件,其他保留。
func TestClearCacheAppFilter(t *testing.T) {
	dir := t.TempDir()
	keep := writeTmp(t, dir, "application_X.gob.zst", 100)
	rm := writeTmp(t, dir, "application_Y.gob.zst", 200)
	rmShs := writeTmp(t, dir, "shs/host/application_Y_999.zip", 300)

	out, err := clearCache(dir, "Y", false)
	if err != nil {
		t.Fatalf("clearCache: %v", err)
	}
	if out.Removed != 2 {
		t.Errorf("removed=%d want 2 (both Y entries)", out.Removed)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Errorf("keep entry %s removed: %v", keep, err)
	}
	if _, err := os.Stat(rm); !os.IsNotExist(err) {
		t.Errorf("rm entry %s should be deleted, err=%v", rm, err)
	}
	if _, err := os.Stat(rmShs); !os.IsNotExist(err) {
		t.Errorf("rm shs %s should be deleted, err=%v", rmShs, err)
	}
}

// clearCache 不带 --app 时删全部 + 清空目录,但 --dry-run 时只打印。
func TestClearCacheDryRun(t *testing.T) {
	dir := t.TempDir()
	writeTmp(t, dir, "application_X.gob.zst", 100)

	out, err := clearCache(dir, "", true)
	if err != nil {
		t.Fatalf("clearCache: %v", err)
	}
	if out.Removed != 1 {
		t.Errorf("would-remove count=%d want 1", out.Removed)
	}
	// dry-run 文件不应被删除
	if _, err := os.Stat(filepath.Join(dir, "application_X.gob.zst")); err != nil {
		t.Errorf("dry-run should not delete: %v", err)
	}
}

func TestCacheClearCommandEmitsJSON(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_CLI_CACHE_DIR", dir)
	writeTmp(t, dir, "application_Y.gob.zst", 200)

	cmd := New()
	cmd.SetArgs([]string{"clear", "--app", "Y"})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stdout)
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("cache clear: %v\n%s", err, stdout.String())
	}
	var got clearOutput
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout should be json: %v\n%s", err, stdout.String())
	}
	if got.Command != "cache clear" || got.CacheDir != dir || got.AppID != "Y" || got.Removed != 1 {
		t.Fatalf("unexpected output: %+v", got)
	}
	if len(got.Entries) != 1 || !got.Entries[0].Removed {
		t.Fatalf("unexpected entries: %+v", got.Entries)
	}
}
