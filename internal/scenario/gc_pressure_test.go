package scenario

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

// 列契约反射守门 — 让 GCPressureColumns() 与 GCExecRow 的 JSON tag 不会脱节。
// AppSummary / SlowStages / DataSkew 各有同款测试,本来 GCExecRow 没加,
// CLAUDE.md 已注明"改它的字段时手工核对" —— 不再依赖手工,统一加上反射守门。
func TestGCPressureColumnsMatchRowFields(t *testing.T) {
	row := GCExecRow{}
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := make([]string, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	sort.Strings(got)
	want := append([]string{}, GCPressureColumns()...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columns mismatch\n got=%v\nwant=%v", got, want)
	}
}

func TestGCPressureRanksAndClassifies(t *testing.T) {
	app := model.NewApplication()
	app.Executors["hi"] = &model.Executor{ID: "hi", Host: "h1", TotalRunMs: 10000, TotalGCMs: 4000, TaskCount: 100}
	app.Executors["mid"] = &model.Executor{ID: "mid", Host: "h2", TotalRunMs: 10000, TotalGCMs: 1500, TaskCount: 80}
	app.Executors["lo"] = &model.Executor{ID: "lo", Host: "h3", TotalRunMs: 10000, TotalGCMs: 200, TaskCount: 50}
	app.Executors["zero"] = &model.Executor{ID: "zero", Host: "h4", TotalRunMs: 0, TotalGCMs: 0, TaskCount: 0}

	rows := GCPressure(app, 10)
	if len(rows) != 3 {
		t.Fatalf("want 3 rows (zero-run filtered), got %d: %+v", len(rows), rows)
	}
	if rows[0].ExecutorID != "hi" || rows[0].Verdict != "severe" {
		t.Errorf("expected hi/severe first, got %+v", rows[0])
	}
	if rows[1].ExecutorID != "mid" || rows[1].Verdict != "warn" {
		t.Errorf("expected mid/warn second, got %+v", rows[1])
	}
	if rows[2].Verdict != "ok" {
		t.Errorf("expected ok last, got %+v", rows[2])
	}
}
