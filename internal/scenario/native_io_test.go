package scenario

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestNativeIOColumnsMatchRowFields(t *testing.T) {
	row := NativeIORow{}
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
	want := append([]string{}, NativeIOColumns()...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columns mismatch\n got=%v\nwant=%v", got, want)
	}
}

func TestNativeIOSummarizesAndRanksEvents(t *testing.T) {
	app := model.NewApplication()
	app.NativeIOEvents = []model.NativeIOEvent{
		{
			EventID:        "read-1",
			AIKind:         "paimon_native_io_reader",
			EventType:      "OPERATION_END",
			OperationID:    "op-read",
			OperationName:  "native-columnar-read",
			Phase:          "READ_BATCH",
			SQLExecutionID: 7,
			StageID:        3,
			TaskAttemptID:  99,
			ExecutorID:     "5",
			Host:           "worker-5",
			FilePath:       "obs://bucket/table/file.parquet",
			DurationMs:     500,
			Rows:           4096,
			Bytes:          16 * 1024 * 1024,
			Metrics:        map[string]float64{"read_batch_ms": 500, "rows": 4096},
		},
		{
			EventID:       "dv-1",
			AIKind:        "paimon_native_io_reader",
			EventType:     "OPERATION_END",
			OperationID:   "op-read",
			OperationName: "native-columnar-read",
			Phase:         "DV_FILTER",
			DurationMs:    100,
			Rows:          400,
		},
		{
			EventID:       "export-1",
			AIKind:        "paimon_native_io_export",
			EventType:     "OPERATION_END",
			OperationID:   "op-export",
			OperationName: "native-export-parquet",
			Phase:         "WRITE_PARQUET",
			DurationMs:    900,
			Bytes:         64 * 1024 * 1024,
			Metrics:       map[string]float64{"write_parquet_ms": 900},
		},
	}

	rows, summary := NativeIO(app, 2)
	if len(rows) != 2 {
		t.Fatalf("rows=%d want top 2", len(rows))
	}
	if rows[0].EventID != "export-1" || rows[1].EventID != "read-1" {
		t.Fatalf("rows not ranked by duration desc: %+v", rows)
	}
	if rows[1].ThroughputMBPerSec < 31.9 || rows[1].ThroughputMBPerSec > 32.1 {
		t.Fatalf("read throughput=%v want ~32 MiB/s", rows[1].ThroughputMBPerSec)
	}
	if rows[1].Metrics["read_batch_ms"] != 500 {
		t.Fatalf("metrics missing from row: %+v", rows[1].Metrics)
	}
	if summary.EventsTotal != 3 || summary.OperationsTotal != 2 {
		t.Fatalf("summary counts wrong: %+v", summary)
	}
	if summary.ReaderEvents != 2 || summary.ExportEvents != 1 {
		t.Fatalf("summary kind counts wrong: %+v", summary)
	}
	if summary.TotalDurationMs != 1500 || summary.TotalRows != 4496 || summary.TotalBytes != 80*1024*1024 {
		t.Fatalf("summary totals wrong: %+v", summary)
	}
	if len(summary.TopPhases) != 3 || summary.TopPhases[0].Phase != "WRITE_PARQUET" {
		t.Fatalf("phase breakdown wrong: %+v", summary.TopPhases)
	}
	if len(summary.TopOperations) != 2 || summary.TopOperations[0].OperationID != "op-export" {
		t.Fatalf("operation breakdown wrong: %+v", summary.TopOperations)
	}
}
