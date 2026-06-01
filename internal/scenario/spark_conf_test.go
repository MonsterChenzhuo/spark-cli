package scenario

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestSparkConfColumnsMatchRowFields(t *testing.T) {
	row := SparkConfRow{}
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
	want := append([]string{}, SparkConfColumns()...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columns mismatch\n got=%v\nwant=%v", got, want)
	}
}

func TestSparkConfRanksImportantConfigsAndAddsTuningHints(t *testing.T) {
	app := model.NewApplication()
	app.SparkConf["spark.driver.memory"] = "4G"
	app.SparkConf["spark.sql.broadcastTimeout"] = "-1"
	app.SparkConf["spark.sql.autoBroadcastJoinThreshold"] = "10485760"
	app.SparkConf["spark.sql.adaptive.skewJoin.enabled"] = "true"
	app.SparkConf["spark.app.name"] = "daily-etl"

	rows, summary := SparkConf(app)
	if summary.Total != 5 {
		t.Fatalf("summary.Total=%d want 5", summary.Total)
	}
	if summary.Important < 4 {
		t.Fatalf("summary.Important=%d want at least 4", summary.Important)
	}
	if len(summary.ParameterHints) == 0 {
		t.Fatalf("expected parameter hints in summary")
	}

	byKey := map[string]SparkConfRow{}
	for _, r := range rows {
		byKey[r.Key] = r
	}
	driver := byKey["spark.driver.memory"]
	if driver.Category != "driver" || driver.Importance != "important" {
		t.Fatalf("driver row not classified as important driver config: %+v", driver)
	}
	if driver.TuningHint == "" {
		t.Fatalf("driver row missing tuning hint: %+v", driver)
	}
	timeout := byKey["spark.sql.broadcastTimeout"]
	if timeout.Category != "broadcast" || timeout.TuningHint == "" {
		t.Fatalf("broadcast timeout row missing broadcast hint: %+v", timeout)
	}
	if rows[0].Importance != "important" {
		t.Fatalf("first row should be important, got %+v", rows[0])
	}
}
