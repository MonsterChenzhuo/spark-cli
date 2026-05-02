package rules

import (
	"testing"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

func TestSkewRuleTriggers(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "x", 100, 0)
	for i := 0; i < 99; i++ {
		s.TaskDurations.Add(100)
	}
	s.TaskDurations.Add(20000)
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = s

	f := SkewRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical", f.Severity)
	}
	if f.Evidence == nil {
		t.Errorf("evidence missing")
	}
}

func TestSkewRuleDowngradesOnUniformInput(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "uniform-but-jittery", 100, 0)
	for i := 0; i < 95; i++ {
		s.TaskDurations.Add(100)
		s.TaskInputBytes.Add(1024 * 1024)
	}
	for i := 0; i < 5; i++ {
		s.TaskDurations.Add(1700) // p99/p50 = 17, below 20 cutoff
		s.TaskInputBytes.Add(1024 * 1024)
	}
	s.MaxInputBytes = 1024 * 1024 // input_skew_factor = 1.0
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = s

	f := SkewRule{}.Eval(app)
	if f.Severity != "warn" {
		t.Fatalf("severity=%s want warn (uniform input + moderate ratio should downgrade)", f.Severity)
	}
}

func TestSkewRuleDowngradesNegligibleWallShare(t *testing.T) {
	app := model.NewApplication()
	app.DurationMs = 1_000_000 // 1000s
	s := model.NewStage(1, 0, "tiny-but-skewed", 100, 0)
	s.SubmitMs = 0
	s.CompleteMs = 5_000 // wall_share = 0.5%,远低于 1% 阈值
	s.Status = "succeeded"
	for i := 0; i < 95; i++ {
		s.TaskDurations.Add(100)
		s.TaskInputBytes.Add(1024)
	}
	for i := 0; i < 5; i++ {
		s.TaskDurations.Add(1500) // p99/p50 = 15, 中等(<20)
		s.TaskInputBytes.Add(50 * 1024 * 1024)
	}
	s.MaxInputBytes = 50 * 1024 * 1024 // 让 f >= 10 触发 critical ladder
	app.Stages[model.StageKey{ID: 1}] = s

	f := SkewRule{}.Eval(app)
	if f.Severity != "warn" {
		t.Fatalf("severity=%s want warn (negligible wall_share should downgrade)", f.Severity)
	}
	if got, ok := f.Evidence["wall_share"].(float64); !ok || got > 0.01 {
		t.Errorf("evidence wall_share=%v want < 0.01 and present", f.Evidence["wall_share"])
	}
}

// 真实场景: P99/P50 ≈ 1.15(任务时长非常均匀,只是有一个极小任务把 min 拉到 0,
// 导致 input_skew_factor 看起来超大)。CLI v0 把这种情况报成 warn,但 wall_share
// 极低、任务时长本身就一致,完全不是倾斜。新增"任务时长紧致闸门"应当直接降到 ok,
// 让 diagnose 不要消耗用户注意力。
func TestSkewRuleDowngradesToOKOnTightP99P50(t *testing.T) {
	app := model.NewApplication()
	app.DurationMs = 1_000_000
	s := model.NewStage(1, 0, "uniform-time-but-huge-input-ratio", 200, 0)
	s.SubmitMs = 0
	s.CompleteMs = 5_000
	s.Status = "succeeded"
	for i := 0; i < 200; i++ {
		s.TaskDurations.Add(4_000) // 全部 4000ms,P99/P50 ≈ 1.0
		s.TaskInputBytes.Add(1024)
	}
	s.MaxInputBytes = 30_000_000 // input_skew_factor 极大
	app.Stages[model.StageKey{ID: 1}] = s

	f := SkewRule{}.Eval(app)
	if f.Severity != "ok" {
		t.Fatalf("severity=%s want ok (P99/P50 紧致时不算倾斜,即便 input_skew_factor 巨大)", f.Severity)
	}
}

func TestSkewRuleStaysCriticalOnExtremeRatio(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(1, 0, "extreme", 100, 0)
	for i := 0; i < 95; i++ {
		s.TaskDurations.Add(100)
		s.TaskInputBytes.Add(1024 * 1024)
	}
	for i := 0; i < 5; i++ {
		s.TaskDurations.Add(2500) // p99/p50 = 25, above 20 cutoff
		s.TaskInputBytes.Add(1024 * 1024)
	}
	s.MaxInputBytes = 1024 * 1024
	s.Status = "succeeded"
	app.Stages[model.StageKey{ID: 1}] = s

	f := SkewRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical (ratio>=20 should bypass uniform-input gate)", f.Severity)
	}
}

// 历史 bug:SkewRule 选 primary stage 用 skew_factor 倒序,导致 wall_share 92% 的
// stage 14 被 wall_share 26% 但 ratio 极端的 stage 7 盖过。primary 现在改用
// wall_share 倒序(平局回退 skew_factor),覆盖 agent 的实际 ROI 期望。
func TestSkewRulePicksMaxWallShareAsPrimary(t *testing.T) {
	app := model.NewApplication()
	app.DurationMs = 1_000_000

	// stage 1: wall_share 0.3,但 skew_factor 极高(应当被打到 similar_stages)
	low := model.NewStage(1, 0, "low-wall-extreme-ratio", 100, 0)
	low.SubmitMs = 0
	low.CompleteMs = 300_000 // wall_share = 0.3
	low.Status = "succeeded"
	for i := 0; i < 95; i++ {
		low.TaskDurations.Add(100)
		low.TaskInputBytes.Add(1024)
	}
	for i := 0; i < 5; i++ {
		low.TaskDurations.Add(3000) // p99/p50 = 30
		low.TaskInputBytes.Add(50 * 1024 * 1024)
	}
	low.MaxInputBytes = 50 * 1024 * 1024
	app.Stages[model.StageKey{ID: 1}] = low

	// stage 2: wall_share 0.7,skew_factor 中等(应当被选为 primary)
	high := model.NewStage(2, 0, "high-wall-mid-ratio", 100, 0)
	high.SubmitMs = 0
	high.CompleteMs = 700_000 // wall_share = 0.7
	high.Status = "succeeded"
	for i := 0; i < 95; i++ {
		high.TaskDurations.Add(1000)
		high.TaskInputBytes.Add(10 * 1024 * 1024)
	}
	for i := 0; i < 5; i++ {
		high.TaskDurations.Add(15_000) // p99/p50 = 15
		high.TaskInputBytes.Add(20 * 1024 * 1024)
	}
	high.MaxInputBytes = 20 * 1024 * 1024 // input_skew = 2
	app.Stages[model.StageKey{ID: 2}] = high

	f := SkewRule{}.Eval(app)
	if got, _ := f.Evidence["stage_id"].(int); got != 2 {
		t.Fatalf("primary stage_id=%v want 2 (max wall_share even with smaller skew_factor)", got)
	}
	if ws, _ := f.Evidence["wall_share"].(float64); ws < 0.69 || ws > 0.71 {
		t.Errorf("primary wall_share=%v want ~0.7", ws)
	}
	sims, ok := f.Evidence["similar_stages"].([]map[string]any)
	if !ok || len(sims) != 1 {
		t.Fatalf("similar_stages=%v want 1 entry covering stage 1", f.Evidence["similar_stages"])
	}
	if got, _ := sims[0]["stage_id"].(int); got != 1 {
		t.Errorf("similar_stages[0].stage_id=%v want 1", got)
	}
	if ws, _ := sims[0]["wall_share"].(float64); ws < 0.29 || ws > 0.31 {
		t.Errorf("similar_stages[0].wall_share=%v want ~0.3", ws)
	}
}

func TestSkewRuleSimilarStagesAbsentWhenSingleHit(t *testing.T) {
	app := model.NewApplication()
	app.DurationMs = 1_000_000
	s := model.NewStage(1, 0, "only-one", 100, 0)
	s.SubmitMs = 0
	s.CompleteMs = 500_000
	s.Status = "succeeded"
	for i := 0; i < 95; i++ {
		s.TaskDurations.Add(100)
		s.TaskInputBytes.Add(1024)
	}
	for i := 0; i < 5; i++ {
		s.TaskDurations.Add(3000)
		s.TaskInputBytes.Add(50 * 1024 * 1024)
	}
	s.MaxInputBytes = 50 * 1024 * 1024
	app.Stages[model.StageKey{ID: 1}] = s

	f := SkewRule{}.Eval(app)
	if _, ok := f.Evidence["similar_stages"]; ok {
		t.Errorf("similar_stages should be absent for single-stage skew, evidence=%+v", f.Evidence)
	}
}

func TestSkewRuleSkipsIdleStageCandidate(t *testing.T) {
	app := model.NewApplication()
	app.MaxConcurrentExecutors = 10

	// idle stage with high skew_factor: wall=120s, sum(task_run)=53.5s,
	// slots=10 → busy_ratio≈0.045 (below idle threshold 0.2).
	idle := model.NewStage(1, 0, "idle-skewed", 100, 0)
	idle.SubmitMs = 0
	idle.CompleteMs = 120_000
	idle.TotalRunMs = 53_500
	idle.Status = "succeeded"
	for i := 0; i < 95; i++ {
		idle.TaskDurations.Add(300)
		idle.TaskInputBytes.Add(1024 * 1024)
	}
	for i := 0; i < 5; i++ {
		idle.TaskDurations.Add(5_000) // p99/p50 ≈ 16.7 — would normally win
		idle.TaskInputBytes.Add(1024 * 1024)
	}
	idle.MaxInputBytes = 1024 * 1024
	app.Stages[model.StageKey{ID: 1}] = idle

	// busy stage with smaller skew but real input skew: wall=60s, run=540s,
	// slots=10 → busy_ratio=0.9, well above idle threshold.
	busy := model.NewStage(2, 0, "busy-skewed", 100, 0)
	busy.SubmitMs = 0
	busy.CompleteMs = 60_000
	busy.TotalRunMs = 540_000
	busy.Status = "succeeded"
	for i := 0; i < 95; i++ {
		busy.TaskDurations.Add(5_000)
		busy.TaskInputBytes.Add(10 * 1024 * 1024)
	}
	for i := 0; i < 5; i++ {
		busy.TaskDurations.Add(60_000) // p99/p50 = 12
		busy.TaskInputBytes.Add(120 * 1024 * 1024)
	}
	busy.MaxInputBytes = 120 * 1024 * 1024 // input_skew_factor = 12
	app.Stages[model.StageKey{ID: 2}] = busy

	f := SkewRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical, evidence=%+v", f.Severity, f.Evidence)
	}
	if got, _ := f.Evidence["stage_id"].(int); got != 2 {
		t.Errorf("reported stage_id=%v want 2 (idle stage 1 should be suppressed)", got)
	}
}

func TestFailedTasksRuleQuiet(t *testing.T) {
	app := model.NewApplication()
	app.TasksTotal = 10000
	app.TasksFailed = 1
	f := FailedTasksRule{}.Eval(app)
	if f.Severity != "ok" {
		t.Errorf("severity=%s want ok", f.Severity)
	}
}

func TestFailedTasksRuleEscalatesOnRepeatedNodeBlacklist(t *testing.T) {
	app := model.NewApplication()
	app.TasksTotal = 10000
	app.TasksFailed = 100 // 1% — would be "warn" alone
	app.Blacklists = []model.BlacklistEvent{
		{Time: 1, Kind: "node", Target: "host-bad", StageID: 5, Failures: 4},
		{Time: 2, Kind: "node", Target: "host-bad", StageID: 7, Failures: 3},
		{Time: 3, Kind: "executor", Target: "exec-9", StageID: 7, Failures: 2},
	}
	f := FailedTasksRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Errorf("severity=%s want critical (concentrated node failures)", f.Severity)
	}
	if hosts, ok := f.Evidence["blacklisted_hosts"].([]string); !ok || len(hosts) == 0 || hosts[0] != "host-bad" {
		t.Errorf("blacklisted_hosts evidence missing/wrong: %v", f.Evidence)
	}
	if f.Evidence["blacklist_node_events"] != 2 {
		t.Errorf("blacklist_node_events = %v, want 2", f.Evidence["blacklist_node_events"])
	}
	if f.Evidence["blacklist_executor_events"] != 1 {
		t.Errorf("blacklist_executor_events = %v, want 1", f.Evidence["blacklist_executor_events"])
	}
}

func TestSpillRuleEmbedsSparkConf(t *testing.T) {
	app := model.NewApplication()
	app.SparkConf["spark.sql.shuffle.partitions"] = "200"
	app.SparkConf["spark.executor.memory"] = "8g"
	s := model.NewStage(0, 0, "spilly", 200, 0)
	s.TotalSpillDisk = 12 * 1024 * 1024 * 1024      // 12 GB → critical
	s.TotalShuffleReadBytes = 200 * 5 * 1024 * 1024 // 200 partitions × 5 MiB
	app.Stages[model.StageKey{ID: 0}] = s
	f := SpillRule{}.Eval(app)
	if f.Severity != "critical" {
		t.Fatalf("severity=%s want critical", f.Severity)
	}
	if f.Evidence["spark_sql_shuffle_partitions"] != "200" {
		t.Errorf("evidence missing shuffle.partitions: %+v", f.Evidence)
	}
	if f.Evidence["spark_executor_memory"] != "8g" {
		t.Errorf("evidence missing executor.memory: %+v", f.Evidence)
	}
	if got := f.Evidence["partitions"]; got != 200 {
		t.Errorf("evidence partitions=%v want 200", got)
	}
	if got, _ := f.Evidence["est_partition_size_mb"].(float64); got < 4.99 || got > 5.01 {
		t.Errorf("evidence est_partition_size_mb=%v want ~5.0", got)
	}
}

func TestSpillRuleSkipsPartitionEvidenceWhenNumTasksZero(t *testing.T) {
	app := model.NewApplication()
	s := model.NewStage(0, 0, "no-tasks", 0, 0)
	s.TotalSpillDisk = 12 * 1024 * 1024 * 1024
	s.TotalShuffleReadBytes = 999 * 1024 * 1024
	app.Stages[model.StageKey{ID: 0}] = s
	f := SpillRule{}.Eval(app)
	if _, ok := f.Evidence["partitions"]; ok {
		t.Errorf("partitions should be omitted when num_tasks=0, evidence=%+v", f.Evidence)
	}
	if _, ok := f.Evidence["est_partition_size_mb"]; ok {
		t.Errorf("est_partition_size_mb should be omitted when num_tasks=0, evidence=%+v", f.Evidence)
	}
}
