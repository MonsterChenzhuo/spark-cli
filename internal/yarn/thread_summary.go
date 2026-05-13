package yarn

import (
	"fmt"
	"sort"
	"strings"
)

const (
	maxInterestingThreads = 8
	maxSummaryFrames      = 40
)

func summarizeThreadDump(executorID string, threads []ThreadInfo) (*ThreadDumpDiagnosis, *ThreadStackSummary, []ThreadStackSummary) {
	var mainThread *ThreadStackSummary
	interesting := make([]ThreadStackSummary, 0, maxInterestingThreads)
	seen := map[string]struct{}{}

	for _, th := range threads {
		frames := stackTraceLines(th.StackTrace)
		tags := classifyThread(th.ThreadName, frames)
		summary := buildThreadSummary(th, frames, tags)
		if th.ThreadName == "main" {
			copy := summary
			mainThread = &copy
		}
		if !isInterestingThread(th.ThreadName, tags) {
			continue
		}
		key := fmt.Sprintf("%d/%s", th.ThreadID, th.ThreadName)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		interesting = append(interesting, summary)
		if len(interesting) >= maxInterestingThreads {
			break
		}
	}

	diagnosis := diagnoseThreads(executorID, mainThread, interesting)
	return diagnosis, mainThread, interesting
}

func buildThreadSummary(th ThreadInfo, frames, tags []string) ThreadStackSummary {
	if len(frames) > maxSummaryFrames {
		frames = frames[:maxSummaryFrames]
	}
	top := ""
	if len(frames) > 0 {
		top = frames[0]
	}
	return ThreadStackSummary{
		ThreadID:    th.ThreadID,
		ThreadName:  th.ThreadName,
		ThreadState: th.ThreadState,
		TopFrame:    top,
		Tags:        tags,
		StackTrace:  frames,
	}
}

func diagnoseThreads(executorID string, mainThread *ThreadStackSummary, interesting []ThreadStackSummary) *ThreadDumpDiagnosis {
	if mainThread != nil {
		switch {
		case hasTag(*mainThread, "driver_waiting_for_spark_job"):
			return &ThreadDumpDiagnosis{
				Category: "driver_waiting_for_spark_job",
				Evidence: []string{
					"main thread is parked in DAGScheduler/SparkContext.runJob",
					"stack contains Dataset/RDD collect under PaimonSparkWriter when present",
				},
				Suggestion: "Job has already been submitted; inspect app-summary/slow-stages, then sample active executor threads if stages are still running.",
			}
		case hasAnyTag(*mainThread, "paimon_schema_validation", "paimon_schema"):
			return &ThreadDumpDiagnosis{
				Category: "paimon_schema_validation",
				Evidence: []string{
					"main thread is inside Paimon schema validation or RowType construction",
				},
				Suggestion: "This is a driver-side planning hotspot; check for very wide schemas, duplicateFields/logicalRowType validation, or stale Paimon jars.",
			}
		case hasTag(*mainThread, "spark_collapse_project"):
			return &ThreadDumpDiagnosis{
				Category: "spark_collapse_project",
				Evidence: []string{
					"main thread is inside Spark optimizer CollapseProject.canCollapseExpressions",
					"wide or deeply nested Project chains can make AttributeSet/expression checks expensive before any job is submitted",
				},
				Suggestion: "For very wide SQL, reduce chained SELECT/Project layers or test excluding org.apache.spark.sql.catalyst.optimizer.CollapseProject.",
			}
		case hasAnyTag(*mainThread, "spark_v2_pushdown", "spark_constraint_inference", "spark_sql_planning"):
			return &ThreadDumpDiagnosis{
				Category: "spark_sql_planning",
				Evidence: []string{
					"main thread is inside Spark SQL planning or optimizer rules",
				},
				Suggestion: "This is pre-job driver planning; inspect optimizer rules and wide-schema planning cost before looking at task metrics.",
			}
		}
	}

	for _, th := range interesting {
		switch {
		case hasAnyTag(th, "spark_codegen", "spark_projection") && hasTag(th, "shuffle_write"):
			return &ThreadDumpDiagnosis{
				Category: "executor_projection_codegen",
				Evidence: []string{
					"executor task thread is in Spark projection codegen/execution before or during shuffle write",
				},
				Suggestion: "For very wide projections, compare codegen on/off and reduce shuffle partitions if many tiny tasks dominate stage wall time.",
			}
		case hasAnyTag(th, "paimon_write", "parquet_io"):
			return &ThreadDumpDiagnosis{
				Category: "executor_paimon_or_parquet_io",
				Evidence: []string{
					"executor task thread is inside Paimon or Parquet read/write code",
				},
				Suggestion: "Inspect slow-stages for input/shuffle/output size per task and fetch executor logs if tasks fail or hang.",
			}
		}
	}

	if mainThread != nil && executorID == "driver" {
		return &ThreadDumpDiagnosis{
			Category:   "driver_thread_dump_collected",
			Evidence:   []string{"main thread was summarized but no known hotspot pattern matched"},
			Suggestion: "Read main_thread and interesting_threads; if main is waiting, pair this with app-summary and slow-stages.",
		}
	}
	return &ThreadDumpDiagnosis{
		Category:   "thread_dump_collected",
		Evidence:   []string{"thread dump was fetched and summarized"},
		Suggestion: "Read interesting_threads first; raw threads are preserved for deeper inspection.",
	}
}

func classifyThread(name string, frames []string) []string {
	joined := strings.Join(frames, "\n")
	tags := make([]string, 0, 6)
	add := func(tag string) {
		for _, existing := range tags {
			if existing == tag {
				return
			}
		}
		tags = append(tags, tag)
	}

	if name == "main" {
		add("main")
	}
	if strings.Contains(name, "Executor task launch") {
		add("executor_task")
	}
	if containsAny(joined, "DAGScheduler.runJob", "SparkContext.runJob") {
		add("driver_waiting_for_spark_job")
	}
	if containsAny(joined, "PaimonSparkWriter.write", "WriteIntoPaimonTable.run") {
		add("paimon_write")
	}
	if containsAny(joined, "SchemaValidation", "Schema.duplicateFields", "TableSchema.logicalRowType", "RowType.validateFields") {
		add("paimon_schema_validation")
	}
	if containsAny(joined, "logicalRowType", "duplicateFields") {
		add("paimon_schema")
	}
	if containsAny(joined, "InferFiltersFromConstraints") {
		add("spark_constraint_inference")
	}
	if containsAny(joined, "CollapseProject") {
		add("spark_collapse_project")
	}
	if containsAny(joined, "V2ScanRelationPushDown") {
		add("spark_v2_pushdown")
	}
	if containsAny(joined, "QueryExecution.optimizedPlan", "QueryExecution.executedPlan", "QueryExecution.eagerlyExecuteCommands") {
		add("spark_sql_planning")
	}
	if containsAny(joined, "GenerateUnsafeProjection", "SpecificUnsafeProjection", "CodeGenerator", "WholeStageCodegen") {
		add("spark_codegen")
	}
	if containsAny(joined, "ProjectExec") {
		add("spark_projection")
	}
	if containsAny(joined, "ShuffleWriteProcessor", "UnsafeShuffleWriter", "SortShuffleWriter") {
		add("shuffle_write")
	}
	if containsAny(joined, "ParquetFileReader", "ParquetFileWriter", "ParquetReaderFactory") {
		add("parquet_io")
	}
	sort.Strings(tags)
	return tags
}

func isInterestingThread(name string, tags []string) bool {
	if name == "main" || strings.Contains(name, "Executor task launch") {
		return true
	}
	return len(tags) > 0
}

func hasTag(th ThreadStackSummary, tag string) bool {
	for _, got := range th.Tags {
		if got == tag {
			return true
		}
	}
	return false
}

func hasAnyTag(th ThreadStackSummary, tags ...string) bool {
	for _, tag := range tags {
		if hasTag(th, tag) {
			return true
		}
	}
	return false
}

func containsAny(s string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}

func stackTraceLines(v any) []string {
	switch st := v.(type) {
	case nil:
		return nil
	case string:
		return splitStackString(st)
	case []any:
		return stackElemsToLines(st)
	case map[string]any:
		for _, key := range []string{"elems", "frames", "stackTrace"} {
			if elems, ok := st[key].([]any); ok {
				return stackElemsToLines(elems)
			}
			if elem, ok := st[key].(string); ok {
				return splitStackString(elem)
			}
		}
	}
	return nil
}

func stackElemsToLines(elems []any) []string {
	lines := make([]string, 0, len(elems))
	for _, elem := range elems {
		switch v := elem.(type) {
		case string:
			if trimmed := strings.TrimSpace(v); trimmed != "" {
				lines = append(lines, trimmed)
			}
		case map[string]any:
			if line := stackFrameMapToLine(v); line != "" {
				lines = append(lines, line)
			}
		}
	}
	return lines
}

func splitStackString(s string) []string {
	raw := strings.Split(s, "\n")
	lines := make([]string, 0, len(raw))
	for _, line := range raw {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	return lines
}

func stackFrameMapToLine(frame map[string]any) string {
	className, _ := frame["className"].(string)
	methodName, _ := frame["methodName"].(string)
	fileName, _ := frame["fileName"].(string)
	location := fileName
	if n, ok := frame["lineNumber"].(float64); ok && n >= 0 {
		location = fmt.Sprintf("%s:%d", fileName, int(n))
	}
	if className == "" && methodName == "" {
		return ""
	}
	call := className
	if methodName != "" {
		if call != "" {
			call += "."
		}
		call += methodName
	}
	if location != "" {
		return fmt.Sprintf("%s(%s)", call, location)
	}
	return call
}
