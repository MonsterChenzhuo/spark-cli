package scenario

import (
	"sort"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/model"
)

type NativeIORow struct {
	EventID              string             `json:"event_id"`
	EventTime            int64              `json:"event_time"`
	EventType            string             `json:"event_type"`
	AIKind               string             `json:"ai_kind"`
	AISummary            string             `json:"ai_summary"`
	OperationID          string             `json:"operation_id"`
	OperationName        string             `json:"operation_name"`
	Phase                string             `json:"phase"`
	SQLExecutionID       int64              `json:"sql_execution_id"`
	StageID              int                `json:"stage_id"`
	StageAttemptID       int                `json:"stage_attempt_id"`
	TaskAttemptID        int64              `json:"task_attempt_id"`
	TaskIndex            int                `json:"task_index"`
	AttemptNumber        int                `json:"attempt_number"`
	ExecutorID           string             `json:"executor_id"`
	Host                 string             `json:"host"`
	FilePath             string             `json:"file_path"`
	OutputPath           string             `json:"output_path"`
	ObjectOperation      string             `json:"object_operation"`
	ObjectRequestID      string             `json:"object_request_id"`
	DurationMs           int64              `json:"duration_ms"`
	Rows                 int64              `json:"rows"`
	Bytes                int64              `json:"bytes"`
	ThroughputMBPerSec   float64            `json:"throughput_mb_per_s"`
	ThroughputRowsPerSec float64            `json:"throughput_rows_per_s"`
	QueueDepth           int                `json:"queue_depth"`
	RuntimeThreads       int                `json:"runtime_threads"`
	NativeMemoryMB       float64            `json:"native_memory_mb"`
	PeakBufferedMB       float64            `json:"peak_buffered_mb"`
	Metrics              map[string]float64 `json:"metrics"`
	ErrorClass           string             `json:"error_class"`
	ErrorMessage         string             `json:"error_message"`
	Verdict              string             `json:"verdict"`
}

func NativeIOColumns() []string {
	return []string{
		"event_id", "event_time", "event_type", "ai_kind", "ai_summary",
		"operation_id", "operation_name", "phase",
		"sql_execution_id", "stage_id", "stage_attempt_id", "task_attempt_id",
		"task_index", "attempt_number", "executor_id", "host",
		"file_path", "output_path", "object_operation", "object_request_id",
		"duration_ms", "rows", "bytes", "throughput_mb_per_s", "throughput_rows_per_s",
		"queue_depth", "runtime_threads", "native_memory_mb", "peak_buffered_mb",
		"metrics", "error_class", "error_message", "verdict",
	}
}

type NativeIOSummary struct {
	EventsTotal          int                        `json:"events_total"`
	OperationsTotal      int                        `json:"operations_total"`
	ReaderEvents         int                        `json:"reader_events"`
	ExportEvents         int                        `json:"export_events"`
	ErrorEvents          int                        `json:"error_events"`
	TotalDurationMs      int64                      `json:"total_duration_ms"`
	TotalRows            int64                      `json:"total_rows"`
	TotalBytes           int64                      `json:"total_bytes"`
	ThroughputMBPerSec   float64                    `json:"throughput_mb_per_s"`
	ThroughputRowsPerSec float64                    `json:"throughput_rows_per_s"`
	TopPhases            []NativeIOPhaseSummary     `json:"top_phases"`
	TopOperations        []NativeIOOperationSummary `json:"top_operations"`
	Warnings             []string                   `json:"warnings,omitempty"`
}

type NativeIOPhaseSummary struct {
	Phase                string  `json:"phase"`
	Events               int     `json:"events"`
	DurationMs           int64   `json:"duration_ms"`
	Rows                 int64   `json:"rows"`
	Bytes                int64   `json:"bytes"`
	ThroughputMBPerSec   float64 `json:"throughput_mb_per_s"`
	ThroughputRowsPerSec float64 `json:"throughput_rows_per_s"`
}

type NativeIOOperationSummary struct {
	OperationID          string  `json:"operation_id"`
	OperationName        string  `json:"operation_name"`
	AIKind               string  `json:"ai_kind"`
	Events               int     `json:"events"`
	DurationMs           int64   `json:"duration_ms"`
	Rows                 int64   `json:"rows"`
	Bytes                int64   `json:"bytes"`
	ThroughputMBPerSec   float64 `json:"throughput_mb_per_s"`
	ThroughputRowsPerSec float64 `json:"throughput_rows_per_s"`
	ErrorEvents          int     `json:"error_events"`
}

type nativeIOAggregate struct {
	operationID   string
	operationName string
	aiKind        string
	events        int
	durationMs    int64
	rows          int64
	bytes         int64
	errorEvents   int
}

func NativeIO(app *model.Application, top int) ([]NativeIORow, NativeIOSummary) {
	rows := make([]NativeIORow, 0, len(app.NativeIOEvents))
	summary := NativeIOSummary{EventsTotal: len(app.NativeIOEvents)}
	ops := make(map[string]*nativeIOAggregate)
	phases := make(map[string]*nativeIOAggregate)

	for _, event := range app.NativeIOEvents {
		rows = append(rows, nativeIORow(event))
		addNativeIOToSummary(&summary, event)
		addNativeIOAggregate(ops, nativeIOOperationKey(event), event)
		addNativeIOAggregate(phases, nativeIOPhaseKey(event), event)
	}

	summary.OperationsTotal = len(ops)
	summary.ThroughputMBPerSec = nativeIOThroughputMBPerSec(summary.TotalBytes, summary.TotalDurationMs)
	summary.ThroughputRowsPerSec = nativeIOThroughputRowsPerSec(summary.TotalRows, summary.TotalDurationMs)
	summary.TopPhases = nativeIOPhaseSummaries(phases)
	summary.TopOperations = nativeIOOperationSummaries(ops)
	if len(app.NativeIOEvents) == 0 {
		summary.Warnings = append(summary.Warnings, "no_native_io_events")
	}
	if summary.ErrorEvents > 0 {
		summary.Warnings = append(summary.Warnings, "native_io_errors_present")
	}

	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].DurationMs == rows[j].DurationMs {
			return rows[i].EventTime < rows[j].EventTime
		}
		return rows[i].DurationMs > rows[j].DurationMs
	})
	if top > 0 && len(rows) > top {
		rows = rows[:top]
	}
	return rows, summary
}

func nativeIORow(event model.NativeIOEvent) NativeIORow {
	metrics := event.Metrics
	if metrics == nil {
		metrics = map[string]float64{}
	}
	return NativeIORow{
		EventID:              event.EventID,
		EventTime:            event.EventTime,
		EventType:            event.EventType,
		AIKind:               event.AIKind,
		AISummary:            event.AISummary,
		OperationID:          event.OperationID,
		OperationName:        event.OperationName,
		Phase:                event.Phase,
		SQLExecutionID:       event.SQLExecutionID,
		StageID:              event.StageID,
		StageAttemptID:       event.StageAttemptID,
		TaskAttemptID:        event.TaskAttemptID,
		TaskIndex:            event.TaskIndex,
		AttemptNumber:        event.AttemptNumber,
		ExecutorID:           event.ExecutorID,
		Host:                 event.Host,
		FilePath:             event.FilePath,
		OutputPath:           event.OutputPath,
		ObjectOperation:      event.ObjectOperation,
		ObjectRequestID:      event.ObjectRequestID,
		DurationMs:           event.DurationMs,
		Rows:                 event.Rows,
		Bytes:                event.Bytes,
		ThroughputMBPerSec:   nativeIOThroughputMBPerSec(event.Bytes, event.DurationMs),
		ThroughputRowsPerSec: nativeIOThroughputRowsPerSec(event.Rows, event.DurationMs),
		QueueDepth:           event.QueueDepth,
		RuntimeThreads:       event.RuntimeThreads,
		NativeMemoryMB:       nativeIOBytesToMB(event.NativeMemoryBytes),
		PeakBufferedMB:       nativeIOBytesToMB(event.PeakBufferedBytes),
		Metrics:              metrics,
		ErrorClass:           event.ErrorClass,
		ErrorMessage:         event.ErrorMessage,
		Verdict:              nativeIOVerdict(event),
	}
}

func addNativeIOToSummary(summary *NativeIOSummary, event model.NativeIOEvent) {
	summary.TotalDurationMs += event.DurationMs
	summary.TotalRows += event.Rows
	summary.TotalBytes += event.Bytes
	if strings.Contains(event.AIKind, "reader") {
		summary.ReaderEvents++
	}
	if strings.Contains(event.AIKind, "export") {
		summary.ExportEvents++
	}
	if nativeIOHasError(event) {
		summary.ErrorEvents++
	}
}

func addNativeIOAggregate(m map[string]*nativeIOAggregate, key string, event model.NativeIOEvent) {
	agg, ok := m[key]
	if !ok {
		agg = &nativeIOAggregate{
			operationID:   event.OperationID,
			operationName: event.OperationName,
			aiKind:        event.AIKind,
		}
		m[key] = agg
	}
	agg.events++
	agg.durationMs += event.DurationMs
	agg.rows += event.Rows
	agg.bytes += event.Bytes
	if nativeIOHasError(event) {
		agg.errorEvents++
	}
}

func nativeIOPhaseSummaries(phases map[string]*nativeIOAggregate) []NativeIOPhaseSummary {
	out := make([]NativeIOPhaseSummary, 0, len(phases))
	for phase, agg := range phases {
		out = append(out, NativeIOPhaseSummary{
			Phase:                phase,
			Events:               agg.events,
			DurationMs:           agg.durationMs,
			Rows:                 agg.rows,
			Bytes:                agg.bytes,
			ThroughputMBPerSec:   nativeIOThroughputMBPerSec(agg.bytes, agg.durationMs),
			ThroughputRowsPerSec: nativeIOThroughputRowsPerSec(agg.rows, agg.durationMs),
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].DurationMs == out[j].DurationMs {
			return out[i].Phase < out[j].Phase
		}
		return out[i].DurationMs > out[j].DurationMs
	})
	return out
}

func nativeIOOperationSummaries(ops map[string]*nativeIOAggregate) []NativeIOOperationSummary {
	out := make([]NativeIOOperationSummary, 0, len(ops))
	for _, agg := range ops {
		out = append(out, NativeIOOperationSummary{
			OperationID:          agg.operationID,
			OperationName:        agg.operationName,
			AIKind:               agg.aiKind,
			Events:               agg.events,
			DurationMs:           agg.durationMs,
			Rows:                 agg.rows,
			Bytes:                agg.bytes,
			ThroughputMBPerSec:   nativeIOThroughputMBPerSec(agg.bytes, agg.durationMs),
			ThroughputRowsPerSec: nativeIOThroughputRowsPerSec(agg.rows, agg.durationMs),
			ErrorEvents:          agg.errorEvents,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].DurationMs == out[j].DurationMs {
			return out[i].OperationID < out[j].OperationID
		}
		return out[i].DurationMs > out[j].DurationMs
	})
	return out
}

func nativeIOOperationKey(event model.NativeIOEvent) string {
	if event.OperationID != "" {
		return event.OperationID
	}
	if event.EventID != "" {
		return event.EventID
	}
	return event.OperationName
}

func nativeIOPhaseKey(event model.NativeIOEvent) string {
	if event.Phase != "" {
		return event.Phase
	}
	return "UNKNOWN"
}

func nativeIOHasError(event model.NativeIOEvent) bool {
	return event.ErrorClass != "" || event.ErrorMessage != "" ||
		strings.Contains(strings.ToLower(event.EventType), "error")
}

func nativeIOVerdict(event model.NativeIOEvent) string {
	if nativeIOHasError(event) {
		return "error"
	}
	text := strings.ToLower(event.EventType + " " + event.OperationName + " " + event.AISummary)
	if strings.Contains(text, "fallback") {
		return "fallback"
	}
	if event.Bytes >= 64*1024*1024 &&
		nativeIOThroughputMBPerSec(event.Bytes, event.DurationMs) > 0 &&
		nativeIOThroughputMBPerSec(event.Bytes, event.DurationMs) < 50 {
		return "low_throughput"
	}
	if event.DurationMs >= 5000 {
		return "slow"
	}
	return "ok"
}

func nativeIOThroughputMBPerSec(bytes, durationMs int64) float64 {
	if bytes <= 0 || durationMs <= 0 {
		return 0
	}
	return round3(float64(bytes) / float64(mb) / (float64(durationMs) / 1000.0))
}

func nativeIOThroughputRowsPerSec(rows, durationMs int64) float64 {
	if rows <= 0 || durationMs <= 0 {
		return 0
	}
	return round3(float64(rows) / (float64(durationMs) / 1000.0))
}

func nativeIOBytesToMB(bytes int64) float64 {
	if bytes <= 0 {
		return 0
	}
	return round3(float64(bytes) / float64(mb))
}
