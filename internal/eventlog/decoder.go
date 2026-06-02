package eventlog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/opay-bigdata/spark-cli/internal/model"

	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
)

const (
	maxScanLine = 16 << 20 // 16MB per JSONL line

	sqlExecutionStartEvent        = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
	sqlAdaptiveExecutionUpdateEvt = "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate"
)

type DecodeOptions struct {
	AllowTruncatedTail bool
}

func Decode(r io.Reader, agg *model.Aggregator) (int, error) {
	return DecodeWithOptions(r, agg, DecodeOptions{})
}

func DecodeWithOptions(r io.Reader, agg *model.Aggregator, opts DecodeOptions) (int, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	parsed := 0
	for {
		line, readErr := br.ReadBytes('\n')
		if len(line) > maxScanLine {
			if readErr != nil && readErr != io.EOF {
				return parsed, cerrors.New(cerrors.CodeLogUnreadable, readErr.Error(), "")
			}
			if isOversizedSkippableSQLEvent(line) {
				if readErr == io.EOF {
					break
				}
				continue
			}
			return parsed, cerrors.New(cerrors.CodeLogParseFailed,
				"event JSONL line exceeds 16MB", "file may be corrupted; if this is a very large Spark SQL description, rerun with SQL description/detail disabled or upgrade spark-cli")
		}
		if len(line) == 0 && readErr == io.EOF {
			break
		}
		if readErr != nil && readErr != io.EOF {
			return parsed, cerrors.New(cerrors.CodeLogUnreadable, readErr.Error(), "")
		}
		atEOF := readErr == io.EOF
		line = bytes.TrimRight(line, "\r\n")
		if len(line) == 0 {
			if atEOF {
				break
			}
			continue
		}
		var base evtBase
		if err := json.Unmarshal(line, &base); err != nil {
			if opts.AllowTruncatedTail && atEOF && isUnexpectedJSONTail(err) {
				return parsed, nil
			}
			return parsed, cerrors.New(cerrors.CodeLogParseFailed, err.Error(), "line "+itoa(parsed+1))
		}
		if err := dispatch(base.Event, line, agg); err != nil {
			return parsed, err
		}
		parsed++
		if atEOF {
			break
		}
	}
	return parsed, nil
}

func isOversizedSkippableSQLEvent(line []byte) bool {
	return hasEvent(line, sqlExecutionStartEvent) || hasEvent(line, sqlAdaptiveExecutionUpdateEvt)
}

func hasEvent(line []byte, event string) bool {
	return bytes.Contains(line, []byte(`"Event":"`+event+`"`)) ||
		bytes.Contains(line, []byte(`"Event": "`+event+`"`))
}

func isUnexpectedJSONTail(err error) bool {
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) && strings.Contains(err.Error(), "unexpected end of JSON input") {
		return true
	}
	return strings.Contains(err.Error(), "unexpected end of JSON input") ||
		strings.Contains(err.Error(), "unexpected EOF")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func dispatch(event string, raw []byte, agg *model.Aggregator) error {
	if handled, err := dispatchAppLifecycle(event, raw, agg); handled || err != nil {
		return err
	}
	if handled, err := dispatchSQLAndBlacklist(event, raw, agg); handled || err != nil {
		return err
	}
	if handled, err := dispatchStageAndTask(event, raw, agg); handled || err != nil {
		return err
	}
	if handled, err := dispatchNativeIO(event, raw, agg); handled || err != nil {
		return err
	}
	// Unknown events skipped silently.
	return nil
}

func dispatchAppLifecycle(event string, raw []byte, agg *model.Aggregator) (bool, error) {
	switch event {
	case "SparkListenerApplicationStart":
		var e evtAppStart
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		agg.OnAppStart(e.AppID, e.AppName, e.User, e.Timestamp)
	case "SparkListenerApplicationEnd":
		var e evtAppEnd
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		agg.OnAppEnd(e.Timestamp)
	case "SparkListenerExecutorAdded":
		var e evtExecutorAdded
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		agg.OnExecutorAdded(e.ExecutorID, e.ExecutorInfo.Host, e.ExecutorInfo.TotalCores, e.Timestamp)
	case "SparkListenerExecutorRemoved":
		var e evtExecutorRemoved
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		agg.OnExecutorRemoved(e.ExecutorID, e.Timestamp, e.RemovedReason)
	case "SparkListenerJobStart":
		var e evtJobStart
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		agg.OnJobStart(e.JobID, e.StageIDs, e.SubmitMs, e.Properties)
	case "SparkListenerJobEnd":
		var e evtJobEnd
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		agg.OnJobEnd(e.JobID, e.EndMs, e.JobResult.Result)
	default:
		return false, nil
	}
	return true, nil
}

func dispatchSQLAndBlacklist(event string, raw []byte, agg *model.Aggregator) (bool, error) {
	switch event {
	case "SparkListenerEnvironmentUpdate":
		var e evtEnvironmentUpdate
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		agg.OnEnvironmentUpdate(e.SparkProperties)
	case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
		var e evtSQLExecutionStart
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		agg.OnSQLExecutionStart(e.ExecutionID, e.Description, e.Details, e.Time)
	case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
		var e evtSQLExecutionEnd
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		agg.OnSQLExecutionEnd(e.ExecutionID, e.Time)
	case "org.apache.spark.scheduler.SparkListenerNodeBlacklistedForStage",
		"org.apache.spark.scheduler.SparkListenerNodeExcludedForStage":
		var e evtNodeBlacklisted
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		agg.OnNodeBlacklisted(e.Time, e.HostID, e.StageID, e.ExecutorFailures)
	case "org.apache.spark.scheduler.SparkListenerExecutorBlacklistedForStage",
		"org.apache.spark.scheduler.SparkListenerExecutorExcludedForStage":
		var e evtExecutorBlacklisted
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		agg.OnExecutorBlacklisted(e.Time, e.ExecutorID, e.StageID, e.TaskFailures)
	default:
		return false, nil
	}
	return true, nil
}

func dispatchStageAndTask(event string, raw []byte, agg *model.Aggregator) (bool, error) {
	switch event {
	case "SparkListenerStageSubmitted":
		var e evtStageSubmitted
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		s := e.StageInfo
		agg.OnStageSubmitted(s.StageID, s.StageAttemptID, s.StageName, s.NumberOfTasks, s.SubmitMs)
	case "SparkListenerStageCompleted":
		var e evtStageCompleted
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		s := e.StageInfo
		status := "succeeded"
		if s.FailureReason != "" {
			status = "failed"
		}
		agg.OnStageCompleted(s.StageID, s.StageAttemptID, s.CompleteMs, status)
	case "SparkListenerTaskEnd":
		var e evtTaskEnd
		if err := json.Unmarshal(raw, &e); err != nil {
			return true, parseErr(err)
		}
		agg.OnTaskEnd(taskEndFromEvent(&e))
	default:
		return false, nil
	}
	return true, nil
}

func dispatchNativeIO(event string, raw []byte, agg *model.Aggregator) (bool, error) {
	if event != "org.apache.spark.scheduler.SparkListenerNativeIOEvent" {
		return false, nil
	}
	var e evtNativeIO
	if err := json.Unmarshal(raw, &e); err != nil {
		return true, parseErr(err)
	}
	nativeEvent := nativeIOFromEvent(&e)
	if nativeEvent.EventID == "" && nativeEvent.OperationID == "" && nativeEvent.EventType == "" {
		return true, nil
	}
	agg.OnNativeIOEvent(nativeEvent)
	return true, nil
}

func taskEndFromEvent(e *evtTaskEnd) model.TaskEnd {
	var m model.TaskMetrics
	if e.TaskMetrics != nil {
		m.RunMs = e.TaskMetrics.ExecutorRunTime
		m.ExecutorCPUMs = e.TaskMetrics.ExecutorCPUTime / 1_000_000
		m.ExecutorDeserializeMs = e.TaskMetrics.ExecutorDeserializeTime
		m.ResultSerializationMs = e.TaskMetrics.ResultSerializationTime
		m.GettingResultMs = gettingResultMs(e.TaskInfo.GettingResultTime, e.TaskInfo.FinishTime)
		m.GCMs = e.TaskMetrics.JVMGCTime
		m.InputBytes = e.TaskMetrics.InputMetrics.BytesRead
		m.InputRecords = e.TaskMetrics.InputMetrics.RecordsRead
		m.OutputBytes = e.TaskMetrics.OutputMetrics.BytesWritten
		m.OutputRecords = e.TaskMetrics.OutputMetrics.RecordsWritten
		m.ShuffleLocalBytesRead = e.TaskMetrics.ShuffleReadMetrics.LocalBytesRead
		m.ShuffleRemoteBytesRead = e.TaskMetrics.ShuffleReadMetrics.RemoteBytesRead
		m.ShuffleReadBytes = m.ShuffleRemoteBytesRead + m.ShuffleLocalBytesRead
		m.ShuffleTotalBlocksFetched = e.TaskMetrics.ShuffleReadMetrics.TotalBlocksFetched
		m.ShuffleLocalBlocksFetched = e.TaskMetrics.ShuffleReadMetrics.LocalBlocksFetched
		m.ShuffleRemoteBlocksFetched = e.TaskMetrics.ShuffleReadMetrics.RemoteBlocksFetched
		m.ShuffleRecordsRead = e.TaskMetrics.ShuffleReadMetrics.TotalRecordsRead
		m.ShuffleWriteBytes = e.TaskMetrics.ShuffleWriteMetrics.BytesWritten
		m.ShuffleWriteRecords = e.TaskMetrics.ShuffleWriteMetrics.RecordsWritten
		m.SpillDisk = e.TaskMetrics.DiskBytesSpilled
		m.SpillMem = e.TaskMetrics.MemoryBytesSpilled
		m.ResultSizeBytes = e.TaskMetrics.ResultSize
		m.PeakExecutionMemoryBytes = e.TaskMetrics.PeakExecutionMemory
	}
	return model.TaskEnd{
		StageID:     e.StageID,
		Attempt:     e.StageAttemptID,
		ExecutorID:  e.TaskInfo.ExecutorID,
		Failed:      e.TaskInfo.Failed,
		Killed:      e.TaskInfo.Killed,
		Speculative: e.TaskInfo.Speculative,
		LaunchMs:    e.TaskInfo.LaunchTime,
		FinishMs:    e.TaskInfo.FinishTime,
		Metrics:     m,
	}
}

func gettingResultMs(start, finish int64) int64 {
	if start == 0 || finish <= start {
		return 0
	}
	return finish - start
}

func nativeIOFromEvent(e *evtNativeIO) model.NativeIOEvent {
	out := model.NativeIOEvent{
		SQLExecutionID: -1,
		StageID:        -1,
		StageAttemptID: -1,
		TaskAttemptID:  -1,
		TaskIndex:      -1,
		AttemptNumber:  -1,
		Metrics:        map[string]float64{},
	}
	if e.EventJson != "" {
		fillNativeIOFromPayload(&out, e.EventJson)
	}
	if e.NativeIOSchemaVersion != nil {
		out.SchemaVersion = *e.NativeIOSchemaVersion
	}
	if e.NativeIOEventID != "" {
		out.EventID = e.NativeIOEventID
	}
	if e.NativeIOEventTime != nil {
		out.EventTime = *e.NativeIOEventTime
	}
	if e.NativeIOAIKind != "" {
		out.AIKind = e.NativeIOAIKind
	}
	if e.NativeIOAISummary != "" {
		out.AISummary = e.NativeIOAISummary
	}
	if e.NativeIOEventType != "" {
		out.EventType = e.NativeIOEventType
	}
	if e.NativeIOOperationID != "" {
		out.OperationID = e.NativeIOOperationID
	}
	if e.NativeIOOperationName != "" {
		out.OperationName = e.NativeIOOperationName
	}
	if e.NativeIOPhase != "" {
		out.Phase = e.NativeIOPhase
	}
	setInt64(&out.SQLExecutionID, e.NativeIOSQLExecutionID)
	setInt(&out.StageID, e.NativeIOStageID)
	setInt(&out.StageAttemptID, e.NativeIOStageAttemptID)
	setInt64(&out.TaskAttemptID, e.NativeIOTaskAttemptID)
	setInt(&out.TaskIndex, e.NativeIOTaskIndex)
	setInt(&out.AttemptNumber, e.NativeIOAttemptNumber)
	if e.NativeIOExecutorID != "" {
		out.ExecutorID = e.NativeIOExecutorID
	}
	if e.NativeIOHost != "" {
		out.Host = e.NativeIOHost
	}
	setInt64(&out.ThreadID, e.NativeIOThreadID)
	if e.NativeIOFilePath != "" {
		out.FilePath = e.NativeIOFilePath
	}
	if e.NativeIOOutputPath != "" {
		out.OutputPath = e.NativeIOOutputPath
	}
	if e.NativeIOObjectRequestID != "" {
		out.ObjectRequestID = e.NativeIOObjectRequestID
	}
	if e.NativeIOObjectOperation != "" {
		out.ObjectOperation = e.NativeIOObjectOperation
	}
	setInt64(&out.DurationMs, e.NativeIODurationMs)
	setInt64(&out.Rows, e.NativeIORows)
	setInt64(&out.Bytes, e.NativeIOBytes)
	setInt(&out.QueueDepth, e.NativeIOQueueDepth)
	setInt(&out.RuntimeThreads, e.NativeIORuntimeThreads)
	setInt64(&out.NativeMemoryBytes, e.NativeIONativeMemoryBytes)
	setInt64(&out.PeakBufferedBytes, e.NativeIOPeakBufferedBytes)
	if metrics := nativeIOMetricsFromRaw(e.NativeIOMetrics); len(metrics) > 0 {
		out.Metrics = metrics
	}
	if e.NativeIOErrorClass != "" {
		out.ErrorClass = e.NativeIOErrorClass
	}
	if e.NativeIOErrorMessage != "" {
		out.ErrorMessage = e.NativeIOErrorMessage
	}
	if e.NativeIOStackTrace != "" {
		out.StackTrace = e.NativeIOStackTrace
	}
	return out
}

func fillNativeIOFromPayload(out *model.NativeIOEvent, eventJson string) {
	var p nativeIOPayload
	if err := json.Unmarshal([]byte(eventJson), &p); err != nil {
		return
	}
	out.SchemaVersion = p.Version
	out.EventID = p.EventID
	out.EventTime = p.EventTime
	out.EventType = p.EventType
	out.OperationID = p.OperationID
	out.OperationName = p.OperationName
	out.Phase = p.Phase
	setInt64(&out.SQLExecutionID, p.SQLExecutionID)
	setInt(&out.StageID, p.StageID)
	setInt(&out.StageAttemptID, p.StageAttemptID)
	setInt64(&out.TaskAttemptID, p.TaskAttemptID)
	setInt(&out.TaskIndex, p.TaskIndex)
	setInt(&out.AttemptNumber, p.AttemptNumber)
	out.ExecutorID = p.ExecutorID
	out.Host = p.Host
	setInt64(&out.ThreadID, p.ThreadID)
	out.FilePath = p.FilePath
	out.OutputPath = p.OutputPath
	out.ObjectRequestID = p.ObjectRequestID
	out.ObjectOperation = p.ObjectOperation
	setInt64(&out.DurationMs, p.DurationMs)
	setInt64(&out.Rows, p.Rows)
	setInt64(&out.Bytes, p.Bytes)
	setInt(&out.QueueDepth, p.QueueDepth)
	setInt(&out.RuntimeThreads, p.RuntimeThreads)
	setInt64(&out.NativeMemoryBytes, p.NativeMemoryBytes)
	setInt64(&out.PeakBufferedBytes, p.PeakBufferedBytes)
	if metrics := nativeIOMetricsFromJSON(p.MetricsJSON); len(metrics) > 0 {
		out.Metrics = metrics
	}
	out.ErrorClass = p.ErrorClass
	out.ErrorMessage = p.ErrorMessage
	out.StackTrace = p.StackTrace
}

func nativeIOMetricsFromJSON(metricsJSON string) map[string]float64 {
	if strings.TrimSpace(metricsJSON) == "" {
		return nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(metricsJSON), &raw); err != nil {
		return nil
	}
	return nativeIOMetricsFromRaw(raw)
}

func nativeIOMetricsFromRaw(raw map[string]json.RawMessage) map[string]float64 {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]float64)
	for k, v := range raw {
		if n, ok := parseNativeIOMetricValue(v); ok {
			out[k] = n
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseNativeIOMetricValue(raw json.RawMessage) (float64, bool) {
	var n float64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if parsed, err := strconv.ParseFloat(s, 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func setInt64(dst *int64, src *int64) {
	if src != nil {
		*dst = *src
	}
}

func setInt(dst *int, src *int) {
	if src != nil {
		*dst = *src
	}
}

func parseErr(err error) error {
	return cerrors.New(cerrors.CodeLogParseFailed, err.Error(), "")
}
