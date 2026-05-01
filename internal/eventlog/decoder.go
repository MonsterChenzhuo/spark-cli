package eventlog

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"

	"github.com/opay-bigdata/spark-cli/internal/model"

	cerrors "github.com/opay-bigdata/spark-cli/internal/errors"
)

const maxScanLine = 16 << 20 // 16MB per JSONL line

func Decode(r io.Reader, agg *model.Aggregator) (int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), maxScanLine)
	parsed := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var base evtBase
		if err := json.Unmarshal(line, &base); err != nil {
			return parsed, cerrors.New(cerrors.CodeLogParseFailed, err.Error(), "line "+itoa(parsed+1))
		}
		if err := dispatch(base.Event, line, agg); err != nil {
			return parsed, err
		}
		parsed++
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return parsed, cerrors.New(cerrors.CodeLogParseFailed,
				"event JSONL line exceeds 16MB", "file may be corrupted")
		}
		return parsed, cerrors.New(cerrors.CodeLogUnreadable, err.Error(), "")
	}
	return parsed, nil
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

func taskEndFromEvent(e *evtTaskEnd) model.TaskEnd {
	var m model.TaskMetrics
	if e.TaskMetrics != nil {
		m.RunMs = e.TaskMetrics.ExecutorRunTime
		m.GCMs = e.TaskMetrics.JVMGCTime
		m.InputBytes = e.TaskMetrics.InputMetrics.BytesRead
		m.ShuffleReadBytes = e.TaskMetrics.ShuffleReadMetrics.RemoteBytesRead + e.TaskMetrics.ShuffleReadMetrics.LocalBytesRead
		m.ShuffleWriteBytes = e.TaskMetrics.ShuffleWriteMetrics.BytesWritten
		m.SpillDisk = e.TaskMetrics.DiskBytesSpilled
		m.SpillMem = e.TaskMetrics.MemoryBytesSpilled
	}
	return model.TaskEnd{
		StageID:    e.StageID,
		Attempt:    e.StageAttemptID,
		ExecutorID: e.TaskInfo.ExecutorID,
		Failed:     e.TaskInfo.Failed,
		Killed:     e.TaskInfo.Killed,
		LaunchMs:   e.TaskInfo.LaunchTime,
		FinishMs:   e.TaskInfo.FinishTime,
		Metrics:    m,
	}
}

func parseErr(err error) error {
	return cerrors.New(cerrors.CodeLogParseFailed, err.Error(), "")
}
