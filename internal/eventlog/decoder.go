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
	switch event {
	case "SparkListenerApplicationStart":
		var e evtAppStart
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		agg.OnAppStart(e.AppID, e.AppName, e.User, e.Timestamp)
	case "SparkListenerApplicationEnd":
		var e evtAppEnd
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		agg.OnAppEnd(e.Timestamp)
	case "SparkListenerExecutorAdded":
		var e evtExecutorAdded
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		agg.OnExecutorAdded(e.ExecutorID, e.ExecutorInfo.Host, e.ExecutorInfo.TotalCores, e.Timestamp)
	case "SparkListenerExecutorRemoved":
		var e evtExecutorRemoved
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		agg.OnExecutorRemoved(e.ExecutorID, e.Timestamp, e.RemovedReason)
	case "SparkListenerJobStart":
		var e evtJobStart
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		agg.OnJobStart(e.JobID, e.StageIDs, e.SubmitMs)
	case "SparkListenerJobEnd":
		var e evtJobEnd
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		agg.OnJobEnd(e.JobID, e.EndMs, e.JobResult.Result)
	case "SparkListenerStageSubmitted":
		var e evtStageSubmitted
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
		}
		s := e.StageInfo
		agg.OnStageSubmitted(s.StageID, s.StageAttemptID, s.StageName, s.NumberOfTasks, s.SubmitMs)
	case "SparkListenerStageCompleted":
		var e evtStageCompleted
		if err := json.Unmarshal(raw, &e); err != nil {
			return parseErr(err)
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
			return parseErr(err)
		}
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
		agg.OnTaskEnd(model.TaskEnd{
			StageID:    e.StageID,
			Attempt:    e.StageAttemptID,
			ExecutorID: e.TaskInfo.ExecutorID,
			Failed:     e.TaskInfo.Failed,
			Killed:     e.TaskInfo.Killed,
			LaunchMs:   e.TaskInfo.LaunchTime,
			FinishMs:   e.TaskInfo.FinishTime,
			Metrics:    m,
		})
	}
	// Unknown events skipped silently.
	return nil
}

func parseErr(err error) error {
	return cerrors.New(cerrors.CodeLogParseFailed, err.Error(), "")
}
