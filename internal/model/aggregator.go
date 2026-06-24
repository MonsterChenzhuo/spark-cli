package model

import "strconv"

type TaskMetrics struct {
	RunMs                      int64
	ExecutorCPUMs              int64
	ExecutorDeserializeMs      int64
	ResultSerializationMs      int64
	GettingResultMs            int64
	GCMs                       int64
	InputBytes                 int64
	InputRecords               int64
	OutputBytes                int64
	OutputRecords              int64
	ShuffleReadBytes           int64
	ShuffleWriteBytes          int64
	ShuffleLocalBytesRead      int64
	ShuffleRemoteBytesRead     int64
	ShuffleTotalBlocksFetched  int64
	ShuffleLocalBlocksFetched  int64
	ShuffleRemoteBlocksFetched int64
	ShuffleRecordsRead         int64
	ShuffleWriteRecords        int64
	SpillDisk                  int64
	SpillMem                   int64
	ResultSizeBytes            int64
	PeakExecutionMemoryBytes   int64
}

type TaskEnd struct {
	StageID, Attempt int
	ExecutorID       string
	Failed, Killed   bool
	Speculative      bool
	LaunchMs         int64
	FinishMs         int64
	Metrics          TaskMetrics
}

type Aggregator struct {
	app           *Application
	concurrentNow int
}

func NewAggregator(app *Application) *Aggregator { return &Aggregator{app: app} }

type taskDerivedMetrics struct {
	runMs            int64
	taskDurationMs   int64
	schedulerDelayMs int64
	shuffleReadBytes int64
}

func (a *Aggregator) OnAppStart(id, name, user string, ts int64) {
	a.app.ID = id
	a.app.Name = name
	a.app.User = user
	a.app.StartMs = ts
}

func (a *Aggregator) OnAppEnd(ts int64) {
	a.app.EndMs = ts
	if a.app.StartMs > 0 {
		a.app.DurationMs = ts - a.app.StartMs
	}
}

func (a *Aggregator) OnExecutorAdded(id, host string, cores int, ts int64) {
	a.app.Executors[id] = &Executor{ID: id, Host: host, Cores: cores, AddMs: ts}
	a.app.ExecutorsAdded++
	a.concurrentNow++
	if a.concurrentNow > a.app.MaxConcurrentExecutors {
		a.app.MaxConcurrentExecutors = a.concurrentNow
	}
}

func (a *Aggregator) OnExecutorRemoved(id string, ts int64, reason string) {
	if e, ok := a.app.Executors[id]; ok {
		e.RemoveMs = ts
		e.RemoveReason = reason
	}
	a.app.ExecutorsRemoved++
	if a.concurrentNow > 0 {
		a.concurrentNow--
	}
}

func (a *Aggregator) OnJobStart(id int, stageIDs []int, submitMs int64, props map[string]string) {
	sqlID := int64(-1)
	if v, ok := props["spark.sql.execution.id"]; ok && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			sqlID = n
			for _, sid := range stageIDs {
				a.app.StageToSQL[sid] = n
			}
		}
	}
	a.app.Jobs[id] = &Job{ID: id, StageIDs: stageIDs, StartMs: submitMs, SQLExecutionID: sqlID}
	a.app.JobsTotal++
}

func (a *Aggregator) OnEnvironmentUpdate(sparkProps map[string]string) {
	if a.app.SparkConf == nil {
		a.app.SparkConf = map[string]string{}
	}
	for k, v := range sparkProps {
		a.app.SparkConf[k] = v
	}
}

func (a *Aggregator) OnSQLExecutionStart(id int64, description, details string, ts int64) {
	if a.app.SQLExecutions == nil {
		a.app.SQLExecutions = map[int64]*SQLExecution{}
	}
	a.app.SQLExecutions[id] = &SQLExecution{
		ID:          id,
		Description: description,
		Details:     details,
		StartMs:     ts,
	}
}

func (a *Aggregator) OnSQLExecutionEnd(id, ts int64) {
	if e, ok := a.app.SQLExecutions[id]; ok {
		e.EndMs = ts
	}
}

func (a *Aggregator) OnNodeBlacklisted(ts int64, host string, stageID, failures int) {
	a.app.Blacklists = append(a.app.Blacklists, BlacklistEvent{
		Time: ts, Kind: "node", Target: host, StageID: stageID, Failures: failures,
	})
}

func (a *Aggregator) OnExecutorBlacklisted(ts int64, executorID string, stageID, failures int) {
	a.app.Blacklists = append(a.app.Blacklists, BlacklistEvent{
		Time: ts, Kind: "executor", Target: executorID, StageID: stageID, Failures: failures,
	})
}

func (a *Aggregator) OnNativeIOEvent(event NativeIOEvent) {
	if event.Metrics == nil {
		event.Metrics = map[string]float64{}
	}
	a.app.NativeIOEvents = append(a.app.NativeIOEvents, event)
}

func (a *Aggregator) OnJobEnd(id int, endMs int64, result string) {
	if j, ok := a.app.Jobs[id]; ok {
		j.EndMs = endMs
		if result == "JobSucceeded" {
			j.Result = "succeeded"
		} else {
			j.Result = "failed"
		}
	}
	if result != "JobSucceeded" {
		a.app.JobsFailed++
	}
}

func (a *Aggregator) OnStageSubmitted(id, attempt int, name string, numTasks int, submitMs int64) {
	k := StageKey{ID: id, Attempt: attempt}
	if _, exists := a.app.Stages[k]; !exists {
		a.app.Stages[k] = NewStage(id, attempt, name, numTasks, submitMs)
	}
}

func (a *Aggregator) OnStageCompleted(id, attempt int, completeMs int64, status string) {
	k := StageKey{ID: id, Attempt: attempt}
	s, ok := a.app.Stages[k]
	if !ok {
		s = NewStage(id, attempt, "", 0, 0)
		a.app.Stages[k] = s
	}
	s.CompleteMs = completeMs
	s.Status = status
}

func (a *Aggregator) OnTaskEnd(t TaskEnd) {
	k := StageKey{ID: t.StageID, Attempt: t.Attempt}
	s, ok := a.app.Stages[k]
	if !ok {
		s = NewStage(t.StageID, t.Attempt, "", 0, 0)
		a.app.Stages[k] = s
	}
	d := deriveTaskMetrics(t)
	addStageTaskMetrics(s, t, d)
	addAppTaskMetrics(a.app, t, d)
	addExecutorTaskMetrics(a.app.Executors[t.ExecutorID], t, d)
}

func deriveTaskMetrics(t TaskEnd) taskDerivedMetrics {
	taskDuration := t.FinishMs - t.LaunchMs
	if taskDuration < 0 {
		taskDuration = 0
	}
	shuffleReadBytes := t.Metrics.ShuffleReadBytes
	if shuffleReadBytes == 0 {
		shuffleReadBytes = t.Metrics.ShuffleLocalBytesRead + t.Metrics.ShuffleRemoteBytesRead
	}
	return taskDerivedMetrics{
		runMs:            taskRunMs(t),
		taskDurationMs:   taskDuration,
		schedulerDelayMs: schedulerDelayMs(t, taskDuration),
		shuffleReadBytes: shuffleReadBytes,
	}
}

func taskRunMs(t TaskEnd) int64 {
	runMs := t.Metrics.RunMs
	if runMs == 0 && t.FinishMs > t.LaunchMs {
		runMs = t.FinishMs - t.LaunchMs
	}
	if runMs < 0 {
		return 0
	}
	return runMs
}

func schedulerDelayMs(t TaskEnd, taskDuration int64) int64 {
	delay := taskDuration - t.Metrics.RunMs - t.Metrics.ExecutorDeserializeMs -
		t.Metrics.ResultSerializationMs - t.Metrics.GettingResultMs
	if delay < 0 {
		return 0
	}
	return delay
}

func addStageTaskMetrics(s *Stage, t TaskEnd, d taskDerivedMetrics) {
	firstTask := s.TaskDurations.Count() == 0
	s.TaskDurations.Add(float64(d.runMs))
	s.TaskInputBytes.Add(float64(t.Metrics.InputBytes))
	s.TotalRunMs += d.runMs
	s.TotalTaskDurationMs += d.taskDurationMs
	s.TotalExecutorCPUMs += t.Metrics.ExecutorCPUMs
	s.TotalSchedulerDelayMs += d.schedulerDelayMs
	s.TotalResultSerializationMs += t.Metrics.ResultSerializationMs
	s.TotalGettingResultMs += t.Metrics.GettingResultMs
	s.TotalGCMs += t.Metrics.GCMs
	s.TotalInputBytes += t.Metrics.InputBytes
	s.TotalInputRecords += t.Metrics.InputRecords
	s.TotalOutputBytes += t.Metrics.OutputBytes
	s.TotalOutputRecords += t.Metrics.OutputRecords
	s.TotalShuffleReadBytes += d.shuffleReadBytes
	s.TotalShuffleWriteBytes += t.Metrics.ShuffleWriteBytes
	s.TotalShuffleLocalReadBytes += t.Metrics.ShuffleLocalBytesRead
	s.TotalShuffleRemoteReadBytes += t.Metrics.ShuffleRemoteBytesRead
	s.TotalShuffleTotalBlocksFetched += t.Metrics.ShuffleTotalBlocksFetched
	s.TotalShuffleLocalBlocksFetched += t.Metrics.ShuffleLocalBlocksFetched
	s.TotalShuffleRemoteBlocksFetched += t.Metrics.ShuffleRemoteBlocksFetched
	s.TotalShuffleReadRecords += t.Metrics.ShuffleRecordsRead
	s.TotalShuffleWriteRecords += t.Metrics.ShuffleWriteRecords
	s.TotalSpillDisk += t.Metrics.SpillDisk
	s.TotalSpillMem += t.Metrics.SpillMem
	if t.Metrics.ResultSizeBytes > s.MaxResultSizeBytes {
		s.MaxResultSizeBytes = t.Metrics.ResultSizeBytes
	}
	if t.Metrics.PeakExecutionMemoryBytes > s.PeakExecutionMemoryBytes {
		s.PeakExecutionMemoryBytes = t.Metrics.PeakExecutionMemoryBytes
	}
	if d.runMs > s.MaxTaskMs {
		s.MaxTaskMs = d.runMs
	}
	if firstTask || d.runMs < s.MinTaskMs {
		s.MinTaskMs = d.runMs
	}
	if t.Metrics.InputBytes > s.MaxInputBytes {
		s.MaxInputBytes = t.Metrics.InputBytes
	}
	if t.Failed {
		s.FailedTasks++
	}
	if t.Killed {
		s.KilledTasks++
	}
	if t.Speculative {
		s.SpeculativeTasks++
	}
}

func addAppTaskMetrics(app *Application, t TaskEnd, d taskDerivedMetrics) {
	app.TasksTotal++
	if t.Failed {
		app.TasksFailed++
	}
	app.TotalRunMs += d.runMs
	app.TotalTaskDurationMs += d.taskDurationMs
	app.TotalExecutorCPUMs += t.Metrics.ExecutorCPUMs
	app.TotalSchedulerDelayMs += d.schedulerDelayMs
	app.TotalGCMs += t.Metrics.GCMs
	app.TotalInputBytes += t.Metrics.InputBytes
	app.TotalInputRecords += t.Metrics.InputRecords
	app.TotalOutputBytes += t.Metrics.OutputBytes
	app.TotalOutputRecords += t.Metrics.OutputRecords
	app.TotalShuffleReadBytes += d.shuffleReadBytes
	app.TotalShuffleWriteBytes += t.Metrics.ShuffleWriteBytes
	app.TotalShuffleLocalReadBytes += t.Metrics.ShuffleLocalBytesRead
	app.TotalShuffleRemoteReadBytes += t.Metrics.ShuffleRemoteBytesRead
	app.TotalShuffleTotalBlocksFetched += t.Metrics.ShuffleTotalBlocksFetched
	app.TotalShuffleLocalBlocksFetched += t.Metrics.ShuffleLocalBlocksFetched
	app.TotalShuffleRemoteBlocksFetched += t.Metrics.ShuffleRemoteBlocksFetched
	app.TotalShuffleReadRecords += t.Metrics.ShuffleRecordsRead
	app.TotalShuffleWriteRecords += t.Metrics.ShuffleWriteRecords
	app.TotalSpillDisk += t.Metrics.SpillDisk
	if t.Metrics.ResultSizeBytes > app.TotalResultSizeBytes {
		app.TotalResultSizeBytes = t.Metrics.ResultSizeBytes
	}
	if t.Metrics.PeakExecutionMemoryBytes > app.PeakExecutionMemoryBytes {
		app.PeakExecutionMemoryBytes = t.Metrics.PeakExecutionMemoryBytes
	}
	if t.Speculative {
		app.SpeculativeTasks++
	}
}

func addExecutorTaskMetrics(e *Executor, t TaskEnd, d taskDerivedMetrics) {
	if e == nil {
		return
	}
	e.TotalRunMs += d.runMs
	e.TotalTaskDurationMs += d.taskDurationMs
	e.TotalExecutorCPUMs += t.Metrics.ExecutorCPUMs
	e.TotalSchedulerDelayMs += d.schedulerDelayMs
	e.TotalGCMs += t.Metrics.GCMs
	e.TaskCount++
	if t.Failed {
		e.FailedTaskCount++
	}
	if t.Speculative {
		e.SpeculativeTasks++
	}
}
