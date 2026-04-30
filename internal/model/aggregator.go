package model

type TaskMetrics struct {
	RunMs             int64
	GCMs              int64
	InputBytes        int64
	ShuffleReadBytes  int64
	ShuffleWriteBytes int64
	SpillDisk         int64
	SpillMem          int64
}

type TaskEnd struct {
	StageID, Attempt int
	ExecutorID       string
	Failed, Killed   bool
	LaunchMs         int64
	FinishMs         int64
	Metrics          TaskMetrics
}

type Aggregator struct {
	app           *Application
	concurrentNow int
}

func NewAggregator(app *Application) *Aggregator { return &Aggregator{app: app} }

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

func (a *Aggregator) OnJobStart(id int, stageIDs []int, submitMs int64) {
	a.app.Jobs[id] = &Job{ID: id, StageIDs: stageIDs, StartMs: submitMs}
	a.app.JobsTotal++
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
	dur := t.Metrics.RunMs
	if dur == 0 && t.FinishMs > t.LaunchMs {
		dur = t.FinishMs - t.LaunchMs
	}
	if dur < 0 {
		dur = 0
	}
	firstTask := s.TaskDurations.Count() == 0
	s.TaskDurations.Add(float64(dur))
	s.TaskInputBytes.Add(float64(t.Metrics.InputBytes))
	s.TotalRunMs += dur
	s.TotalGCMs += t.Metrics.GCMs
	s.TotalInputBytes += t.Metrics.InputBytes
	s.TotalShuffleReadBytes += t.Metrics.ShuffleReadBytes
	s.TotalShuffleWriteBytes += t.Metrics.ShuffleWriteBytes
	s.TotalSpillDisk += t.Metrics.SpillDisk
	s.TotalSpillMem += t.Metrics.SpillMem
	if dur > s.MaxTaskMs {
		s.MaxTaskMs = dur
	}
	if firstTask || dur < s.MinTaskMs {
		s.MinTaskMs = dur
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

	a.app.TasksTotal++
	if t.Failed {
		a.app.TasksFailed++
	}
	a.app.TotalRunMs += dur
	a.app.TotalGCMs += t.Metrics.GCMs
	a.app.TotalInputBytes += t.Metrics.InputBytes
	a.app.TotalShuffleReadBytes += t.Metrics.ShuffleReadBytes
	a.app.TotalShuffleWriteBytes += t.Metrics.ShuffleWriteBytes
	a.app.TotalSpillDisk += t.Metrics.SpillDisk

	if e, ok := a.app.Executors[t.ExecutorID]; ok {
		e.TotalRunMs += dur
		e.TotalGCMs += t.Metrics.GCMs
		e.TaskCount++
		if t.Failed {
			e.FailedTaskCount++
		}
	}
}
