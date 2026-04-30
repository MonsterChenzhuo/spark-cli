// Package model holds the in-memory aggregated Spark application model.
package model

import "github.com/opay-bigdata/spark-cli/internal/stats"

type StageKey struct {
	ID      int
	Attempt int
}

type Application struct {
	ID, Name, User string
	StartMs, EndMs int64
	DurationMs     int64

	Executors map[string]*Executor
	Jobs      map[int]*Job
	Stages    map[StageKey]*Stage
}

func NewApplication() *Application {
	return &Application{
		Executors: map[string]*Executor{},
		Jobs:      map[int]*Job{},
		Stages:    map[StageKey]*Stage{},
	}
}

type Executor struct {
	ID, Host        string
	Cores           int
	AddMs, RemoveMs int64
	RemoveReason    string
	TotalRunMs      int64
	TotalGCMs       int64
	TaskCount       int
	FailedTaskCount int
}

type Job struct {
	ID       int
	StageIDs []int
	StartMs  int64
	EndMs    int64
	Result   string // "succeeded" | "failed"
}

type Stage struct {
	ID, Attempt          int
	Name                 string
	NumTasks             int
	SubmitMs, CompleteMs int64
	Status               string

	TaskDurations  *stats.Digest
	TaskInputBytes *stats.Digest

	TotalShuffleReadBytes  int64
	TotalShuffleWriteBytes int64
	TotalSpillDisk         int64
	TotalSpillMem          int64
	TotalGCMs              int64
	TotalRunMs             int64
	FailedTasks            int
	KilledTasks            int
	MaxTaskMs              int64
	MinTaskMs              int64
	MaxInputBytes          int64
}

func NewStage(id, attempt int, name string, numTasks int, submitMs int64) *Stage {
	return &Stage{
		ID:             id,
		Attempt:        attempt,
		Name:           name,
		NumTasks:       numTasks,
		SubmitMs:       submitMs,
		Status:         "running",
		TaskDurations:  stats.NewDigest(),
		TaskInputBytes: stats.NewDigest(),
	}
}
