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

	ExecutorsAdded         int
	ExecutorsRemoved       int
	MaxConcurrentExecutors int

	JobsTotal  int
	JobsFailed int

	TasksTotal  int64
	TasksFailed int64

	TotalInputBytes                 int64
	TotalOutputBytes                int64
	TotalShuffleReadBytes           int64
	TotalShuffleWriteBytes          int64
	TotalSpillDisk                  int64
	TotalRunMs                      int64
	TotalGCMs                       int64
	TotalTaskDurationMs             int64
	TotalExecutorCPUMs              int64
	TotalSchedulerDelayMs           int64
	TotalResultSizeBytes            int64
	PeakExecutionMemoryBytes        int64
	TotalInputRecords               int64
	TotalOutputRecords              int64
	TotalShuffleReadRecords         int64
	TotalShuffleWriteRecords        int64
	TotalShuffleLocalReadBytes      int64
	TotalShuffleRemoteReadBytes     int64
	TotalShuffleTotalBlocksFetched  int64
	TotalShuffleLocalBlocksFetched  int64
	TotalShuffleRemoteBlocksFetched int64
	SpeculativeTasks                int64

	// SparkConf is filled from SparkListenerEnvironmentUpdate's Spark Properties.
	// Empty map until that event is observed.
	SparkConf map[string]string

	// SQLExecutions maps SQL executionId to its description, populated by
	// SparkListenerSQLExecutionStart. StageToSQL gives the reverse lookup
	// from stageID to executionId, populated when JobStart properties carry
	// `spark.sql.execution.id`.
	SQLExecutions map[int64]*SQLExecution
	StageToSQL    map[int]int64

	// Blacklists records every Node/Executor Blacklisted-or-Excluded-ForStage
	// event in the order they appeared. Used by failed_tasks rule to
	// distinguish node-level failures from random task failures.
	Blacklists []BlacklistEvent

	// NativeIOEvents records Paimon native IO listener events from Spark
	// EventLog. Scenarios aggregate these records into AI-readable native IO
	// metrics without requiring callers to parse the embedded eventJson.
	NativeIOEvents []NativeIOEvent
}

func NewApplication() *Application {
	return &Application{
		Executors:     map[string]*Executor{},
		Jobs:          map[int]*Job{},
		Stages:        map[StageKey]*Stage{},
		SparkConf:     map[string]string{},
		SQLExecutions: map[int64]*SQLExecution{},
		StageToSQL:    map[int]int64{},
	}
}

type SQLExecution struct {
	ID          int64
	Description string
	Details     string
	StartMs     int64
	EndMs       int64
}

// BlacklistEvent is a unified record for Spark's
// {Node,Executor}{Blacklisted,Excluded}ForStage events. Kind is
// "node" or "executor"; Target is hostId or executorId respectively.
type BlacklistEvent struct {
	Time     int64
	Kind     string
	Target   string
	StageID  int
	Failures int
}

type NativeIOEvent struct {
	SchemaVersion int
	EventID       string
	EventTime     int64
	AIKind        string
	AISummary     string
	EventType     string
	OperationID   string
	OperationName string
	Phase         string

	SQLExecutionID int64
	StageID        int
	StageAttemptID int
	TaskAttemptID  int64
	TaskIndex      int
	AttemptNumber  int
	ExecutorID     string
	Host           string
	ThreadID       int64

	FilePath          string
	OutputPath        string
	ObjectRequestID   string
	ObjectOperation   string
	DurationMs        int64
	Rows              int64
	Bytes             int64
	QueueDepth        int
	RuntimeThreads    int
	NativeMemoryBytes int64
	PeakBufferedBytes int64
	Metrics           map[string]float64
	ErrorClass        string
	ErrorMessage      string
	StackTrace        string
}

type Executor struct {
	ID, Host              string
	Cores                 int
	AddMs, RemoveMs       int64
	RemoveReason          string
	TotalRunMs            int64
	TotalGCMs             int64
	TotalTaskDurationMs   int64
	TotalExecutorCPUMs    int64
	TotalSchedulerDelayMs int64
	SpeculativeTasks      int64
	TaskCount             int
	FailedTaskCount       int
}

type Job struct {
	ID             int
	StageIDs       []int
	StartMs        int64
	EndMs          int64
	Result         string // "succeeded" | "failed"
	SQLExecutionID int64  // -1 if no spark.sql.execution.id in JobStart properties
}

type Stage struct {
	ID, Attempt          int
	Name                 string
	NumTasks             int
	SubmitMs, CompleteMs int64
	Status               string

	TaskDurations  *stats.Digest
	TaskInputBytes *stats.Digest

	TotalInputBytes                 int64
	TotalShuffleReadBytes           int64
	TotalShuffleWriteBytes          int64
	TotalSpillDisk                  int64
	TotalSpillMem                   int64
	TotalGCMs                       int64
	TotalRunMs                      int64
	TotalTaskDurationMs             int64
	TotalExecutorCPUMs              int64
	TotalSchedulerDelayMs           int64
	TotalResultSerializationMs      int64
	TotalGettingResultMs            int64
	MaxResultSizeBytes              int64
	PeakExecutionMemoryBytes        int64
	TotalInputRecords               int64
	TotalOutputBytes                int64
	TotalOutputRecords              int64
	TotalShuffleReadRecords         int64
	TotalShuffleWriteRecords        int64
	TotalShuffleLocalReadBytes      int64
	TotalShuffleRemoteReadBytes     int64
	TotalShuffleTotalBlocksFetched  int64
	TotalShuffleLocalBlocksFetched  int64
	TotalShuffleRemoteBlocksFetched int64
	SpeculativeTasks                int64
	FailedTasks                     int
	KilledTasks                     int
	MaxTaskMs                       int64
	MinTaskMs                       int64
	MaxInputBytes                   int64
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
