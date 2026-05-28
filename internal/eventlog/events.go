package eventlog

import "encoding/json"

// Minimal event field structs — only what aggregator consumes.
// Field names match Spark's JSON keys exactly.

type evtBase struct {
	Event string `json:"Event"`
}

type evtAppStart struct {
	AppName   string `json:"App Name"`
	AppID     string `json:"App ID"`
	Timestamp int64  `json:"Timestamp"`
	User      string `json:"User"`
}

type evtAppEnd struct {
	Timestamp int64 `json:"Timestamp"`
}

type evtExecutorAdded struct {
	Timestamp    int64  `json:"Timestamp"`
	ExecutorID   string `json:"Executor ID"`
	ExecutorInfo struct {
		Host       string `json:"Host"`
		TotalCores int    `json:"Total Cores"`
	} `json:"Executor Info"`
}

type evtExecutorRemoved struct {
	Timestamp     int64  `json:"Timestamp"`
	ExecutorID    string `json:"Executor ID"`
	RemovedReason string `json:"Removed Reason"`
}

type evtJobStart struct {
	JobID      int               `json:"Job ID"`
	StageIDs   []int             `json:"Stage IDs"`
	SubmitMs   int64             `json:"Submission Time"`
	Properties map[string]string `json:"Properties"`
}

type evtJobEnd struct {
	JobID     int   `json:"Job ID"`
	EndMs     int64 `json:"Completion Time"`
	JobResult struct {
		Result string `json:"Result"`
	} `json:"Job Result"`
}

type evtStageInfo struct {
	StageID        int    `json:"Stage ID"`
	StageAttemptID int    `json:"Stage Attempt ID"`
	StageName      string `json:"Stage Name"`
	NumberOfTasks  int    `json:"Number of Tasks"`
	SubmitMs       int64  `json:"Submission Time"`
	CompleteMs     int64  `json:"Completion Time"`
	FailureReason  string `json:"Failure Reason"`
}

type evtStageSubmitted struct {
	StageInfo evtStageInfo `json:"Stage Info"`
}

type evtStageCompleted struct {
	StageInfo evtStageInfo `json:"Stage Info"`
}

type evtEnvironmentUpdate struct {
	SparkProperties map[string]string `json:"Spark Properties"`
}

type evtSQLExecutionStart struct {
	ExecutionID int64  `json:"executionId"`
	Description string `json:"description"`
	Details     string `json:"details"`
	Time        int64  `json:"time"`
}

type evtSQLExecutionEnd struct {
	ExecutionID int64 `json:"executionId"`
	Time        int64 `json:"time"`
}

type evtNodeBlacklisted struct {
	Time             int64  `json:"time"`
	HostID           string `json:"hostId"`
	ExecutorFailures int    `json:"executorFailures"`
	StageID          int    `json:"stageId"`
	StageAttemptID   int    `json:"stageAttemptId"`
}

type evtExecutorBlacklisted struct {
	Time           int64  `json:"time"`
	ExecutorID     string `json:"executorId"`
	TaskFailures   int    `json:"taskFailures"`
	StageID        int    `json:"stageId"`
	StageAttemptID int    `json:"stageAttemptId"`
}

type evtTaskEnd struct {
	StageID        int `json:"Stage ID"`
	StageAttemptID int `json:"Stage Attempt ID"`
	TaskInfo       struct {
		TaskID     int    `json:"Task ID"`
		ExecutorID string `json:"Executor ID"`
		Host       string `json:"Host"`
		LaunchTime int64  `json:"Launch Time"`
		FinishTime int64  `json:"Finish Time"`
		Failed     bool   `json:"Failed"`
		Killed     bool   `json:"Killed"`
	} `json:"Task Info"`
	TaskMetrics *struct {
		ExecutorRunTime         int64 `json:"Executor Run Time"`
		ExecutorCPUTime         int64 `json:"Executor CPU Time"`
		ExecutorDeserializeTime int64 `json:"Executor Deserialize Time"`
		JVMGCTime               int64 `json:"JVM GC Time"`
		MemoryBytesSpilled      int64 `json:"Memory Bytes Spilled"`
		DiskBytesSpilled        int64 `json:"Disk Bytes Spilled"`
		InputMetrics            struct {
			BytesRead   int64 `json:"Bytes Read"`
			RecordsRead int64 `json:"Records Read"`
		} `json:"Input Metrics"`
		OutputMetrics struct {
			BytesWritten   int64 `json:"Bytes Written"`
			RecordsWritten int64 `json:"Records Written"`
		} `json:"Output Metrics"`
		ShuffleReadMetrics struct {
			RemoteBytesRead  int64 `json:"Remote Bytes Read"`
			LocalBytesRead   int64 `json:"Local Bytes Read"`
			FetchWaitTime    int64 `json:"Fetch Wait Time"`
			TotalRecordsRead int64 `json:"Total Records Read"`
		} `json:"Shuffle Read Metrics"`
		ShuffleWriteMetrics struct {
			BytesWritten   int64 `json:"Shuffle Bytes Written"`
			WriteTime      int64 `json:"Shuffle Write Time"`
			RecordsWritten int64 `json:"Shuffle Records Written"`
		} `json:"Shuffle Write Metrics"`
	} `json:"Task Metrics"`
}

type evtNativeIO struct {
	EventJson string `json:"eventJson"`

	NativeIOSchemaVersion *int   `json:"native_io_schema_version"`
	NativeIOEventID       string `json:"native_io_event_id"`
	NativeIOEventTime     *int64 `json:"native_io_event_time"`
	NativeIOAIKind        string `json:"native_io_ai_kind"`
	NativeIOAISummary     string `json:"native_io_ai_summary"`
	NativeIOEventType     string `json:"native_io_event_type"`
	NativeIOOperationID   string `json:"native_io_operation_id"`
	NativeIOOperationName string `json:"native_io_operation_name"`
	NativeIOPhase         string `json:"native_io_phase"`

	NativeIOSQLExecutionID *int64 `json:"native_io_sql_execution_id"`
	NativeIOStageID        *int   `json:"native_io_stage_id"`
	NativeIOStageAttemptID *int   `json:"native_io_stage_attempt_id"`
	NativeIOTaskAttemptID  *int64 `json:"native_io_task_attempt_id"`
	NativeIOTaskIndex      *int   `json:"native_io_task_index"`
	NativeIOAttemptNumber  *int   `json:"native_io_attempt_number"`
	NativeIOExecutorID     string `json:"native_io_executor_id"`
	NativeIOHost           string `json:"native_io_host"`
	NativeIOThreadID       *int64 `json:"native_io_thread_id"`

	NativeIOFilePath          string                     `json:"native_io_file_path"`
	NativeIOOutputPath        string                     `json:"native_io_output_path"`
	NativeIOObjectRequestID   string                     `json:"native_io_object_request_id"`
	NativeIOObjectOperation   string                     `json:"native_io_object_operation"`
	NativeIODurationMs        *int64                     `json:"native_io_duration_ms"`
	NativeIORows              *int64                     `json:"native_io_rows"`
	NativeIOBytes             *int64                     `json:"native_io_bytes"`
	NativeIOQueueDepth        *int                       `json:"native_io_queue_depth"`
	NativeIORuntimeThreads    *int                       `json:"native_io_runtime_threads"`
	NativeIONativeMemoryBytes *int64                     `json:"native_io_native_memory_bytes"`
	NativeIOPeakBufferedBytes *int64                     `json:"native_io_peak_buffered_bytes"`
	NativeIOMetrics           map[string]json.RawMessage `json:"native_io_metrics"`
	NativeIOErrorClass        string                     `json:"native_io_error_class"`
	NativeIOErrorMessage      string                     `json:"native_io_error_message"`
	NativeIOStackTrace        string                     `json:"native_io_stack_trace"`
}

type nativeIOPayload struct {
	Version       int    `json:"version"`
	EventID       string `json:"event_id"`
	EventTime     int64  `json:"event_time"`
	EventType     string `json:"event_type"`
	OperationID   string `json:"operation_id"`
	OperationName string `json:"operation_name"`
	Phase         string `json:"phase"`

	SQLExecutionID *int64 `json:"sql_execution_id"`
	StageID        *int   `json:"stage_id"`
	StageAttemptID *int   `json:"stage_attempt_id"`
	TaskAttemptID  *int64 `json:"task_attempt_id"`
	TaskIndex      *int   `json:"task_index"`
	AttemptNumber  *int   `json:"attempt_number"`
	ExecutorID     string `json:"executor_id"`
	Host           string `json:"host"`
	ThreadID       *int64 `json:"thread_id"`

	FilePath          string `json:"file_path"`
	OutputPath        string `json:"output_path"`
	ObjectRequestID   string `json:"object_request_id"`
	ObjectOperation   string `json:"object_operation"`
	DurationMs        *int64 `json:"duration_ms"`
	Rows              *int64 `json:"rows"`
	Bytes             *int64 `json:"bytes"`
	QueueDepth        *int   `json:"queue_depth"`
	RuntimeThreads    *int   `json:"runtime_threads"`
	NativeMemoryBytes *int64 `json:"native_memory_bytes"`
	PeakBufferedBytes *int64 `json:"peak_buffered_bytes"`
	MetricsJSON       string `json:"metrics_json"`
	ErrorClass        string `json:"error_class"`
	ErrorMessage      string `json:"error_message"`
	StackTrace        string `json:"stack_trace"`
}
