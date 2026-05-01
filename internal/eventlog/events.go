package eventlog

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
