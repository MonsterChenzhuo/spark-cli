package scenario

// Envelope is the canonical JSON shape returned by every scenario.
// Data is `any` because gc-pressure returns an object, others return arrays.
// Columns mirrors data: []string for arrays, map[string][]string for gc-pressure.
type Envelope struct {
	Scenario     string `json:"scenario"`
	AppID        string `json:"app_id"`
	AppName      string `json:"app_name"`
	LogPath      string `json:"log_path"`
	LogFormat    string `json:"log_format"`
	Compression  string `json:"compression"`
	Incomplete   bool   `json:"incomplete"`
	ParsedEvents int64  `json:"parsed_events"`
	ElapsedMs    int64  `json:"elapsed_ms"`
	Columns      any    `json:"columns"`
	Data         any    `json:"data"`
	Summary      any    `json:"summary,omitempty"`
}

type DiagnoseSummary struct {
	Critical int `json:"critical"`
	Warn     int `json:"warn"`
	OK       int `json:"ok"`
}
