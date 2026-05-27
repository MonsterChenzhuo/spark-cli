package yarn

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

type PaimonDiagnosticsReport struct {
	BaseURL       string         `json:"base_url"`
	App           Application    `json:"app"`
	UIURL         string         `json:"ui_url"`
	OverviewURL   string         `json:"overview_url,omitempty"`
	ThreadDumpURL string         `json:"thread_dump_url,omitempty"`
	ProfilerURL   string         `json:"profiler_url,omitempty"`
	ExecutorID    string         `json:"executor_id"`
	Overview      map[string]any `json:"overview,omitempty"`
	ThreadDump    map[string]any `json:"thread_dump,omitempty"`
	Profiler      map[string]any `json:"profiler,omitempty"`
	Warnings      []string       `json:"warnings,omitempty"`
}

func (c *Client) FetchPaimonDiagnostics(ctx context.Context, appID, executorID string) (*PaimonDiagnosticsReport, error) {
	if executorID == "" {
		executorID = "driver"
	}
	var lastErr error
	for _, base := range c.baseURLs {
		rep, err := c.fetchPaimonDiagnosticsFromBase(ctx, base, appID, executorID)
		if err == nil {
			return rep, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("yarn: no base_urls configured")
	}
	return nil, lastErr
}

func (c *Client) fetchPaimonDiagnosticsFromBase(ctx context.Context, base, appID, executorID string) (*PaimonDiagnosticsReport, error) {
	app, err := c.fetchApp(ctx, base, appID)
	if err != nil {
		return nil, err
	}
	uiURL := sparkUIBaseURL(base, appID, app.TrackingURL)
	root := strings.TrimRight(uiURL, "/")
	rep := &PaimonDiagnosticsReport{
		BaseURL:       base,
		App:           app,
		UIURL:         uiURL,
		OverviewURL:   root + "/paimon-diagnostics/json",
		ThreadDumpURL: root + "/paimon-diagnostics/threadDump/json?executorId=" + url.QueryEscape(executorID),
		ProfilerURL:   root + "/paimon-diagnostics/profiler/json",
		ExecutorID:    executorID,
	}
	rep.Overview = c.fetchOptionalJSON(ctx, rep.OverviewURL, "overview", &rep.Warnings)
	rep.ThreadDump = c.fetchOptionalJSON(ctx, rep.ThreadDumpURL, "thread_dump", &rep.Warnings)
	rep.Profiler = c.fetchOptionalJSON(ctx, rep.ProfilerURL, "profiler", &rep.Warnings)
	return rep, nil
}

func (c *Client) fetchOptionalJSON(ctx context.Context, endpoint, name string, warnings *[]string) map[string]any {
	var out map[string]any
	if err := c.getJSON(ctx, endpoint, &out); err != nil {
		*warnings = append(*warnings, fmt.Sprintf("fetch %s failed: %v", name, err))
		return nil
	}
	return out
}
