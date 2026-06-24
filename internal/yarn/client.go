// Package yarn fetches ResourceManager application metadata and NodeManager
// container log links/snippets for a Spark application.
package yarn

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Options struct {
	TopContainers int
	LogTypes      []string
	MaxLogBytes   int64
	ExecutorID    string
}

type Report struct {
	BaseURL    string          `json:"base_url"`
	App        Application     `json:"app"`
	Containers []ContainerLogs `json:"containers,omitempty"`
	Warnings   []string        `json:"warnings,omitempty"`
}

type Application struct {
	ID              string `json:"id"`
	User            string `json:"user,omitempty"`
	Name            string `json:"name,omitempty"`
	Queue           string `json:"queue,omitempty"`
	State           string `json:"state,omitempty"`
	FinalStatus     string `json:"final_status,omitempty"`
	Diagnostics     string `json:"diagnostics,omitempty"`
	TrackingURL     string `json:"tracking_url,omitempty"`
	AMContainerLogs string `json:"am_container_logs,omitempty"`
}

type ContainerLogs struct {
	ID              string       `json:"id"`
	SparkExecutorID string       `json:"spark_executor_id,omitempty"`
	NodeHTTPAddress string       `json:"node_http_address,omitempty"`
	State           string       `json:"state,omitempty"`
	ExitStatus      int          `json:"exit_status,omitempty"`
	Diagnostics     string       `json:"diagnostics,omitempty"`
	LogURL          string       `json:"log_url,omitempty"`
	Logs            LogSnippets  `json:"logs,omitempty"`
	LogFindings     []LogFinding `json:"log_findings,omitempty"`
}

type LogSnippets map[string]string

type LogFinding struct {
	Type       string `json:"type"`
	Severity   string `json:"severity"`
	LogType    string `json:"log_type,omitempty"`
	Evidence   string `json:"evidence,omitempty"`
	Suggestion string `json:"suggestion,omitempty"`
}

type ThreadDumpReport struct {
	BaseURL            string               `json:"base_url"`
	App                Application          `json:"app"`
	UIURL              string               `json:"ui_url"`
	ThreadDumpURL      string               `json:"thread_dump_url,omitempty"`
	ExecutorID         string               `json:"executor_id"`
	ThreadCount        int                  `json:"thread_count"`
	StateCounts        map[string]int       `json:"state_counts,omitempty"`
	Warnings           []string             `json:"warnings,omitempty"`
	Diagnosis          *ThreadDumpDiagnosis `json:"diagnosis,omitempty"`
	MainThread         *ThreadStackSummary  `json:"main_thread,omitempty"`
	InterestingThreads []ThreadStackSummary `json:"interesting_threads,omitempty"`
	Threads            []ThreadInfo         `json:"threads,omitempty"`
}

type ThreadDumpDiagnosis struct {
	Category   string   `json:"category,omitempty"`
	Evidence   []string `json:"evidence,omitempty"`
	Suggestion string   `json:"suggestion,omitempty"`
}

type ThreadStackSummary struct {
	ThreadID    int64    `json:"thread_id,omitempty"`
	ThreadName  string   `json:"thread_name,omitempty"`
	ThreadState string   `json:"thread_state,omitempty"`
	TopFrame    string   `json:"top_frame,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	StackTrace  []string `json:"stack_trace,omitempty"`
}

type ThreadInfo struct {
	ThreadID          int64          `json:"threadId,omitempty"`
	ThreadName        string         `json:"threadName,omitempty"`
	ThreadState       string         `json:"threadState,omitempty"`
	BlockedByThreadID any            `json:"blockedByThreadId,omitempty"`
	BlockedByLock     string         `json:"blockedByLock,omitempty"`
	HoldingLocks      []string       `json:"holdingLocks,omitempty"`
	StackTrace        any            `json:"stackTrace,omitempty"`
	Raw               map[string]any `json:"raw,omitempty"`
}

type Client struct {
	baseURLs   []string
	httpClient *http.Client
}

type ClientOptions struct {
	InsecureSkipVerify bool
}

func NewClient(baseURLs []string, timeout time.Duration) *Client {
	return NewClientWithOptions(baseURLs, timeout, ClientOptions{})
}

func NewClientWithOptions(baseURLs []string, timeout time.Duration, opts ClientOptions) *Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	out := make([]string, 0, len(baseURLs))
	for _, b := range baseURLs {
		if t := strings.TrimRight(strings.TrimSpace(b), "/"); t != "" {
			out = append(out, t)
		}
	}
	httpClient := &http.Client{Timeout: timeout}
	if opts.InsecureSkipVerify {
		httpClient.Transport = &http.Transport{
			//nolint:gosec // 用户通过配置显式允许内网 HTTPS gateway 使用自签证书。
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}
	return &Client{baseURLs: out, httpClient: httpClient}
}

func (c *Client) FetchApplicationLogs(ctx context.Context, appID string, opts Options) (*Report, error) {
	appID = canonicalAppID(appID)
	if opts.TopContainers <= 0 {
		opts.TopContainers = 5
	}
	if opts.MaxLogBytes < 0 {
		opts.MaxLogBytes = 0
	}
	if len(opts.LogTypes) == 0 {
		opts.LogTypes = []string{"stderr", "stdout", "syslog"}
	}
	var lastErr error
	for _, base := range c.baseURLs {
		rep, err := c.fetchFromBase(ctx, base, appID, opts)
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

func (c *Client) FetchThreadDump(ctx context.Context, appID, executorID string) (*ThreadDumpReport, error) {
	appID = canonicalAppID(appID)
	if executorID == "" {
		executorID = "driver"
	}
	var lastErr error
	for _, base := range c.baseURLs {
		rep, err := c.fetchThreadDumpFromBase(ctx, base, appID, executorID)
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

func (c *Client) fetchFromBase(ctx context.Context, base, appID string, opts Options) (*Report, error) {
	app, err := c.fetchApp(ctx, base, appID)
	if err != nil {
		return nil, err
	}
	opts.LogTypes = normalizeLogTypes(opts.LogTypes)
	rep := &Report{BaseURL: base, App: app}
	if strings.TrimSpace(opts.ExecutorID) != "" {
		containers, warnings := c.fetchExecutorLogs(ctx, base, app, appID, opts)
		rep.Warnings = append(rep.Warnings, warnings...)
		if len(containers) > 0 {
			rep.Containers = containers
			return rep, nil
		}
	}
	attempts, err := c.fetchAttempts(ctx, base, appID)
	if err != nil {
		return nil, err
	}
	for i := len(attempts) - 1; i >= 0 && len(rep.Containers) < opts.TopContainers; i-- {
		cs, err := c.fetchContainers(ctx, base, appID, attempts[i])
		if err != nil {
			rep.Warnings = append(rep.Warnings, err.Error())
		}
		if len(cs) == 0 {
			if am, ok := attempts[i].container(app.User); ok {
				cs = append(cs, am)
			}
		}
		for _, container := range cs {
			if len(rep.Containers) >= opts.TopContainers {
				break
			}
			container.LogURL = normalizeContainerLogURL(base, app.User, container.ID, container.NodeHTTPAddress, container.LogURL)
			if opts.MaxLogBytes > 0 && container.LogURL != "" {
				container.Logs = c.fetchLogSnippets(ctx, container.LogURL, opts.LogTypes, opts.MaxLogBytes)
				container.LogFindings = analyzeLogFindings(container.Logs)
			}
			appendContainer(&rep.Containers, container, opts.TopContainers)
		}
	}
	if len(rep.Containers) < opts.TopContainers {
		htmlContainers, err := c.fetchHTMLContainers(ctx, base, appID, app.User, attempts, opts.TopContainers-len(rep.Containers))
		if err != nil {
			if len(rep.Containers) == 0 {
				rep.Warnings = append(rep.Warnings, err.Error())
			}
		}
		for _, container := range htmlContainers {
			if opts.MaxLogBytes > 0 && container.LogURL != "" {
				container.Logs = c.fetchLogSnippets(ctx, container.LogURL, opts.LogTypes, opts.MaxLogBytes)
				container.LogFindings = analyzeLogFindings(container.Logs)
			}
			appendContainer(&rep.Containers, container, opts.TopContainers)
		}
	}
	return rep, nil
}

func (c *Client) fetchThreadDumpFromBase(ctx context.Context, base, appID, executorID string) (*ThreadDumpReport, error) {
	app, err := c.fetchApp(ctx, base, appID)
	if err != nil {
		return nil, err
	}
	uiURL := sparkUIBaseURL(base, appID, app.TrackingURL)
	endpoint := strings.TrimRight(uiURL, "/") + "/api/v1/applications/" + url.PathEscape(appID) + "/executors/" + url.PathEscape(executorID) + "/threads"
	var threads []ThreadInfo
	if err := c.getJSON(ctx, endpoint, &threads); err != nil {
		return unavailableThreadDumpReport(base, app, uiURL, endpoint, executorID, err), nil
	}
	counts := make(map[string]int)
	for _, th := range threads {
		if th.ThreadState != "" {
			counts[th.ThreadState]++
		}
	}
	diagnosis, mainThread, interesting := summarizeThreadDump(executorID, threads)
	return &ThreadDumpReport{
		BaseURL:            base,
		App:                app,
		UIURL:              uiURL,
		ThreadDumpURL:      endpoint,
		ExecutorID:         executorID,
		ThreadCount:        len(threads),
		StateCounts:        counts,
		Diagnosis:          diagnosis,
		MainThread:         mainThread,
		InterestingThreads: interesting,
		Threads:            threads,
	}, nil
}

func (c *Client) fetchApp(ctx context.Context, base, appID string) (Application, error) {
	var raw struct {
		App struct {
			ID              string `json:"id"`
			User            string `json:"user"`
			Name            string `json:"name"`
			Queue           string `json:"queue"`
			State           string `json:"state"`
			FinalStatus     string `json:"finalStatus"`
			Diagnostics     string `json:"diagnostics"`
			TrackingURL     string `json:"trackingUrl"`
			AMContainerLogs string `json:"amContainerLogs"`
		} `json:"app"`
	}
	if err := c.getJSON(ctx, base+"/ws/v1/cluster/apps/"+url.PathEscape(appID), &raw); err != nil {
		return Application{}, err
	}
	return Application{
		ID:              raw.App.ID,
		User:            raw.App.User,
		Name:            raw.App.Name,
		Queue:           raw.App.Queue,
		State:           raw.App.State,
		FinalStatus:     raw.App.FinalStatus,
		Diagnostics:     strings.TrimSpace(raw.App.Diagnostics),
		TrackingURL:     strings.TrimSpace(raw.App.TrackingURL),
		AMContainerLogs: strings.TrimSpace(raw.App.AMContainerLogs),
	}, nil
}

type appAttempt struct {
	ID              string
	ContainerID     string
	NodeHTTPAddress string
	LogURL          string
	State           string
	Diagnostics     string
}

func (a appAttempt) container(user string) (ContainerLogs, bool) {
	if a.ContainerID == "" && a.LogURL == "" {
		return ContainerLogs{}, false
	}
	id := a.ContainerID
	if id == "" {
		id = containerIDFromLogURL(a.LogURL)
	}
	return ContainerLogs{
		ID:              id,
		NodeHTTPAddress: a.NodeHTTPAddress,
		State:           a.State,
		Diagnostics:     strings.TrimSpace(a.Diagnostics),
		LogURL:          a.LogURL,
	}, id != "" || user != ""
}

func (c *Client) fetchAttempts(ctx context.Context, base, appID string) ([]appAttempt, error) {
	var raw struct {
		AppAttempts struct {
			AppAttempt []struct {
				ID              flexibleString `json:"id"`
				AppAttemptID    flexibleString `json:"appAttemptId"`
				ContainerID     string         `json:"containerId"`
				NodeHTTPAddress string         `json:"nodeHttpAddress"`
				LogURL          string         `json:"logUrl"`
				LogsLink        string         `json:"logsLink"`
				State           string         `json:"appAttemptState"`
				Diagnostics     string         `json:"diagnostics"`
			} `json:"appAttempt"`
		} `json:"appAttempts"`
	}
	if err := c.getJSON(ctx, base+"/ws/v1/cluster/apps/"+url.PathEscape(appID)+"/appattempts", &raw); err != nil {
		return nil, err
	}
	out := make([]appAttempt, 0, len(raw.AppAttempts.AppAttempt))
	for _, a := range raw.AppAttempts.AppAttempt {
		id := string(a.AppAttemptID)
		if id == "" {
			id = normalizeAttemptID(appID, string(a.ID))
		}
		if id == "" {
			continue
		}
		logURL := a.LogURL
		if logURL == "" {
			logURL = a.LogsLink
		}
		out = append(out, appAttempt{
			ID:              id,
			ContainerID:     a.ContainerID,
			NodeHTTPAddress: a.NodeHTTPAddress,
			LogURL:          logURL,
			State:           a.State,
			Diagnostics:     a.Diagnostics,
		})
	}
	return out, nil
}

func (c *Client) fetchContainers(ctx context.Context, base, appID string, attempt appAttempt) ([]ContainerLogs, error) {
	var raw struct {
		Containers struct {
			Container []struct {
				ID              string `json:"id"`
				NodeHTTPAddress string `json:"nodeHttpAddress"`
				LogURL          string `json:"logUrl"`
				LogsLink        string `json:"logsLink"`
				State           string `json:"state"`
				ExitStatus      int    `json:"containerExitStatus"`
				Diagnostics     string `json:"diagnostics"`
			} `json:"container"`
		} `json:"containers"`
	}
	u := base + "/ws/v1/cluster/apps/" + url.PathEscape(appID) + "/appattempts/" + url.PathEscape(attempt.ID) + "/containers"
	if err := c.getJSON(ctx, u, &raw); err != nil {
		return nil, err
	}
	out := make([]ContainerLogs, 0, len(raw.Containers.Container))
	for _, c := range raw.Containers.Container {
		logURL := c.LogURL
		if logURL == "" {
			logURL = c.LogsLink
		}
		out = append(out, ContainerLogs{
			ID:              c.ID,
			NodeHTTPAddress: c.NodeHTTPAddress,
			State:           c.State,
			ExitStatus:      c.ExitStatus,
			Diagnostics:     strings.TrimSpace(c.Diagnostics),
			LogURL:          logURL,
		})
	}
	return out, nil
}

// canonicalAppID 把可能带 attempt 后缀的 appID 还原成 YARN REST / Spark UI 认识的
// 裸 application id。EventLog (尤其 SHS V2) 的文件名常被归一化成
// `application_<ts>_<seq>_<attempt>`,直接拼进 `/ws/v1/cluster/apps/<id>` 会被
// ResourceManager 以 400 拒绝。YARN 的 appID 永远是 `application_<ts>_<seq>` 两段,
// 第三段是 attempt 计数器,必须剥掉。非法/非 application_ 形态原样返回。
func canonicalAppID(appID string) string {
	appID = strings.TrimSpace(appID)
	const prefix = "application_"
	if !strings.HasPrefix(appID, prefix) {
		return appID
	}
	rest := strings.TrimPrefix(appID, prefix)
	parts := strings.Split(rest, "_")
	if len(parts) <= 2 {
		return appID
	}
	// 只保留前两段 (<ts>_<seq>),丢弃 attempt 及之后任何后缀。
	return prefix + parts[0] + "_" + parts[1]
}

func normalizeAttemptID(appID, attemptID string) string {
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" || strings.HasPrefix(attemptID, "appattempt_") {
		return attemptID
	}
	n, err := strconv.Atoi(attemptID)
	if err != nil {
		return attemptID
	}
	const prefix = "application_"
	if !strings.HasPrefix(appID, prefix) {
		return attemptID
	}
	rest := strings.TrimPrefix(appID, prefix)
	parts := strings.SplitN(rest, "_", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return attemptID
	}
	return fmt.Sprintf("appattempt_%s_%s_%06d", parts[0], parts[1], n)
}

func (c *Client) getJSON(ctx context.Context, u string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("yarn: app not found at %s", u)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("yarn: GET %s: status %d", u, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("yarn: decode %s: %w", u, err)
	}
	return nil
}

func sparkUIBaseURL(yarnBase, appID, trackingURL string) string {
	// 只有当 trackingURL 的 host 与 yarnBase host 相同(同一已证明可达的主机)时才直接用它。
	// RM 返回的 trackingURL 在 gateway 部署里常指向内网 AM 主机名
	// (如华为 MRS 的 hw-id-...mrs-mteo.com:8088),本机 DNS 解析不了 ——
	// 而 yarnBase 是刚刚成功拉到 app metadata 的 gateway,可达。host 不一致时
	// 一律走 <yarnBase>/proxy/<appId>,强制路由到可达的 gateway proxy。
	if trackingURL != "" && trackingURL != "UNASSIGNED" && !isYARNAppPageURL(trackingURL, appID) &&
		sameHost(trackingURL, yarnBase) {
		return strings.TrimRight(trackingURL, "/")
	}
	u, err := url.Parse(yarnBase)
	if err != nil {
		return strings.TrimRight(yarnBase, "/") + "/proxy/" + url.PathEscape(appID)
	}
	u.Path = path.Join(u.Path, "proxy", appID)
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimRight(u.String(), "/")
}

// sameHost 判断两个 URL 是否指向同一个 host:port。解析失败时回退到 false
// (宁可走 gateway proxy 这条已证明可达的路径,也不冒险用解析不出的 trackingURL host)。
func sameHost(a, b string) bool {
	ua, err := url.Parse(a)
	if err != nil {
		return false
	}
	ub, err := url.Parse(b)
	if err != nil {
		return false
	}
	return ua.Host != "" && ua.Host == ub.Host
}

func isYARNAppPageURL(trackingURL, appID string) bool {
	u, err := url.Parse(trackingURL)
	if err != nil {
		return strings.Contains(trackingURL, "/cluster/app/"+appID)
	}
	return strings.Contains(u.Path, "/cluster/app/"+appID)
}

type flexibleString string

func (s *flexibleString) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err == nil {
		*s = flexibleString(str)
		return nil
	}
	var num json.Number
	if err := json.Unmarshal(b, &num); err == nil {
		*s = flexibleString(num.String())
		return nil
	}
	if string(b) == "null" {
		*s = ""
		return nil
	}
	return fmt.Errorf("expected string or number, got %s", string(b))
}

func (c *Client) fetchLogSnippets(ctx context.Context, logURL string, types []string, maxBytes int64) map[string]string {
	return c.fetchLogSnippetsFromURLs(ctx, logURL, nil, types, maxBytes)
}

func (c *Client) fetchLogSnippetsFromURLs(ctx context.Context, logURL string, direct map[string]string, types []string, maxBytes int64) LogSnippets {
	out := LogSnippets{}
	for _, typ := range types {
		u := direct[typ]
		if u == "" {
			u = appendLogType(logURL, typ)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			continue
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			continue
		}
		func() {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return
			}
			b, err := io.ReadAll(io.LimitReader(resp.Body, logReadLimit(maxBytes)))
			if err != nil {
				return
			}
			s := extractYARNLogText(string(b))
			if int64(len(s)) > maxBytes {
				s = s[:maxBytes] + "\n...(truncated)"
			}
			out[typ] = s
		}()
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func logReadLimit(maxBytes int64) int64 {
	const htmlHeadroom = 64 << 10
	const maxRead = 1 << 20
	limit := maxBytes + htmlHeadroom + 1
	if limit > maxRead {
		return maxRead
	}
	if limit < maxBytes+1 {
		return maxBytes + 1
	}
	return limit
}

var preBlockRE = regexp.MustCompile(`(?is)<pre[^>]*>(.*?)</pre>`)

func extractYARNLogText(body string) string {
	matches := preBlockRE.FindAllStringSubmatch(body, -1)
	if len(matches) == 0 {
		return body
	}
	parts := make([]string, 0, len(matches))
	for _, match := range matches {
		parts = append(parts, strings.TrimSpace(htmlUnescape(match[1])))
	}
	return strings.Join(parts, "\n")
}

func normalizeLogTypes(types []string) []string {
	if len(types) == 0 {
		return []string{"stderr", "stdout", "syslog"}
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(types))
	var add func(string)
	add = func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if value == "gc" {
			for _, gc := range []string{"gc.log.0.current", "gc.log.1.current", "gc.log"} {
				add(gc)
			}
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	for _, typ := range types {
		for _, part := range strings.Split(typ, ",") {
			add(part)
		}
	}
	if len(out) == 0 {
		return []string{"stderr", "stdout", "syslog"}
	}
	return out
}

func appendLogType(logURL, typ string) string {
	u, err := url.Parse(logURL)
	if err != nil {
		return strings.TrimRight(logURL, "/") + "/" + url.PathEscape(typ)
	}
	u.Path = path.Join(u.Path, typ)
	return u.String()
}

func normalizeContainerLogURL(base, user, containerID, nodeHTTPAddress, existing string) string {
	if existing != "" {
		return logDirectoryURL(existing)
	}
	if base == "" || user == "" || containerID == "" || nodeHTTPAddress == "" {
		return ""
	}
	scheme := "http"
	host, port, err := net.SplitHostPort(nodeHTTPAddress)
	if err != nil {
		host = nodeHTTPAddress
		port = ""
	}
	u, err := url.Parse(base)
	if err != nil {
		return ""
	}
	u.Path = path.Join(u.Path, "nodemanager", "node", "containerlogs", containerID, user)
	q := u.Query()
	q.Set("scheme", scheme)
	q.Set("host", host)
	if port != "" {
		q.Set("port", port)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (c *Client) fetchExecutorLogs(ctx context.Context, base string, app Application, appID string, opts Options) ([]ContainerLogs, []string) {
	executorID := strings.TrimSpace(opts.ExecutorID)
	if executorID == "" {
		return nil, nil
	}
	uiURL := sparkUIBaseURL(base, appID, app.TrackingURL)
	endpoint := strings.TrimRight(uiURL, "/") + "/api/v1/applications/" + url.PathEscape(appID) + "/executors"
	var raw []struct {
		ID           flexibleString    `json:"id"`
		HostPort     string            `json:"hostPort"`
		ExecutorLogs map[string]string `json:"executorLogs"`
	}
	if err := c.getJSON(ctx, endpoint, &raw); err != nil {
		return nil, []string{fmt.Sprintf("spark executor logs unavailable for executor %s: %v", executorID, err)}
	}
	for _, ex := range raw {
		if string(ex.ID) != executorID {
			continue
		}
		direct := make(map[string]string, len(ex.ExecutorLogs))
		for typ, logURL := range ex.ExecutorLogs {
			if normalized := normalizeLogFileURL(base, logURL); normalized != "" {
				direct[typ] = normalized
			}
		}
		logDir := firstLogDirectory(direct)
		if logDir == "" {
			return nil, []string{fmt.Sprintf("spark executor %s has no executorLogs URLs", executorID)}
		}
		logs := LogSnippets(nil)
		if opts.MaxLogBytes > 0 {
			logs = c.fetchLogSnippetsFromURLs(ctx, logDir, direct, opts.LogTypes, opts.MaxLogBytes)
		}
		return []ContainerLogs{{
			ID:              containerIDFromLogURL(logDir),
			SparkExecutorID: executorID,
			NodeHTTPAddress: ex.HostPort,
			LogURL:          logDir,
			Logs:            logs,
			LogFindings:     analyzeLogFindings(logs),
		}}, nil
	}
	return nil, []string{fmt.Sprintf("spark executor %s not found in Spark UI executors API", executorID)}
}

func firstLogDirectory(logs map[string]string) string {
	names := []string{"stderr", "stdout", "syslog"}
	for _, name := range names {
		if u := logs[name]; u != "" {
			return logDirectoryURL(u)
		}
	}
	for _, u := range logs {
		if u != "" {
			return logDirectoryURL(u)
		}
	}
	return ""
}

func normalizeLogFileURL(base, raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "/containerlogs/") {
		return raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	containerID, user, logType := containerLogParts(parsed.Path)
	if containerID == "" || user == "" {
		return raw
	}
	nodeHTTPAddress := parsed.Host
	dir := normalizeContainerLogURL(base, user, containerID, nodeHTTPAddress, "")
	if dir == "" {
		return raw
	}
	if logType == "" {
		return dir
	}
	return appendLogType(dir, logType)
}

func logDirectoryURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return strings.TrimRight(raw, "/")
	}
	containerID, user, _ := logURLParts(parsed.Path)
	if containerID == "" || user == "" {
		return strings.TrimRight(raw, "/")
	}
	segments := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	for i := range segments {
		if segments[i] == user {
			parsed.Path = "/" + path.Join(segments[:i+1]...)
			parsed.RawQuery = ""
			parsed.Fragment = ""
			return strings.TrimRight(parsed.String(), "/")
		}
	}
	return strings.TrimRight(raw, "/")
}

func containerIDFromLogURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	id, _, _ := logURLParts(u.Path)
	return id
}

func logURLParts(p string) (containerID, user, logType string) {
	if id, usr, typ := containerLogParts(p); id != "" {
		return id, usr, typ
	}
	return jobHistoryLogParts(p)
}

func containerLogParts(p string) (containerID, user, logType string) {
	parts := strings.Split(strings.Trim(p, "/"), "/")
	for i := 0; i < len(parts); i++ {
		if parts[i] != "containerlogs" || i+2 >= len(parts) {
			continue
		}
		containerID = parts[i+1]
		user = parts[i+2]
		if i+3 < len(parts) {
			logType = parts[i+3]
		}
		return containerID, user, logType
	}
	return "", "", ""
}

func jobHistoryLogParts(p string) (containerID, user, logType string) {
	parts := strings.Split(strings.Trim(p, "/"), "/")
	for i := 0; i < len(parts); i++ {
		if parts[i] != "logs" {
			continue
		}
		for j := i + 1; j < len(parts); j++ {
			if strings.HasPrefix(parts[j], "container_") {
				containerID = parts[j]
				next := j + 1
				if next < len(parts) && parts[next] == containerID {
					next++
				}
				if next < len(parts) {
					user = parts[next]
				}
				if next+1 < len(parts) {
					logType = parts[next+1]
				}
				return containerID, user, logType
			}
		}
	}
	return "", "", ""
}

var hrefLogRE = regexp.MustCompile(`(?i)href=['"]([^'"]*(?:containerlogs|/logs/)[^'"]*)['"]`)

func (c *Client) fetchHTMLContainers(ctx context.Context, base, appID, user string, attempts []appAttempt, limit int) ([]ContainerLogs, error) {
	if limit <= 0 {
		return nil, nil
	}
	pages := []string{strings.TrimRight(base, "/") + "/cluster/app/" + url.PathEscape(appID)}
	for _, attempt := range attempts {
		if attempt.ID != "" {
			pages = append(pages, strings.TrimRight(base, "/")+"/cluster/appattempt/"+url.PathEscape(attempt.ID))
		}
	}
	var out []ContainerLogs
	var lastErr error
	for _, pageURL := range pages {
		containers, err := c.fetchHTMLContainersFromPage(ctx, base, pageURL, user)
		if err != nil {
			lastErr = err
			continue
		}
		for _, container := range containers {
			appendContainer(&out, container, limit)
			if len(out) >= limit {
				return out, nil
			}
		}
	}
	if len(out) > 0 {
		return out, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("yarn: no container log links found in YARN HTML pages for %s", appID)
}

func (c *Client) fetchHTMLContainersFromPage(ctx context.Context, base, pageURL, defaultUser string) ([]ContainerLogs, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("yarn: GET %s: status %d", pageURL, resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	page, _ := url.Parse(pageURL)
	matches := hrefLogRE.FindAllStringSubmatch(string(b), -1)
	out := make([]ContainerLogs, 0, len(matches))
	for _, match := range matches {
		rawHref := strings.ReplaceAll(match[1], `\/`, `/`)
		href := htmlUnescape(rawHref)
		resolved := resolveURL(page, href)
		id, parsedUser, _ := logURLPartsFromString(resolved)
		if id == "" {
			continue
		}
		if parsedUser == "" {
			parsedUser = defaultUser
		}
		out = append(out, ContainerLogs{
			ID:              id,
			NodeHTTPAddress: nodeAddressFromLogURL(resolved),
			LogURL:          logDirectoryURL(resolved),
		})
		if parsedUser != "" && !strings.Contains(out[len(out)-1].LogURL, "/"+parsedUser) {
			out[len(out)-1].LogURL = normalizeContainerLogURL(base, parsedUser, id, out[len(out)-1].NodeHTTPAddress, out[len(out)-1].LogURL)
		}
	}
	return out, nil
}

func htmlUnescape(s string) string {
	replacer := strings.NewReplacer("&amp;", "&", "&lt;", "<", "&gt;", ">", "&quot;", `"`, "&#39;", "'")
	return replacer.Replace(s)
}

func resolveURL(base *url.URL, href string) string {
	u, err := url.Parse(href)
	if err != nil {
		return href
	}
	if base != nil {
		return base.ResolveReference(u).String()
	}
	return u.String()
}

func logURLPartsFromString(raw string) (containerID, user, logType string) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", ""
	}
	return logURLParts(u.Path)
}

func nodeAddressFromLogURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	q := u.Query()
	if host := q.Get("host"); host != "" {
		if port := q.Get("port"); port != "" {
			return net.JoinHostPort(host, port)
		}
		return host
	}
	if strings.Contains(u.Path, "/jobhistory/logs/") {
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		for i := 0; i+1 < len(parts); i++ {
			if parts[i] == "logs" {
				return parts[i+1]
			}
		}
	}
	return ""
}

func appendContainer(containers *[]ContainerLogs, container ContainerLogs, limit int) {
	if limit <= 0 || len(*containers) >= limit {
		return
	}
	for _, existing := range *containers {
		if existing.ID != "" && existing.ID == container.ID {
			return
		}
		if existing.LogURL != "" && existing.LogURL == container.LogURL {
			return
		}
	}
	*containers = append(*containers, container)
}

func analyzeLogFindings(logs LogSnippets) []LogFinding {
	if len(logs) == 0 {
		return nil
	}
	for typ, body := range logs {
		if evidence := firstMatchingLine(body, []string{"Full GC", "Pause Full", "Pause Young (Concurrent Start)"}); evidence != "" {
			return []LogFinding{{
				Type:       "full_gc",
				Severity:   "critical",
				LogType:    typ,
				Evidence:   evidence,
				Suggestion: "Executor log contains Full GC evidence. Treat heartbeat timeout / executor lost symptoms as likely JVM GC stall first; inspect executor memory, memoryOverhead, GC options, and object pressure.",
			}}
		}
	}
	return nil
}

func firstMatchingLine(body string, needles []string) string {
	for _, line := range strings.Split(body, "\n") {
		for _, needle := range needles {
			if strings.Contains(line, needle) {
				line = strings.TrimSpace(line)
				if len(line) > 300 {
					line = line[:300] + "..."
				}
				return line
			}
		}
	}
	return ""
}
