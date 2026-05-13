// Package yarn fetches ResourceManager application metadata and NodeManager
// container log links/snippets for a Spark application.
package yarn

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

type Options struct {
	TopContainers int
	LogTypes      []string
	MaxLogBytes   int64
}

type Report struct {
	BaseURL    string          `json:"base_url"`
	App        Application     `json:"app"`
	Containers []ContainerLogs `json:"containers,omitempty"`
	Warnings   []string        `json:"warnings,omitempty"`
}

type Application struct {
	ID          string `json:"id"`
	User        string `json:"user,omitempty"`
	Name        string `json:"name,omitempty"`
	Queue       string `json:"queue,omitempty"`
	State       string `json:"state,omitempty"`
	FinalStatus string `json:"final_status,omitempty"`
	Diagnostics string `json:"diagnostics,omitempty"`
}

type ContainerLogs struct {
	ID              string            `json:"id"`
	NodeHTTPAddress string            `json:"node_http_address,omitempty"`
	State           string            `json:"state,omitempty"`
	ExitStatus      int               `json:"exit_status,omitempty"`
	Diagnostics     string            `json:"diagnostics,omitempty"`
	LogURL          string            `json:"log_url,omitempty"`
	Logs            map[string]string `json:"logs,omitempty"`
}

type Client struct {
	baseURLs   []string
	httpClient *http.Client
}

func NewClient(baseURLs []string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	out := make([]string, 0, len(baseURLs))
	for _, b := range baseURLs {
		if t := strings.TrimRight(strings.TrimSpace(b), "/"); t != "" {
			out = append(out, t)
		}
	}
	return &Client{baseURLs: out, httpClient: &http.Client{Timeout: timeout}}
}

func (c *Client) FetchApplicationLogs(ctx context.Context, appID string, opts Options) (*Report, error) {
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

func (c *Client) fetchFromBase(ctx context.Context, base, appID string, opts Options) (*Report, error) {
	app, err := c.fetchApp(ctx, base, appID)
	if err != nil {
		return nil, err
	}
	attempts, err := c.fetchAttempts(ctx, base, appID)
	if err != nil {
		return nil, err
	}
	rep := &Report{BaseURL: base, App: app}
	for i := len(attempts) - 1; i >= 0 && len(rep.Containers) < opts.TopContainers; i-- {
		cs, err := c.fetchContainers(ctx, base, appID, attempts[i])
		if err != nil {
			rep.Warnings = append(rep.Warnings, err.Error())
			continue
		}
		for _, container := range cs {
			if len(rep.Containers) >= opts.TopContainers {
				break
			}
			container.LogURL = normalizeLogURL(base, app.User, container.ID, container.NodeHTTPAddress, container.LogURL)
			if opts.MaxLogBytes > 0 && container.LogURL != "" {
				container.Logs = c.fetchLogSnippets(ctx, container.LogURL, opts.LogTypes, opts.MaxLogBytes)
			}
			rep.Containers = append(rep.Containers, container)
		}
	}
	return rep, nil
}

func (c *Client) fetchApp(ctx context.Context, base, appID string) (Application, error) {
	var raw struct {
		App struct {
			ID          string `json:"id"`
			User        string `json:"user"`
			Name        string `json:"name"`
			Queue       string `json:"queue"`
			State       string `json:"state"`
			FinalStatus string `json:"finalStatus"`
			Diagnostics string `json:"diagnostics"`
		} `json:"app"`
	}
	if err := c.getJSON(ctx, base+"/ws/v1/cluster/apps/"+url.PathEscape(appID), &raw); err != nil {
		return Application{}, err
	}
	return Application{
		ID:          raw.App.ID,
		User:        raw.App.User,
		Name:        raw.App.Name,
		Queue:       raw.App.Queue,
		State:       raw.App.State,
		FinalStatus: raw.App.FinalStatus,
		Diagnostics: strings.TrimSpace(raw.App.Diagnostics),
	}, nil
}

func (c *Client) fetchAttempts(ctx context.Context, base, appID string) ([]string, error) {
	var raw struct {
		AppAttempts struct {
			AppAttempt []struct {
				ID string `json:"id"`
			} `json:"appAttempt"`
		} `json:"appAttempts"`
	}
	if err := c.getJSON(ctx, base+"/ws/v1/cluster/apps/"+url.PathEscape(appID)+"/appattempts", &raw); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(raw.AppAttempts.AppAttempt))
	for _, a := range raw.AppAttempts.AppAttempt {
		if a.ID != "" {
			out = append(out, a.ID)
		}
	}
	return out, nil
}

func (c *Client) fetchContainers(ctx context.Context, base, appID, attemptID string) ([]ContainerLogs, error) {
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
	u := base + "/ws/v1/cluster/apps/" + url.PathEscape(appID) + "/appattempts/" + url.PathEscape(attemptID) + "/containers"
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

func (c *Client) fetchLogSnippets(ctx context.Context, logURL string, types []string, maxBytes int64) map[string]string {
	out := map[string]string{}
	for _, typ := range types {
		u := appendLogType(logURL, typ)
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
			b, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
			if err != nil {
				return
			}
			s := string(b)
			if int64(len(b)) > maxBytes {
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

func appendLogType(logURL, typ string) string {
	u, err := url.Parse(logURL)
	if err != nil {
		return strings.TrimRight(logURL, "/") + "/" + url.PathEscape(typ)
	}
	u.Path = path.Join(u.Path, typ)
	return u.String()
}

func normalizeLogURL(base, user, containerID, nodeHTTPAddress, existing string) string {
	if existing != "" {
		return existing
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
