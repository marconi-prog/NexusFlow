package hotglue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Status de um job no ciclo de vida do Hotglue.
const (
	StatusJobCreated     = "JOB_CREATED"
	StatusJobCompleted   = "JOB_COMPLETED"
	StatusDiscoverFailed = "DISCOVER_FAILED"
	StatusSyncFailed     = "SYNC_FAILED"
	StatusETLFailed      = "ETL_FAILED"
	StatusExportFailed   = "EXPORT_FAILED"
)

// Terminal indica se o status encerra o job (sucesso ou falha).
func Terminal(status string) bool {
	switch status {
	case StatusJobCompleted, StatusDiscoverFailed, StatusSyncFailed, StatusETLFailed, StatusExportFailed:
		return true
	}
	return false
}

// Succeeded indica se o job terminou com sucesso.
func Succeeded(status string) bool { return status == StatusJobCompleted }

type Client struct {
	cfg  Config
	http *http.Client
}

func NewClient(cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Client{cfg: cfg, http: &http.Client{Timeout: cfg.Timeout}}, nil
}

// RunJobRequest é o corpo de POST /{env_id}/{flow_id}/{tenant}/jobs (campos opcionais).
type RunJobRequest struct {
	JobName           string         `json:"job_name,omitempty"`
	ExternalJobID     string         `json:"external_job_id,omitempty"`
	State             map[string]any `json:"state,omitempty"`
	OverrideStartDate *time.Time     `json:"override_start_date,omitempty"`
	OverrideEndDate   *time.Time     `json:"override_end_date,omitempty"`
	QueueIfActiveJob  bool           `json:"queue_if_active_job,omitempty"`
}

// Job é a representação de um job devolvida pela API.
type Job struct {
	JobID         string `json:"job_id"`
	EnvID         string `json:"env_id"`
	FlowID        string `json:"flow_id"`
	Flow          string `json:"flow"` // /jobs/status usa "flow" em vez de "flow_id"
	JobName       string `json:"job_name"`
	ExternalJobID string `json:"external_job_id"`
	Tenant        string `json:"tenant"`
	Status        string `json:"status"`
	Message       string `json:"message"`
	StartTime     string `json:"start_time"`
	LastUpdated   string `json:"last_updated"`
	ProcessedRows int64  `json:"processed_rows"`
}

// ErrQueued indica que o Hotglue enfileirou o job (HTTP 202) porque outro está rodando.
type ErrQueued struct{ Reason string }

func (e *ErrQueued) Error() string { return "hotglue: job enfileirado: " + e.Reason }

// APIError é uma resposta de erro da API do Hotglue.
type APIError struct {
	Status int
	Body   string
}

func (e *APIError) Error() string { return fmt.Sprintf("hotglue: HTTP %d: %s", e.Status, e.Body) }

// Retryable indica se vale tentar de novo (limite de taxa ou falha do servidor).
func (e *APIError) Retryable() bool { return e.Status == http.StatusTooManyRequests || e.Status >= 500 }

// RunJob dispara um job de sincronização para o tenant.
func (c *Client) RunJob(ctx context.Context, tenant string, req RunJobRequest) (*Job, error) {
	var job Job
	status, body, err := c.do(ctx, http.MethodPost, c.flowPath(tenant)+"/jobs", nil, req)
	if err != nil {
		return nil, err
	}
	if status == http.StatusAccepted {
		var q struct {
			Reason string `json:"reason"`
		}
		_ = json.Unmarshal(body, &q)
		return nil, &ErrQueued{Reason: q.Reason}
	}
	if err := json.Unmarshal(body, &job); err != nil {
		return nil, fmt.Errorf("hotglue: resposta inválida: %w", err)
	}
	return &job, nil
}

// JobStatus consulta o status de um job (GET /{env_id}/{flow_id}/{tenant}/jobs/status).
func (c *Client) JobStatus(ctx context.Context, tenant, jobID string) (*Job, error) {
	var job Job
	_, body, err := c.do(ctx, http.MethodGet, c.flowPath(tenant)+"/jobs/status", url.Values{"job_id": {jobID}}, nil)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(body, &job); err != nil {
		return nil, fmt.Errorf("hotglue: resposta inválida: %w", err)
	}
	return &job, nil
}

func (c *Client) flowPath(tenant string) string {
	return "/" + url.PathEscape(c.cfg.EnvID) + "/" + url.PathEscape(c.cfg.FlowID) + "/" + url.PathEscape(tenant)
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, in any) (int, []byte, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(b)
	}
	u := c.cfg.BaseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("x-api-key", c.cfg.APIKey)
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("hotglue: falha de rede: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return 0, nil, err
	}
	if resp.StatusCode >= 400 {
		return resp.StatusCode, nil, &APIError{Status: resp.StatusCode, Body: truncate(string(data), 500)}
	}
	return resp.StatusCode, data, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
