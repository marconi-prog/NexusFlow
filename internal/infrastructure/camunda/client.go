// Package camunda implementa ports.Engine sobre a Orchestration Cluster REST API (v2) do Camunda 8.
package camunda

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/config"
	"github.com/marconi-prog/nexusflow/internal/domain"
)

var _ ports.Engine = (*Client)(nil)

type Client struct {
	baseURL  string
	http     *http.Client
	basic    func(*http.Request)
	tenantID string
	logger   *slog.Logger
	retries  int
}

// New cria o cliente com o modo de autenticação configurado.
func New(cfg config.CamundaConfig, logger *slog.Logger) (*Client, error) {
	if _, err := url.ParseRequestURI(cfg.BaseURL); err != nil {
		return nil, fmt.Errorf("CAMUNDA_REST_ADDRESS inválido: %w", err)
	}
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	base := &http.Client{Transport: transport}

	c := &Client{
		baseURL:  cfg.BaseURL + "/v2",
		http:     base,
		tenantID: cfg.TenantID,
		logger:   logger,
		retries:  3,
	}

	switch cfg.AuthMode {
	case config.CamundaAuthBasic:
		user, pass := cfg.Username, cfg.Password
		c.basic = func(r *http.Request) { r.SetBasicAuth(user, pass) }
	case config.CamundaAuthOAuth:
		cc := clientcredentials.Config{
			ClientID:       cfg.ClientID,
			ClientSecret:   cfg.ClientSecret,
			TokenURL:       cfg.OAuthURL,
			EndpointParams: url.Values{},
		}
		if cfg.Audience != "" {
			cc.EndpointParams.Set("audience", cfg.Audience)
		}
		if cfg.Scope != "" {
			cc.Scopes = strings.Fields(cfg.Scope)
		}
		// O token é cacheado e renovado automaticamente pelo TokenSource.
		ctx := context.WithValue(context.Background(), oauth2.HTTPClient, base)
		c.http = cc.Client(ctx)
	}
	return c, nil
}

// ---------- infraestrutura HTTP ----------

type request struct {
	method      string
	path        string
	body        any
	rawBody     io.Reader
	contentType string
	accept      string // padrão: application/json
	out         any
	// idempotent permite retentativas automáticas em falhas transitórias.
	idempotent bool
}

func (c *Client) do(ctx context.Context, r request) error {
	var payload []byte
	if r.body != nil {
		b, err := json.Marshal(r.body)
		if err != nil {
			return domain.NewError(domain.KindInternal, "falha ao serializar requisição", err)
		}
		payload = b
	}

	attempts := 1
	if r.idempotent && r.rawBody == nil {
		attempts = c.retries
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		lastErr = c.once(ctx, r, payload)
		if lastErr == nil || !retryable(lastErr) || attempt == attempts {
			return lastErr
		}
		backoff := time.Duration(attempt*attempt)*150*time.Millisecond + time.Duration(rand.IntN(100))*time.Millisecond
		c.logger.Debug("camunda: retentativa", "path", r.path, "attempt", attempt, "error", lastErr)
		select {
		case <-ctx.Done():
			return mapCtxErr(ctx.Err())
		case <-time.After(backoff):
		}
	}
	return lastErr
}

func (c *Client) once(ctx context.Context, r request, payload []byte) error {
	var body io.Reader
	switch {
	case r.rawBody != nil:
		body = r.rawBody
	case payload != nil:
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, r.method, c.baseURL+r.path, body)
	if err != nil {
		return domain.NewError(domain.KindInternal, "falha ao criar requisição", err)
	}
	accept := r.accept
	if accept == "" {
		accept = "application/json"
	}
	// application/problem+json garante que erros continuem vindo em JSON
	req.Header.Set("Accept", accept+", application/problem+json")
	switch {
	case r.contentType != "":
		req.Header.Set("Content-Type", r.contentType)
	case payload != nil:
		req.Header.Set("Content-Type", "application/json")
	}
	if c.basic != nil {
		c.basic(req)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return mapCtxErr(ctx.Err())
		}
		return domain.NewError(domain.KindUnavailable, "camunda indisponível", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return domain.NewError(domain.KindUnavailable, "falha ao ler resposta do camunda", err)
	}
	if resp.StatusCode >= 400 {
		return problemError(resp.StatusCode, data)
	}
	if r.out == nil || len(data) == 0 {
		return nil
	}
	if s, ok := r.out.(*string); ok {
		*s = string(data)
		return nil
	}
	if err := json.Unmarshal(data, r.out); err != nil {
		return domain.NewError(domain.KindInternal, "resposta inválida do camunda", err)
	}
	return nil
}

// problem segue RFC 7807, formato de erro da REST API v2.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
}

// EngineError preserva o status HTTP original do Camunda.
type EngineError struct {
	Status int
	Title  string
	Detail string
}

func (e *EngineError) Error() string {
	return fmt.Sprintf("camunda %d %s: %s", e.Status, e.Title, e.Detail)
}

func problemError(status int, body []byte) error {
	var p problem
	_ = json.Unmarshal(body, &p)
	if p.Detail == "" && p.Title == "" {
		p.Detail = strings.TrimSpace(string(body))
	}
	ee := &EngineError{Status: status, Title: p.Title, Detail: p.Detail}
	msg := p.Detail
	if msg == "" {
		msg = p.Title
	}
	if msg == "" {
		msg = http.StatusText(status)
	}
	var kind domain.Kind
	switch status {
	case http.StatusBadRequest:
		kind = domain.KindInvalid
	case http.StatusUnauthorized:
		kind = domain.KindUnauthorized
	case http.StatusForbidden:
		kind = domain.KindForbidden
	case http.StatusNotFound:
		kind = domain.KindNotFound
	case http.StatusConflict:
		kind = domain.KindConflict
	case http.StatusGatewayTimeout, http.StatusRequestTimeout:
		kind = domain.KindTimeout
	case http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusBadGateway:
		kind = domain.KindUnavailable
	default:
		kind = domain.KindInternal
	}
	return domain.NewError(kind, msg, ee)
}

func retryable(err error) bool {
	switch domain.KindOf(err) {
	case domain.KindUnavailable:
		return true
	case domain.KindTimeout:
		var ee *EngineError
		return errors.As(err, &ee) // timeout do servidor, não do contexto local
	}
	return false
}

func mapCtxErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return domain.NewError(domain.KindTimeout, "tempo esgotado aguardando o camunda", err)
	}
	return domain.NewError(domain.KindUnavailable, "requisição cancelada", err)
}

func (c *Client) tenant(t string) string {
	if t != "" {
		return t
	}
	return c.tenantID
}

func ms(d time.Duration) int64 { return d.Milliseconds() }

func p(key string) string { return url.PathEscape(key) }

// ---------- Topologia ----------

func (c *Client) Topology(ctx context.Context) (*domain.Topology, error) {
	var out domain.Topology
	if err := c.do(ctx, request{method: http.MethodGet, path: "/topology", out: &out, idempotent: true}); err != nil {
		return nil, err
	}
	return &out, nil
}

// ---------- Deployments ----------

func (c *Client) Deploy(ctx context.Context, tenantID string, resources []domain.Resource) (*domain.Deployment, error) {
	if len(resources) == 0 {
		return nil, domain.Invalid("ao menos um recurso é obrigatório")
	}
	buf := &bytes.Buffer{}
	mw := multipart.NewWriter(buf)
	for _, r := range resources {
		fw, err := mw.CreateFormFile("resources", r.Name)
		if err != nil {
			return nil, domain.NewError(domain.KindInternal, "falha ao montar multipart", err)
		}
		if _, err := fw.Write(r.Content); err != nil {
			return nil, domain.NewError(domain.KindInternal, "falha ao montar multipart", err)
		}
	}
	if t := c.tenant(tenantID); t != "" {
		_ = mw.WriteField("tenantId", t)
	}
	if err := mw.Close(); err != nil {
		return nil, domain.NewError(domain.KindInternal, "falha ao montar multipart", err)
	}

	var out deployResponse
	err := c.do(ctx, request{
		method: http.MethodPost, path: "/deployments",
		rawBody: buf, contentType: mw.FormDataContentType(), out: &out,
	})
	if err != nil {
		return nil, err
	}
	return out.toDomain(), nil
}

// ---------- Definições ----------

func (c *Client) SearchProcessDefinitions(ctx context.Context, f domain.ProcessDefinitionFilter, pg domain.Page) (*domain.Result[domain.ProcessDefinition], error) {
	filter := map[string]any{}
	setIf(filter, "processDefinitionId", f.ID)
	setIf(filter, "name", f.Name)
	setIf(filter, "tenantId", f.TenantID)
	if f.Version > 0 {
		filter["version"] = f.Version
	}
	if f.Latest {
		if pg.From > 0 {
			return nil, domain.Invalid("latest=true não suporta paginação com from")
		}
		filter["isLatestVersion"] = true
	}
	var out searchResponse[processDefinitionDTO]
	if err := c.search(ctx, "/process-definitions/search", filter, pg, nil, &out); err != nil {
		return nil, err
	}
	return mapResult(out, processDefinitionDTO.toDomain), nil
}

func (c *Client) GetProcessDefinition(ctx context.Context, key string) (*domain.ProcessDefinition, error) {
	var out processDefinitionDTO
	if err := c.do(ctx, request{method: http.MethodGet, path: "/process-definitions/" + p(key), out: &out, idempotent: true}); err != nil {
		return nil, err
	}
	d := out.toDomain()
	return &d, nil
}

func (c *Client) GetProcessDefinitionXML(ctx context.Context, key string) (string, error) {
	var out string
	err := c.do(ctx, request{method: http.MethodGet, path: "/process-definitions/" + p(key) + "/xml", accept: "text/xml", out: &out, idempotent: true})
	return out, err
}

// ---------- Instâncias ----------

func (c *Client) StartInstance(ctx context.Context, cmd domain.StartInstanceCommand) (*domain.StartedInstance, error) {
	body := map[string]any{}
	setIf(body, "processDefinitionId", cmd.ProcessDefinitionID)
	setIf(body, "processDefinitionKey", cmd.ProcessDefinitionKey)
	setIf(body, "tenantId", c.tenant(cmd.TenantID))
	if cmd.ProcessDefinitionVersion > 0 {
		body["processDefinitionVersion"] = cmd.ProcessDefinitionVersion
	}
	if cmd.Variables != nil {
		body["variables"] = cmd.Variables
	}
	if cmd.AwaitCompletion {
		body["awaitCompletion"] = true
		if cmd.RequestTimeout > 0 {
			body["requestTimeout"] = ms(cmd.RequestTimeout)
		}
		if len(cmd.FetchVariables) > 0 {
			body["fetchVariables"] = cmd.FetchVariables
		}
	}
	var out startedInstanceDTO
	if err := c.do(ctx, request{method: http.MethodPost, path: "/process-instances", body: body, out: &out}); err != nil {
		return nil, err
	}
	return out.toDomain(), nil
}

func (c *Client) CancelInstance(ctx context.Context, key string) error {
	return c.do(ctx, request{method: http.MethodPost, path: "/process-instances/" + p(key) + "/cancellation", body: map[string]any{}})
}

func (c *Client) GetInstance(ctx context.Context, key string) (*domain.ProcessInstance, error) {
	var out processInstanceDTO
	if err := c.do(ctx, request{method: http.MethodGet, path: "/process-instances/" + p(key), out: &out, idempotent: true}); err != nil {
		return nil, err
	}
	pi := out.toDomain()
	return &pi, nil
}

func (c *Client) SearchInstances(ctx context.Context, f domain.ProcessInstanceFilter, pg domain.Page, sort []domain.Sort) (*domain.Result[domain.ProcessInstance], error) {
	filter := map[string]any{}
	setIf(filter, "processDefinitionId", f.ProcessDefinitionID)
	setIf(filter, "processDefinitionKey", f.ProcessDefinitionKey)
	setIf(filter, "state", string(f.State))
	setIf(filter, "parentProcessInstanceKey", f.ParentInstanceKey)
	setIf(filter, "tenantId", f.TenantID)
	if f.HasIncident != nil {
		filter["hasIncident"] = *f.HasIncident
	}
	var out searchResponse[processInstanceDTO]
	if err := c.search(ctx, "/process-instances/search", filter, pg, sort, &out); err != nil {
		return nil, err
	}
	return mapResult(out, processInstanceDTO.toDomain), nil
}

func (c *Client) SearchVariables(ctx context.Context, processInstanceKey string, pg domain.Page) (*domain.Result[domain.Variable], error) {
	filter := map[string]any{"processInstanceKey": processInstanceKey}
	var out searchResponse[variableDTO]
	if err := c.search(ctx, "/variables/search", filter, pg, nil, &out); err != nil {
		return nil, err
	}
	return mapResult(out, variableDTO.toDomain), nil
}

func (c *Client) SetVariables(ctx context.Context, elementInstanceKey string, vars domain.Variables, local bool) error {
	body := map[string]any{"variables": vars, "local": local}
	return c.do(ctx, request{method: http.MethodPut, path: "/element-instances/" + p(elementInstanceKey) + "/variables", body: body})
}

// ---------- Mensagens / sinais ----------

func (c *Client) PublishMessage(ctx context.Context, cmd domain.PublishMessageCommand) (*domain.MessageResult, error) {
	body := map[string]any{"name": cmd.Name, "correlationKey": cmd.CorrelationKey}
	setIf(body, "tenantId", c.tenant(cmd.TenantID))
	if cmd.Variables != nil {
		body["variables"] = cmd.Variables
	}
	path := "/messages/correlation"
	if !cmd.Correlate {
		path = "/messages/publication"
		setIf(body, "messageId", cmd.MessageID)
		if cmd.TimeToLive > 0 {
			body["timeToLive"] = ms(cmd.TimeToLive)
		}
	}
	var out messageDTO
	if err := c.do(ctx, request{method: http.MethodPost, path: path, body: body, out: &out}); err != nil {
		return nil, err
	}
	return &domain.MessageResult{MessageKey: string(out.MessageKey), ProcessInstanceKey: string(out.ProcessInstanceKey), TenantID: out.TenantID}, nil
}

func (c *Client) BroadcastSignal(ctx context.Context, cmd domain.BroadcastSignalCommand) (*domain.SignalResult, error) {
	body := map[string]any{"signalName": cmd.Name}
	setIf(body, "tenantId", c.tenant(cmd.TenantID))
	if cmd.Variables != nil {
		body["variables"] = cmd.Variables
	}
	var out struct {
		SignalKey flexKey `json:"signalKey"`
		TenantID  string  `json:"tenantId"`
	}
	if err := c.do(ctx, request{method: http.MethodPost, path: "/signals/broadcast", body: body, out: &out}); err != nil {
		return nil, err
	}
	return &domain.SignalResult{SignalKey: string(out.SignalKey), TenantID: out.TenantID}, nil
}

// ---------- Incidentes ----------

func (c *Client) SearchIncidents(ctx context.Context, f domain.IncidentFilter, pg domain.Page) (*domain.Result[domain.Incident], error) {
	filter := map[string]any{}
	setIf(filter, "processInstanceKey", f.ProcessInstanceKey)
	setIf(filter, "processDefinitionId", f.ProcessDefinitionID)
	setIf(filter, "state", f.State)
	setIf(filter, "errorType", f.ErrorType)
	setIf(filter, "tenantId", f.TenantID)
	var out searchResponse[incidentDTO]
	sort := []domain.Sort{{Field: "creationTime", Order: "DESC"}}
	if err := c.search(ctx, "/incidents/search", filter, pg, sort, &out); err != nil {
		return nil, err
	}
	return mapResult(out, incidentDTO.toDomain), nil
}

func (c *Client) GetIncident(ctx context.Context, key string) (*domain.Incident, error) {
	var out incidentDTO
	if err := c.do(ctx, request{method: http.MethodGet, path: "/incidents/" + p(key), out: &out, idempotent: true}); err != nil {
		return nil, err
	}
	i := out.toDomain()
	return &i, nil
}

func (c *Client) ResolveIncident(ctx context.Context, key string) error {
	return c.do(ctx, request{method: http.MethodPost, path: "/incidents/" + p(key) + "/resolution", body: map[string]any{}})
}

func (c *Client) UpdateJobRetries(ctx context.Context, jobKey string, retries int) error {
	body := map[string]any{"changeset": map[string]any{"retries": retries}}
	return c.do(ctx, request{method: http.MethodPatch, path: "/jobs/" + p(jobKey), body: body})
}

// ---------- Jobs ----------

func (c *Client) ActivateJobs(ctx context.Context, cmd domain.ActivateJobsCommand) ([]domain.Job, error) {
	body := map[string]any{
		"type":              cmd.Type,
		"worker":            cmd.Worker,
		"timeout":           ms(cmd.Timeout),
		"maxJobsToActivate": cmd.MaxJobs,
	}
	if len(cmd.FetchVariables) > 0 {
		body["fetchVariable"] = cmd.FetchVariables
	}
	if cmd.RequestTimeout > 0 {
		body["requestTimeout"] = ms(cmd.RequestTimeout)
	}
	tenants := cmd.TenantIDs
	if len(tenants) == 0 && c.tenantID != "" {
		tenants = []string{c.tenantID}
	}
	if len(tenants) > 0 {
		body["tenantIds"] = tenants
	}
	var out struct {
		Jobs []jobDTO `json:"jobs"`
	}
	if err := c.do(ctx, request{method: http.MethodPost, path: "/jobs/activation", body: body, out: &out}); err != nil {
		return nil, err
	}
	jobs := make([]domain.Job, 0, len(out.Jobs))
	for _, j := range out.Jobs {
		jobs = append(jobs, j.toDomain())
	}
	return jobs, nil
}

func (c *Client) CompleteJob(ctx context.Context, jobKey string, vars domain.Variables) error {
	body := map[string]any{}
	if vars != nil {
		body["variables"] = vars
	}
	return c.do(ctx, request{method: http.MethodPost, path: "/jobs/" + p(jobKey) + "/completion", body: body, idempotent: true})
}

func (c *Client) FailJob(ctx context.Context, jobKey string, cmd domain.FailJobCommand) error {
	body := map[string]any{"retries": cmd.Retries, "errorMessage": truncate(cmd.ErrorMessage, 4000)}
	if cmd.RetryBackOff > 0 {
		body["retryBackOff"] = ms(cmd.RetryBackOff)
	}
	if cmd.Variables != nil {
		body["variables"] = cmd.Variables
	}
	return c.do(ctx, request{method: http.MethodPost, path: "/jobs/" + p(jobKey) + "/failure", body: body, idempotent: true})
}

func (c *Client) ThrowError(ctx context.Context, jobKey string, cmd domain.ThrowErrorCommand) error {
	body := map[string]any{"errorCode": cmd.ErrorCode}
	setIf(body, "errorMessage", truncate(cmd.ErrorMessage, 4000))
	if cmd.Variables != nil {
		body["variables"] = cmd.Variables
	}
	return c.do(ctx, request{method: http.MethodPost, path: "/jobs/" + p(jobKey) + "/error", body: body, idempotent: true})
}

// ---------- User tasks ----------

func (c *Client) SearchUserTasks(ctx context.Context, f domain.UserTaskFilter, pg domain.Page) (*domain.Result[domain.UserTask], error) {
	filter := map[string]any{}
	setIf(filter, "state", f.State)
	setIf(filter, "assignee", f.Assignee)
	setIf(filter, "candidateGroup", f.CandidateGroup)
	setIf(filter, "candidateUser", f.CandidateUser)
	setIf(filter, "processInstanceKey", f.ProcessInstanceKey)
	setIf(filter, "elementId", f.ElementID)
	setIf(filter, "tenantId", f.TenantID)
	var out searchResponse[userTaskDTO]
	sort := []domain.Sort{{Field: "creationDate", Order: "DESC"}}
	if err := c.search(ctx, "/user-tasks/search", filter, pg, sort, &out); err != nil {
		return nil, err
	}
	return mapResult(out, userTaskDTO.toDomain), nil
}

func (c *Client) GetUserTask(ctx context.Context, key string) (*domain.UserTask, error) {
	var out userTaskDTO
	if err := c.do(ctx, request{method: http.MethodGet, path: "/user-tasks/" + p(key), out: &out, idempotent: true}); err != nil {
		return nil, err
	}
	t := out.toDomain()
	return &t, nil
}

func (c *Client) AssignUserTask(ctx context.Context, key, assignee string, allowOverride bool) error {
	body := map[string]any{"assignee": assignee, "allowOverride": allowOverride}
	return c.do(ctx, request{method: http.MethodPost, path: "/user-tasks/" + p(key) + "/assignment", body: body})
}

func (c *Client) UnassignUserTask(ctx context.Context, key string) error {
	return c.do(ctx, request{method: http.MethodDelete, path: "/user-tasks/" + p(key) + "/assignee"})
}

func (c *Client) CompleteUserTask(ctx context.Context, key string, vars domain.Variables, action string) error {
	body := map[string]any{}
	if vars != nil {
		body["variables"] = vars
	}
	setIf(body, "action", action)
	return c.do(ctx, request{method: http.MethodPost, path: "/user-tasks/" + p(key) + "/completion", body: body})
}

// ---------- helpers ----------

func (c *Client) search(ctx context.Context, path string, filter map[string]any, pg domain.Page, sort []domain.Sort, out any) error {
	pg = pg.Normalize()
	page := map[string]any{"limit": pg.Limit}
	if pg.From > 0 { // alguns filtros (ex.: isLatestVersion) não aceitam "from"
		page["from"] = pg.From
	}
	body := map[string]any{"page": page}
	if len(filter) > 0 {
		body["filter"] = filter
	}
	if len(sort) > 0 {
		s := make([]map[string]string, 0, len(sort))
		for _, it := range sort {
			order := strings.ToUpper(it.Order)
			if order != "ASC" {
				order = "DESC"
			}
			s = append(s, map[string]string{"field": it.Field, "order": order})
		}
		body["sort"] = s
	}
	return c.do(ctx, request{method: http.MethodPost, path: path, body: body, out: out, idempotent: true})
}

func setIf(m map[string]any, k, v string) {
	if v != "" {
		m[k] = v
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
