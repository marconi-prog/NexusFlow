package camunda

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marconi-prog/nexusflow/internal/config"
	"github.com/marconi-prog/nexusflow/internal/domain"
)

func newTestClient(t *testing.T, h http.HandlerFunc, mod ...func(*config.CamundaConfig)) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	cfg := config.CamundaConfig{BaseURL: srv.URL, AuthMode: config.CamundaAuthNone}
	for _, m := range mod {
		m(&cfg)
	}
	c, err := New(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestStartInstanceSendsExpectedBody(t *testing.T) {
	var got map[string]any
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v2/process-instances" {
			t.Errorf("rota inesperada %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"processInstanceKey":"2251799813685249","processDefinitionKey":"2251799813685100","processDefinitionId":"pedido","processDefinitionVersion":3,"tenantId":"<default>"}`)
	}, func(c *config.CamundaConfig) { c.TenantID = "t1" })

	inst, err := c.StartInstance(context.Background(), domain.StartInstanceCommand{
		ProcessDefinitionID: "pedido", Variables: domain.Variables{"valor": 10},
		AwaitCompletion: true, RequestTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if inst.ProcessInstanceKey != "2251799813685249" || inst.ProcessDefinitionVersion != 3 {
		t.Fatalf("mapeamento incorreto: %+v", inst)
	}
	if got["processDefinitionId"] != "pedido" || got["tenantId"] != "t1" || got["awaitCompletion"] != true || got["requestTimeout"] != float64(5000) {
		t.Fatalf("corpo inesperado: %v", got)
	}
}

func TestSearchAcceptsNumericKeysAndLegacyDates(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["filter"].(map[string]any)["state"] != "ACTIVE" {
			t.Errorf("filtro ausente: %v", body)
		}
		_, _ = io.WriteString(w, `{"items":[{"processInstanceKey":2251799813685249,"processDefinitionId":"pedido","state":"ACTIVE","startDate":"2025-01-02T10:00:00.000+0000","hasIncident":true}],"page":{"totalItems":42}}`)
	})
	res, err := c.SearchInstances(context.Background(), domain.ProcessInstanceFilter{State: domain.InstanceActive}, domain.Page{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 42 || len(res.Items) != 1 {
		t.Fatalf("resultado inesperado: %+v", res)
	}
	it := res.Items[0]
	if it.Key != "2251799813685249" || !it.HasIncident || it.StartDate == nil || it.StartDate.Year() != 2025 {
		t.Fatalf("item mal mapeado: %+v", it)
	}
}

func TestProblemDetailMapsToDomainKind(t *testing.T) {
	cases := map[int]domain.Kind{
		400: domain.KindInvalid, 404: domain.KindNotFound, 409: domain.KindConflict,
		401: domain.KindUnauthorized, 500: domain.KindInternal,
	}
	for status, kind := range cases {
		c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `{"type":"about:blank","title":"X","status":0,"detail":"detalhe do motor"}`)
		})
		err := c.CancelInstance(context.Background(), "1")
		if domain.KindOf(err) != kind {
			t.Errorf("status %d: kind=%v, esperado %v", status, domain.KindOf(err), kind)
		}
		var ee *EngineError
		if !errors.As(err, &ee) || ee.Status != status || ee.Detail != "detalhe do motor" {
			t.Errorf("status %d: EngineError não preservado: %v", status, err)
		}
	}
}

func TestIdempotentRequestsRetryOnUnavailable(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"processInstanceKey":"1","state":"COMPLETED"}`)
	})
	inst, err := c.GetInstance(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || inst.State != domain.InstanceCompleted {
		t.Fatalf("calls=%d state=%s", calls.Load(), inst.State)
	}
}

func TestCommandsAreNotRetried(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	_, err := c.StartInstance(context.Background(), domain.StartInstanceCommand{ProcessDefinitionID: "x"})
	if domain.KindOf(err) != domain.KindUnavailable || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestDeployMultipart(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatal(err)
		}
		files := r.MultipartForm.File["resources"]
		if len(files) != 1 || files[0].Filename != "pedido.bpmn" {
			t.Fatalf("arquivos: %+v", files)
		}
		_, _ = io.WriteString(w, `{"deploymentKey":"9","deployments":[{"processDefinition":{"processDefinitionId":"pedido","processDefinitionVersion":2,"resourceName":"pedido.bpmn","processDefinitionKey":"10"}}]}`)
	})
	d, err := c.Deploy(context.Background(), "", []domain.Resource{{Name: "pedido.bpmn", Content: []byte("<xml/>")}})
	if err != nil {
		t.Fatal(err)
	}
	if d.Key != "9" || len(d.Processes) != 1 || d.Processes[0].Version != 2 {
		t.Fatalf("deployment: %+v", d)
	}
}

func TestOAuthTokenIsFetchedAndCached(t *testing.T) {
	var tokenCalls atomic.Int32
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenCalls.Add(1)
		_ = r.ParseForm()
		if r.Form.Get("audience") != "zeebe.camunda.io" || r.Form.Get("grant_type") != "client_credentials" {
			t.Errorf("form inesperado: %v", r.Form)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"tok-123","token_type":"Bearer","expires_in":3600}`)
	}))
	defer tokenSrv.Close()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok-123" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, `{"clusterSize":3,"partitionsCount":3,"replicationFactor":3,"gatewayVersion":"8.8.0","brokers":[]}`)
	}, func(c *config.CamundaConfig) {
		c.AuthMode = config.CamundaAuthOAuth
		c.OAuthURL = tokenSrv.URL
		c.ClientID, c.ClientSecret, c.Audience = "id", "secret", "zeebe.camunda.io"
	})

	for range 3 {
		top, err := c.Topology(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if top.ClusterSize != 3 {
			t.Fatalf("topologia: %+v", top)
		}
	}
	if tokenCalls.Load() != 1 {
		t.Fatalf("token deveria ser cacheado; chamadas=%d", tokenCalls.Load())
	}
}

func TestActivateJobsMapsPayload(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(b), `"maxJobsToActivate":5`) || !strings.Contains(string(b), `"timeout":60000`) {
			t.Errorf("corpo: %s", b)
		}
		_, _ = io.WriteString(w, `{"jobs":[{"jobKey":"77","type":"pagar","processInstanceKey":"5","retries":3,"variables":{"a":1},"customHeaders":{"url":"x"}}]}`)
	})
	jobs, err := c.ActivateJobs(context.Background(), domain.ActivateJobsCommand{Type: "pagar", Worker: "w", Timeout: time.Minute, MaxJobs: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Key != "77" || jobs[0].Variables["a"] != float64(1) || jobs[0].CustomHeaders["url"] != "x" {
		t.Fatalf("jobs: %+v", jobs)
	}
}

func TestLatestDefinitionsOmitsFrom(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if _, has := body["page"].(map[string]any)["from"]; has {
			t.Errorf("from não deve ser enviado com isLatestVersion: %v", body)
		}
		_, _ = io.WriteString(w, `{"items":[],"page":{"totalItems":0}}`)
	})
	if _, err := c.SearchProcessDefinitions(context.Background(), domain.ProcessDefinitionFilter{Latest: true}, domain.Page{}); err != nil {
		t.Fatal(err)
	}
	_, err := c.SearchProcessDefinitions(context.Background(), domain.ProcessDefinitionFilter{Latest: true}, domain.Page{From: 10})
	if domain.KindOf(err) != domain.KindInvalid {
		t.Fatalf("from com latest deveria ser inválido: %v", err)
	}
}

func TestDefinitionXMLRequestsXML(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Accept"), "text/xml") {
			w.WriteHeader(http.StatusNotAcceptable)
			return
		}
		_, _ = io.WriteString(w, `<definitions/>`)
	})
	x, err := c.GetProcessDefinitionXML(context.Background(), "1")
	if err != nil || x != "<definitions/>" {
		t.Fatalf("%q %v", x, err)
	}
}
