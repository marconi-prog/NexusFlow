package routes

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/marconi-prog/nexusflow/internal/application/services"
	"github.com/marconi-prog/nexusflow/internal/config"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/database"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/observability"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/handlers"
	"github.com/marconi-prog/nexusflow/tests/fakes"
)

const (
	adminKey    = "admin-key-0123456789"
	operatorKey = "operator-key-0123456789"
	viewerKey   = "viewer-key-0123456789"
)

type env struct {
	srv   *httptest.Server
	fake  *fakes.Fake
	store *database.MemoryStore
}

func setup(t *testing.T) *env {
	t.Helper()
	gin.SetMode(gin.TestMode)
	log := slog.New(slog.DiscardHandler)
	fake := fakes.New()
	st := database.NewMemoryStore()
	rec := observability.NewRecorder(st, "api", log)
	cfg := &config.Config{Env: "test", Auth: config.AuthConfig{APIKeys: map[string]config.APIKey{
		adminKey: {Role: "admin", Name: "admin"}, operatorKey: {Role: "operator", Name: "erp"}, viewerKey: {Role: "viewer", Name: "dash"},
	}}}
	hub := handlers.NewHub(log)
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Run(ctx, st)
	r := New(handlers.Deps{
		Config: cfg, Logger: log, Store: st, Hub: hub,
		Processes:  services.NewProcesses(fake, st, rec, log),
		Incidents:  services.NewIncidents(fake, rec, log),
		Tasks:      services.NewTasks(fake, rec),
		Monitoring: services.NewMonitoring(fake, st, rec, log),
	})
	srv := httptest.NewServer(r)
	t.Cleanup(func() { cancel(); srv.Close() })
	return &env{srv: srv, fake: fake, store: st}
}

func (e *env) do(t *testing.T, method, path, key string, body any, headers ...string) (*http.Response, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.srv.URL+path, rdr)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp, out
}

func TestAuthAndRBAC(t *testing.T) {
	e := setup(t)
	start := map[string]any{"processDefinitionId": "pedido"}

	if r, _ := e.do(t, "GET", "/api/v1/me", "", nil); r.StatusCode != 401 {
		t.Fatalf("sem credencial: %d", r.StatusCode)
	}
	if r, _ := e.do(t, "GET", "/api/v1/me", "chave-errada-123456789", nil); r.StatusCode != 401 {
		t.Fatalf("credencial inválida: %d", r.StatusCode)
	}
	if r, body := e.do(t, "GET", "/api/v1/me", viewerKey, nil); r.StatusCode != 200 || body["id"] != "dash" {
		t.Fatalf("me: %d %v", r.StatusCode, body)
	}
	if r, _ := e.do(t, "POST", "/api/v1/process-instances", viewerKey, start); r.StatusCode != 403 {
		t.Fatalf("viewer iniciando instância: %d", r.StatusCode)
	}
	if r, _ := e.do(t, "POST", "/api/v1/process-instances", operatorKey, start); r.StatusCode != 201 {
		t.Fatalf("operator iniciando instância: %d", r.StatusCode)
	}
	if r, _ := e.do(t, "POST", "/api/v1/deployments", operatorKey, nil); r.StatusCode != 403 {
		t.Fatalf("operator fazendo deploy: %d", r.StatusCode)
	}
	if r, _ := e.do(t, "GET", "/health/live", "", nil); r.StatusCode != 200 {
		t.Fatalf("health deve ser público: %d", r.StatusCode)
	}
}

func TestStartInstanceWithBusinessKeyAndAudit(t *testing.T) {
	e := setup(t)
	r, body := e.do(t, "POST", "/api/v1/process-instances", operatorKey, map[string]any{
		"processDefinitionId": "pedido", "businessKey": "PED-1", "variables": map[string]any{"valor": 99},
	})
	if r.StatusCode != 201 {
		t.Fatalf("status %d %v", r.StatusCode, body)
	}
	key := body["processInstanceKey"].(string)
	if r.Header.Get("Location") != "/api/v1/process-instances/"+key {
		t.Fatalf("Location: %s", r.Header.Get("Location"))
	}
	cmd := e.fake.CallsTo("StartInstance")[0].Args.(domain.StartInstanceCommand)
	if cmd.Variables["businessKey"] != "PED-1" {
		t.Fatalf("businessKey não propagada como variável: %v", cmd.Variables)
	}

	_, found := e.do(t, "GET", "/api/v1/process-instances?businessKey=PED-1", viewerKey, nil)
	if found["total"] != float64(1) {
		t.Fatalf("busca por business key: %v", found)
	}

	_, detail := e.do(t, "GET", "/api/v1/process-instances/"+key, viewerKey, nil)
	tl := detail["timeline"].([]any)
	if len(tl) != 1 || tl[0].(map[string]any)["actor"] != "erp" {
		t.Fatalf("timeline/auditoria: %v", tl)
	}
}

func TestValidationErrors(t *testing.T) {
	e := setup(t)
	cases := []struct {
		method, path string
		body         any
		want         int
	}{
		{"POST", "/api/v1/process-instances", map[string]any{}, 400},
		{"POST", "/api/v1/process-instances", map[string]any{"processDefinitionId": "a", "campoDesconhecido": 1}, 400},
		{"GET", "/api/v1/process-instances/abc", nil, 400},
		{"GET", "/api/v1/process-instances?state=BOGUS", nil, 400},
		{"GET", "/api/v1/process-instances?limit=9999", nil, 400},
		{"GET", "/api/v1/process-instances/123", nil, 404},
		{"GET", "/api/v1/nada", nil, 404},
	}
	for _, c := range cases {
		r, body := e.do(t, c.method, c.path, adminKey, c.body)
		if r.StatusCode != c.want {
			t.Errorf("%s %s: status %d, esperado %d (%v)", c.method, c.path, r.StatusCode, c.want, body)
		}
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/problem+json") {
			t.Errorf("%s %s: content-type %s", c.method, c.path, r.Header.Get("Content-Type"))
		}
	}
}

func TestEngineUnavailableMapsTo503(t *testing.T) {
	e := setup(t)
	e.fake.Err = domain.NewError(domain.KindUnavailable, "camunda indisponível", nil)
	r, body := e.do(t, "POST", "/api/v1/process-instances", adminKey, map[string]any{"processDefinitionId": "x"})
	if r.StatusCode != 503 || body["detail"] != "camunda indisponível" || body["requestId"] == "" {
		t.Fatalf("%d %v", r.StatusCode, body)
	}
}

func TestIdempotencyKeyReplaysResponse(t *testing.T) {
	e := setup(t)
	req := map[string]any{"processDefinitionId": "pedido"}
	r1, b1 := e.do(t, "POST", "/api/v1/process-instances", operatorKey, req, "Idempotency-Key", "abc-1")
	r2, b2 := e.do(t, "POST", "/api/v1/process-instances", operatorKey, req, "Idempotency-Key", "abc-1")
	if r1.StatusCode != 201 || r2.StatusCode != 201 || b1["processInstanceKey"] != b2["processInstanceKey"] {
		t.Fatalf("replay: %d %v / %d %v", r1.StatusCode, b1, r2.StatusCode, b2)
	}
	if r2.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatal("header Idempotent-Replayed ausente")
	}
	if n := len(e.fake.CallsTo("StartInstance")); n != 1 {
		t.Fatalf("motor chamado %d vezes", n)
	}
	r3, _ := e.do(t, "POST", "/api/v1/process-instances", operatorKey, map[string]any{"processDefinitionId": "outro"}, "Idempotency-Key", "abc-1")
	if r3.StatusCode != 422 {
		t.Fatalf("corpo diferente com mesma chave: %d", r3.StatusCode)
	}
	// falhas liberam a chave
	e.fake.Err = domain.NewError(domain.KindUnavailable, "fora", nil)
	r4, _ := e.do(t, "POST", "/api/v1/process-instances", operatorKey, req, "Idempotency-Key", "abc-2")
	e.fake.Err = nil
	r5, _ := e.do(t, "POST", "/api/v1/process-instances", operatorKey, req, "Idempotency-Key", "abc-2")
	if r4.StatusCode != 503 || r5.StatusCode != 201 {
		t.Fatalf("após falha: %d / %d", r4.StatusCode, r5.StatusCode)
	}
}

func TestCancelInstance(t *testing.T) {
	e := setup(t)
	_, b := e.do(t, "POST", "/api/v1/process-instances", operatorKey, map[string]any{"processDefinitionId": "p"})
	key := b["processInstanceKey"].(string)
	if r, _ := e.do(t, "POST", "/api/v1/process-instances/"+key+"/cancellation", operatorKey, map[string]any{"reason": "teste"}); r.StatusCode != 204 {
		t.Fatalf("cancel: %d", r.StatusCode)
	}
	if r, _ := e.do(t, "POST", "/api/v1/process-instances/"+key+"/cancellation", operatorKey, nil); r.StatusCode != 409 {
		t.Fatalf("cancelar instância já terminada: %d", r.StatusCode)
	}
}

func TestResolveIncidentRestoresJobRetries(t *testing.T) {
	e := setup(t)
	e.fake.Incidents["500"] = &domain.Incident{Key: "500", JobKey: "600", ProcessInstanceKey: "1", ErrorType: "JOB_NO_RETRIES", State: "ACTIVE"}
	r, _ := e.do(t, "POST", "/api/v1/incidents/500/resolution", operatorKey, map[string]any{"retries": 2})
	if r.StatusCode != 200 {
		t.Fatalf("status %d", r.StatusCode)
	}
	up := e.fake.CallsTo("UpdateJobRetries")
	if len(up) != 1 || up[0].Key != "600" || up[0].Args.(int) != 2 {
		t.Fatalf("retries: %+v", up)
	}
	if len(e.fake.CallsTo("ResolveIncident")) != 1 {
		t.Fatal("incidente não resolvido")
	}
	if r, _ := e.do(t, "POST", "/api/v1/incidents/500/resolution", operatorKey, nil); r.StatusCode != 409 {
		t.Fatalf("resolver incidente já resolvido: %d", r.StatusCode)
	}
}

func TestDeployValidatesExtensions(t *testing.T) {
	e := setup(t)
	post := func(name string) int {
		buf := &bytes.Buffer{}
		mw := multipart.NewWriter(buf)
		fw, _ := mw.CreateFormFile("resources", name)
		_, _ = fw.Write([]byte("<definitions/>"))
		_ = mw.Close()
		req, _ := http.NewRequest("POST", e.srv.URL+"/api/v1/deployments", buf)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		req.Header.Set("X-API-Key", adminKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if s := post("pedido.bpmn"); s != 201 {
		t.Fatalf("bpmn: %d", s)
	}
	if s := post("script.sh"); s != 400 {
		t.Fatalf("extensão inválida: %d", s)
	}
}

func TestOverviewAndReadiness(t *testing.T) {
	e := setup(t)
	e.do(t, "POST", "/api/v1/process-instances", operatorKey, map[string]any{"processDefinitionId": "p"})
	_, o := e.do(t, "GET", "/api/v1/monitoring/overview?window=1h", viewerKey, nil)
	if o["activeInstances"] != float64(1) || o["partial"] != false {
		t.Fatalf("overview: %v", o)
	}
	e.fake.TopologyErr = domain.NewError(domain.KindUnavailable, "fora", nil)
	r, h := e.do(t, "GET", "/health/ready", "", nil)
	if r.StatusCode != 503 || h["components"].(map[string]any)["engine"].(map[string]any)["status"] != "DOWN" {
		t.Fatalf("ready: %d %v", r.StatusCode, h)
	}
	_, o = e.do(t, "GET", "/api/v1/monitoring/overview", viewerKey, nil)
	if o["partial"] != true || o["activeInstances"] != float64(1) {
		t.Fatalf("overview parcial: %v", o)
	}
}

func TestEventStream(t *testing.T) {
	e := setup(t)
	// eventos anteriores à conexão: id 1 já foi visto pelo cliente, id 2 deve ser reenviado
	e.do(t, "POST", "/api/v1/signals", operatorKey, map[string]any{"signalName": "visto"})
	e.do(t, "POST", "/api/v1/signals", operatorKey, map[string]any{"signalName": "perdido"})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", e.srv.URL+"/api/v1/events/stream?type=signal.broadcast,instance.started", nil)
	req.Header.Set("X-API-Key", viewerKey)
	req.Header.Set("Last-Event-ID", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content-type: %s", resp.Header.Get("Content-Type"))
	}

	// t.Fatal só pode ser chamado da goroutine do teste: aqui os erros vão por canal
	postErr := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		for _, p := range []struct{ path, body string }{
			{"/api/v1/messages", `{"name":"ignorado","correlationKey":"1"}`},
			{"/api/v1/process-instances", `{"processDefinitionId":"p"}`},
		} {
			req, _ := http.NewRequest("POST", e.srv.URL+p.path, strings.NewReader(p.body))
			req.Header.Set("X-API-Key", operatorKey)
			req.Header.Set("Content-Type", "application/json")
			r, err := http.DefaultClient.Do(req)
			if err != nil {
				postErr <- err
				return
			}
			r.Body.Close()
		}
		postErr <- nil
	}()

	sc := bufio.NewScanner(resp.Body)
	var got []string
	id := ""
	for len(got) < 2 && sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "id: "); ok {
			id = v
		}
		if v, ok := strings.CutPrefix(sc.Text(), "event: "); ok {
			got = append(got, id+" "+v)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("falha ao ler o stream SSE: %v", err)
	}
	if err := <-postErr; err != nil {
		t.Fatalf("falha ao gerar eventos: %v", err)
	}
	// id 3 (message.published) é filtrado; id 4 é a instância iniciada ao vivo
	if len(got) != 2 || got[0] != "2 signal.broadcast" || got[1] != "4 instance.started" {
		t.Fatalf("eventos recebidos: %v", got)
	}
}
