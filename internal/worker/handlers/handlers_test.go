package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/worker"
)

func httpJob(headers map[string]string, vars domain.Variables) *worker.Job {
	return &worker.Job{
		Job:    domain.Job{Key: "1", ProcessInstanceKey: "2", ElementInstanceKey: "3", CustomHeaders: headers, Variables: vars},
		Logger: slog.New(slog.DiscardHandler),
	}
}

func TestHTTPHandler(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Idempotency-Key") != "3" || r.Header.Get("Authorization") != "Bearer abc" {
			t.Errorf("headers: %v", r.Header)
		}
		switch r.URL.Path {
		case "/clientes/42":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			_ = json.NewEncoder(w).Encode(map[string]any{"recebido": body["nome"]})
		case "/nao-existe":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusBadGateway)
		}
	}))
	defer srv.Close()
	host := mustHost(srv.URL)
	spec := HTTP(HTTPOptions{AllowedHosts: []string{host}})
	ctx := context.Background()

	// sucesso com interpolação de URL e corpo
	vars, err := spec.Handler(ctx, httpJob(map[string]string{
		"url": srv.URL + "/clientes/${id}", "method": "POST", "bodyVariable": "cliente",
		"resultVariable": "resp", "headers": `{"Authorization":"Bearer ${token}"}`,
	}, domain.Variables{"id": 42, "cliente": map[string]any{"nome": "Ana"}, "token": "abc"}))
	if err != nil {
		t.Fatal(err)
	}
	resp := vars["resp"].(map[string]any)
	if resp["status"] != 200 || resp["body"].(map[string]any)["recebido"] != "Ana" {
		t.Fatalf("resultado: %+v", resp)
	}

	// 4xx => erro de negócio
	_, err = spec.Handler(ctx, httpJob(map[string]string{"url": srv.URL + "/nao-existe", "headers": `{"Authorization":"Bearer abc"}`}, nil))
	var be *worker.BusinessError
	if !errors.As(err, &be) || be.Code != "HTTP_404" {
		t.Fatalf("esperava BusinessError HTTP_404, veio %v", err)
	}

	// 5xx => erro técnico (retentável)
	_, err = spec.Handler(ctx, httpJob(map[string]string{"url": srv.URL + "/falha", "headers": `{"Authorization":"Bearer abc"}`}, nil))
	if err == nil || errors.As(err, &be) {
		t.Fatalf("esperava erro técnico, veio %v", err)
	}
}

func TestHTTPHandlerBlocksHostsOutsideAllowlist(t *testing.T) {
	spec := HTTP(HTTPOptions{AllowedHosts: []string{".empresa.com"}})
	_, err := spec.Handler(context.Background(), httpJob(map[string]string{"url": "http://169.254.169.254/latest/meta-data"}, nil))
	if err == nil {
		t.Fatal("deveria bloquear host fora da allowlist")
	}
	if !hostAllowed("api.empresa.com", []string{".empresa.com"}) || hostAllowed("empresa.com.evil.io", []string{".empresa.com"}) {
		t.Fatal("regra de sufixo incorreta")
	}
}

func mustHost(raw string) string {
	u, _ := url.Parse(raw)
	return u.Hostname()
}
