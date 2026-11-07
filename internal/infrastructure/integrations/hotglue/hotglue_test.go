package hotglue

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/marconi-prog/nexusflow/internal/domain"
)

func sign(secret, body []byte) string {
	m := hmac.New(sha1.New, secret)
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

func TestRunJobAndStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "k" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/env.hotglue.xyz/flow1/tenant-a/jobs":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["job_name"] != "j1" {
				t.Errorf("corpo: %v", body)
			}
			_, _ = io.WriteString(w, `{"job_id":"37g_DK","job_name":"j1","tenant":"tenant-a","status":"JOB_CREATED"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/env.hotglue.xyz/flow1/tenant-a/jobs/status" && r.URL.Query().Get("job_id") == "37g_DK":
			_, _ = io.WriteString(w, `{"job_id":"37g_DK","flow":"flow1","status":"JOB_COMPLETED"}`)
		case r.URL.Path == "/env.hotglue.xyz/flow1/ocupado/jobs":
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"message":"Job has been queued","reason":"There is a job currently running"}`)
		default:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{}`)
		}
	}))
	defer srv.Close()
	c, err := NewClient(Config{BaseURL: srv.URL, APIKey: "k", EnvID: "env.hotglue.xyz", FlowID: "flow1"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	job, err := c.RunJob(ctx, "tenant-a", RunJobRequest{JobName: "j1"})
	if err != nil || job.JobID != "37g_DK" || job.Status != StatusJobCreated {
		t.Fatalf("%+v %v", job, err)
	}
	st, err := c.JobStatus(ctx, "tenant-a", "37g_DK")
	if err != nil || !Succeeded(st.Status) {
		t.Fatalf("%+v %v", st, err)
	}
	var q *ErrQueued
	if _, err := c.RunJob(ctx, "ocupado", RunJobRequest{}); !errors.As(err, &q) {
		t.Fatalf("esperava ErrQueued: %v", err)
	}
	var api *APIError
	if _, err := c.RunJob(ctx, "inexistente/x", RunJobRequest{}); !errors.As(err, &api) || api.Retryable() {
		t.Fatalf("esperava APIError 400 não retentável: %v", err)
	}
}

type fakePublisher struct {
	got []domain.PublishMessageCommand
	err error
}

func (f *fakePublisher) PublishMessage(_ context.Context, cmd domain.PublishMessageCommand) (*domain.MessageResult, error) {
	f.got = append(f.got, cmd)
	if f.err != nil {
		return nil, f.err
	}
	return &domain.MessageResult{MessageKey: "1"}, nil
}

func TestWebhook(t *testing.T) {
	gin.SetMode(gin.TestMode)
	secret := []byte("segredo")
	pub := &fakePublisher{}
	r := gin.New()
	r.POST("/webhooks/hotglue", NewWebhookHandler(string(secret), pub, slog.New(slog.DiscardHandler)).Handle)

	post := func(body []byte, sig string) int {
		req := httptest.NewRequest(http.MethodPost, "/webhooks/hotglue", bytes.NewReader(body))
		if sig != "" {
			req.Header.Set("X-Hub-Signature", sig)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w.Code
	}
	done := []byte(`{"job_id":"37g_DK","tenant":"t","status":"JOB_COMPLETED","metrics":{"recordCount":42}}`)
	if c := post(done, ""); c != http.StatusUnauthorized {
		t.Fatalf("sem assinatura: %d", c)
	}
	if c := post(done, sign([]byte("outro"), done)); c != http.StatusUnauthorized {
		t.Fatalf("assinatura errada: %d", c)
	}
	running := []byte(`{"job_id":"37g_DK","status":"SYNC_STARTED"}`)
	if c := post(running, sign(secret, running)); c != http.StatusAccepted || len(pub.got) != 0 {
		t.Fatalf("status intermediário não deve publicar: %d %v", c, pub.got)
	}
	if c := post(done, sign(secret, done)); c != http.StatusOK {
		t.Fatalf("webhook válido: %d", c)
	}
	m := pub.got[0]
	if m.Name != MessageJobFinished || m.CorrelationKey != "37g_DK" || m.Variables["hotglueSucesso"] != true || m.Variables["hotglueRegistros"] != int64(42) {
		t.Fatalf("mensagem: %+v", m)
	}
	pub.err = domain.NewError(domain.KindUnavailable, "fora", nil)
	if c := post(done, sign(secret, done)); c != http.StatusBadGateway {
		t.Fatalf("motor fora deve pedir reenvio (5xx): %d", c)
	}
	pub.err = domain.Conflict("já publicada")
	if c := post(done, sign(secret, done)); c != http.StatusOK {
		t.Fatalf("duplicada deve ser sucesso: %d", c)
	}
}

func TestWebhookDisabledWithoutSecret(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/w", NewWebhookHandler("", &fakePublisher{}, slog.New(slog.DiscardHandler)).Handle)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/w", bytes.NewReader([]byte(`{}`))))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("%d", w.Code)
	}
}
