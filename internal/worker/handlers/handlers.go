// Package handlers contém handlers de job genéricos fornecidos pela plataforma.
// Handlers específicos de negócio devem ser registrados em cmd/workers (ou em pacotes próprios).
package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/worker"
)

const (
	TypeHTTP = "nexusflow.http"
	TypeLog  = "nexusflow.log"
)

// Log registra o job e suas variáveis; útil para depuração de modelos.
func Log() worker.Spec {
	return worker.Spec{
		Type: TypeLog,
		Handler: func(ctx context.Context, j *worker.Job) (domain.Variables, error) {
			j.Logger.Info("nexusflow.log", "variables", j.Variables, "headers", j.CustomHeaders)
			return nil, nil
		},
	}
}

// HTTPOptions controla o handler HTTP genérico.
type HTTPOptions struct {
	// AllowedHosts restringe destinos (proteção contra SSRF). "*" libera qualquer host.
	// Aceita sufixos de domínio iniciados por "." (ex.: ".interno.empresa.com").
	AllowedHosts   []string
	DefaultTimeout time.Duration
	MaxBodyBytes   int64
	Client         *http.Client
}

// HTTP chama um endpoint HTTP configurado nos custom headers do service task:
//
//	url (obrigatório, aceita placeholders ${variavel}), method (GET), timeout (30s),
//	bodyVariable (variável enviada como JSON), resultVariable ("response"),
//	headers (JSON objeto com cabeçalhos extras).
//
// 2xx => completa com {resultVariable: {status, body}}; 4xx => erro BPMN "HTTP_<status>";
// 5xx/rede => falha com retentativa.
func HTTP(opts HTTPOptions) worker.Spec {
	if opts.DefaultTimeout == 0 {
		opts.DefaultTimeout = 30 * time.Second
	}
	if opts.MaxBodyBytes == 0 {
		opts.MaxBodyBytes = 4 << 20
	}
	if opts.Client == nil {
		opts.Client = &http.Client{
			// redirects poderiam contornar a allowlist
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return worker.Spec{
		Type: TypeHTTP,
		Handler: func(ctx context.Context, j *worker.Job) (domain.Variables, error) {
			return doHTTP(ctx, j, opts)
		},
	}
}

func doHTTP(ctx context.Context, j *worker.Job, opts HTTPOptions) (domain.Variables, error) {
	h := j.CustomHeaders
	rawURL := interpolate(h["url"], j.Variables, true)
	if rawURL == "" {
		return nil, worker.Permanent(errors.New("header 'url' é obrigatório"))
	}
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, worker.Permanent(fmt.Errorf("url inválida: %q", rawURL))
	}
	if !hostAllowed(u.Hostname(), opts.AllowedHosts) {
		return nil, worker.Permanent(fmt.Errorf("host não permitido: %s (configure WORKER_HTTP_ALLOWED_HOSTS)", u.Hostname()))
	}

	method := strings.ToUpper(h["method"])
	if method == "" {
		method = http.MethodGet
	}
	timeout := opts.DefaultTimeout
	if t, err := time.ParseDuration(h["timeout"]); err == nil && t > 0 {
		timeout = t
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var body io.Reader
	if bv := h["bodyVariable"]; bv != "" {
		v, ok := j.Variables[bv]
		if !ok {
			return nil, worker.Permanent(fmt.Errorf("variável %q não encontrada", bv))
		}
		b, err := json.Marshal(v)
		if err != nil {
			return nil, worker.Permanent(err)
		}
		body = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, worker.Permanent(err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	// rastreabilidade ponta a ponta e idempotência no destino
	req.Header.Set("X-NexusFlow-Process-Instance", j.ProcessInstanceKey)
	req.Header.Set("X-NexusFlow-Job", j.Key)
	req.Header.Set("Idempotency-Key", j.ElementInstanceKey)
	if extra := h["headers"]; extra != "" {
		var hs map[string]string
		if err := json.Unmarshal([]byte(extra), &hs); err != nil {
			return nil, worker.Permanent(fmt.Errorf("header 'headers' deve ser JSON objeto: %w", err))
		}
		for k, v := range hs {
			req.Header.Set(k, interpolate(v, j.Variables, false))
		}
	}

	resp, err := opts.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("chamada HTTP falhou: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, opts.MaxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("falha ao ler resposta: %w", err)
	}

	var parsed any = string(data)
	if len(data) > 0 && json.Valid(data) {
		_ = json.Unmarshal(data, &parsed)
	}
	result := map[string]any{"status": resp.StatusCode, "body": parsed}

	switch {
	case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests:
		return nil, fmt.Errorf("HTTP %d de %s", resp.StatusCode, u.Host)
	case resp.StatusCode >= 400:
		return nil, worker.NewBusinessError("HTTP_"+strconv.Itoa(resp.StatusCode),
			fmt.Sprintf("%s %s retornou %d", method, u.Host, resp.StatusCode),
			domain.Variables{resultVar(h): result})
	case resp.StatusCode >= 300:
		return nil, worker.Permanent(fmt.Errorf("redirecionamento não suportado (HTTP %d)", resp.StatusCode))
	}
	return domain.Variables{resultVar(h): result}, nil
}

func resultVar(h map[string]string) string {
	if v := h["resultVariable"]; v != "" {
		return v
	}
	return "response"
}

func hostAllowed(host string, allowed []string) bool {
	host = strings.ToLower(host)
	if slices.Contains(allowed, "*") {
		return true
	}
	for _, a := range allowed {
		a = strings.ToLower(a)
		if host == a || (strings.HasPrefix(a, ".") && strings.HasSuffix(host, a)) {
			return true
		}
	}
	return false
}

// interpolate substitui ${nome} pelo valor da variável (escapado para URL se escape=true).
func interpolate(s string, vars domain.Variables, escape bool) string {
	if !strings.Contains(s, "${") {
		return s
	}
	var b strings.Builder
	for {
		i := strings.Index(s, "${")
		if i < 0 {
			b.WriteString(s)
			break
		}
		end := strings.Index(s[i:], "}")
		if end < 0 {
			b.WriteString(s)
			break
		}
		b.WriteString(s[:i])
		name := s[i+2 : i+end]
		if v, ok := vars[name]; ok {
			val := fmt.Sprint(v)
			if escape {
				val = url.PathEscape(val)
			}
			b.WriteString(val)
		}
		s = s[i+end+1:]
	}
	return b.String()
}
