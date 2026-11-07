// Package middleware contém os middlewares HTTP: request id, log, métricas, CORS, auth, RBAC e idempotência.
package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/auth"
	"github.com/marconi-prog/nexusflow/internal/config"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/observability"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/respond"
)

const headerReqID = "X-Request-ID"

// RequestContext atribui/propaga X-Request-ID, registra log de acesso e métricas.
func RequestContext(base *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		rid := c.GetHeader(headerReqID)
		if rid == "" || len(rid) > 128 {
			rid = uuid.NewString()
		}
		c.Set(respond.RequestIDKey, rid)
		c.Header(headerReqID, rid)
		log := base.With("requestId", rid)
		c.Set(respond.LoggerKey, log)

		start := time.Now()
		c.Next()

		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		status := c.Writer.Status()
		elapsed := time.Since(start)
		observability.HTTPRequests.WithLabelValues(c.Request.Method, route, strconv.Itoa(status)).Inc()
		observability.HTTPDuration.WithLabelValues(c.Request.Method, route).Observe(elapsed.Seconds())

		if strings.HasPrefix(route, "/health") || route == "/metrics" {
			return
		}
		log.Info("http",
			"method", c.Request.Method, "route", route, "status", status,
			"durationMs", elapsed.Milliseconds(), "actor", auth.ActorID(c.Request.Context()),
			"clientIp", c.ClientIP())
	}
}

func Recovery() gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, rec any) {
		respond.Logger(c).Error("panic", "panic", rec)
		respond.Problem(c, http.StatusInternalServerError, "erro interno")
	})
}

func CORS(origins []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin != "" && (slices.Contains(origins, "*") || slices.Contains(origins, origin)) {
			h := c.Writer.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Set("Vary", "Origin")
			h.Set("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Authorization,Content-Type,X-API-Key,X-Request-ID,Idempotency-Key,Last-Event-ID")
			h.Set("Access-Control-Expose-Headers", "X-Request-ID,Idempotent-Replayed")
			h.Set("Access-Control-Max-Age", "600")
		}
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

type keyEntry struct {
	hash [32]byte
	p    auth.Principal
}

// Authenticate valida a API key (Authorization: Bearer <key> ou X-API-Key). Sem chaves
// configuradas, todas as requisições recebem um principal admin "dev" (modo desenvolvimento).
func Authenticate(keys map[string]config.APIKey) gin.HandlerFunc {
	entries := make([]keyEntry, 0, len(keys))
	for k, info := range keys {
		entries = append(entries, keyEntry{hash: sha256.Sum256([]byte(k)), p: auth.Principal{ID: info.Name, Role: auth.Role(info.Role)}})
	}
	return func(c *gin.Context) {
		if len(entries) == 0 {
			c.Request = c.Request.WithContext(auth.WithPrincipal(c.Request.Context(), auth.Principal{ID: "dev", Role: auth.RoleAdmin}))
			c.Next()
			return
		}
		key := c.GetHeader("X-API-Key")
		if key == "" {
			if h := c.GetHeader("Authorization"); len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
				key = strings.TrimSpace(h[7:])
			}
		}
		if key == "" {
			c.Header("WWW-Authenticate", `Bearer realm="nexusflow"`)
			respond.Problem(c, http.StatusUnauthorized, "credencial ausente")
			return
		}
		h := sha256.Sum256([]byte(key))
		var found *auth.Principal
		for i := range entries { // compara todas para tempo constante
			if subtle.ConstantTimeCompare(h[:], entries[i].hash[:]) == 1 {
				found = &entries[i].p
			}
		}
		if found == nil {
			c.Header("WWW-Authenticate", `Bearer realm="nexusflow", error="invalid_token"`)
			respond.Problem(c, http.StatusUnauthorized, "credencial inválida")
			return
		}
		c.Request = c.Request.WithContext(auth.WithPrincipal(c.Request.Context(), *found))
		c.Next()
	}
}

func Require(min auth.Role) gin.HandlerFunc {
	return func(c *gin.Context) {
		p, ok := auth.PrincipalFrom(c.Request.Context())
		if !ok || !p.Role.Allows(min) {
			respond.Problem(c, http.StatusForbidden, "papel insuficiente: requer "+string(min))
			return
		}
		c.Next()
	}
}

// ---------- idempotência ----------

type captureWriter struct {
	gin.ResponseWriter
	buf bytes.Buffer
}

func (w *captureWriter) Write(b []byte) (int, error) {
	w.buf.Write(b)
	return w.ResponseWriter.Write(b)
}

const maxIdempotentBody = 1 << 20

// Idempotent torna a rota segura para retentativas do cliente via header Idempotency-Key:
// a primeira resposta 2xx é armazenada e reproduzida em chamadas repetidas com o mesmo corpo.
func Idempotent(s ports.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.GetHeader("Idempotency-Key")
		if key == "" {
			c.Next()
			return
		}
		if len(key) > 255 {
			respond.BadRequest(c, "Idempotency-Key excede 255 caracteres")
			return
		}
		body, err := io.ReadAll(io.LimitReader(c.Request.Body, maxIdempotentBody+1))
		if err != nil {
			respond.BadRequest(c, "falha ao ler corpo")
			return
		}
		if len(body) > maxIdempotentBody {
			respond.Problem(c, http.StatusRequestEntityTooLarge, "corpo excede 1 MiB")
			return
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(body))

		sum := sha256.Sum256(append([]byte(c.Request.Method+" "+c.FullPath()+"\n"), body...))
		scope := auth.ActorID(c.Request.Context()) + " " + c.FullPath()
		rec := ports.IdempotencyRecord{Scope: scope, Key: key, RequestHash: hex.EncodeToString(sum[:])}

		existing, err := s.ReserveIdempotency(c.Request.Context(), rec)
		switch {
		case errors.Is(err, ports.ErrHashMismatch):
			respond.Problem(c, http.StatusUnprocessableEntity, "Idempotency-Key já usada com outro corpo de requisição")
			return
		case errors.Is(err, ports.ErrDuplicate):
			if existing.StatusCode == 0 {
				respond.Problem(c, http.StatusConflict, "requisição com esta Idempotency-Key ainda em processamento")
				return
			}
			c.Header("Idempotent-Replayed", "true")
			c.Data(existing.StatusCode, "application/json; charset=utf-8", existing.Response)
			c.Abort()
			return
		case err != nil:
			respond.Fail(c, err)
			return
		}

		cw := &captureWriter{ResponseWriter: c.Writer}
		c.Writer = cw
		c.Next()

		// grava mesmo se o cliente desconectou: a operação já foi executada
		ctx := context.WithoutCancel(c.Request.Context())
		if st := cw.Status(); st >= 200 && st < 300 {
			if err := s.CompleteIdempotency(ctx, scope, key, st, cw.buf.Bytes()); err != nil {
				respond.Logger(c).Error("falha ao gravar resposta idempotente", "error", err)
			}
			return
		}
		// falhou: libera a chave para permitir nova tentativa
		if err := s.ReleaseIdempotency(ctx, scope, key); err != nil {
			respond.Logger(c).Error("falha ao liberar chave idempotente", "error", err)
		}
	}
}
