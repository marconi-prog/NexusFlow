// Package respond centraliza a escrita de erros (RFC 7807) e o acesso ao contexto da requisição.
package respond

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/marconi-prog/nexusflow/internal/application/dto/response"
	"github.com/marconi-prog/nexusflow/internal/domain"
)

const (
	RequestIDKey = "requestId"
	LoggerKey    = "logger"
)

// RequestID devolve o id da requisição atual.
func RequestID(c *gin.Context) string { return c.GetString(RequestIDKey) }

// Logger devolve o logger contextualizado da requisição.
func Logger(c *gin.Context) *slog.Logger {
	if l, ok := c.Get(LoggerKey); ok {
		return l.(*slog.Logger)
	}
	return slog.Default()
}

func statusFor(kind domain.Kind) int {
	switch kind {
	case domain.KindInvalid:
		return http.StatusBadRequest
	case domain.KindNotFound:
		return http.StatusNotFound
	case domain.KindConflict:
		return http.StatusConflict
	case domain.KindUnauthorized:
		// credenciais da plataforma com o motor falharam: problema do servidor, não do cliente
		return http.StatusBadGateway
	case domain.KindForbidden:
		return http.StatusForbidden
	case domain.KindUnavailable:
		return http.StatusServiceUnavailable
	case domain.KindTimeout:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

func Problem(c *gin.Context, status int, detail string) {
	c.Header("Content-Type", "application/problem+json")
	c.AbortWithStatusJSON(status, response.Problem{
		Type:      "about:blank",
		Title:     http.StatusText(status),
		Status:    status,
		Detail:    detail,
		Instance:  c.Request.URL.Path,
		RequestID: c.GetString(RequestIDKey),
	})
}

// Fail converte o erro de domínio em resposta HTTP. Detalhes de erros internos não vazam.
func Fail(c *gin.Context, err error) {
	var de *domain.Error
	status := http.StatusInternalServerError
	detail := "erro interno"
	if errors.As(err, &de) {
		status = statusFor(de.Kind)
		if de.Kind != domain.KindInternal {
			detail = de.Message
		}
	}
	if status >= 500 {
		Logger(c).Error("falha na requisição", "status", status, "error", err)
	} else {
		Logger(c).Info("requisição rejeitada", "status", status, "error", err)
	}
	Problem(c, status, detail)
}

func BadRequest(c *gin.Context, detail string) { Problem(c, http.StatusBadRequest, detail) }
