package hotglue

import (
	"context"
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec // algoritmo definido pelo Hotglue para X-Hub-Signature
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/marconi-prog/nexusflow/internal/domain"
)

// MessagePublisher é o que o webhook precisa do serviço de processos.
type MessagePublisher interface {
	PublishMessage(ctx context.Context, cmd domain.PublishMessageCommand) (*domain.MessageResult, error)
}

const maxWebhookBody = 1 << 20

// WebhookHandler recebe o webhook de ciclo de vida de job do Hotglue. Não usa API key
// (o Hotglue não a conhece): a autenticidade vem da assinatura HMAC no X-Hub-Signature.
type WebhookHandler struct {
	secret    []byte
	publisher MessagePublisher
	logger    *slog.Logger
}

func NewWebhookHandler(secret string, p MessagePublisher, logger *slog.Logger) *WebhookHandler {
	return &WebhookHandler{secret: []byte(secret), publisher: p, logger: logger}
}

// VerifySignature confere o HMAC-SHA1 (hex) do corpo cru, em tempo constante.
func VerifySignature(secret, body []byte, signature string) bool {
	signature = strings.TrimPrefix(strings.TrimSpace(signature), "sha1=")
	got, err := hex.DecodeString(signature)
	if err != nil || len(secret) == 0 {
		return false
	}
	mac := hmac.New(sha1.New, secret)
	mac.Write(body)
	return hmac.Equal(mac.Sum(nil), got)
}

func (h *WebhookHandler) Handle(c *gin.Context) {
	if len(h.secret) == 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "webhook do hotglue não configurado (HOTGLUE_WEBHOOK_SECRET)"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, maxWebhookBody+1))
	if err != nil || len(body) > maxWebhookBody {
		c.JSON(http.StatusBadRequest, gin.H{"error": "corpo inválido"})
		return
	}
	if !VerifySignature(h.secret, body, c.GetHeader("X-Hub-Signature")) {
		h.logger.Warn("webhook do hotglue com assinatura inválida", "clientIp", c.ClientIP())
		c.JSON(http.StatusUnauthorized, gin.H{"error": "assinatura inválida"})
		return
	}

	var p LifecyclePayload
	if err := json.Unmarshal(body, &p); err != nil || p.JobID == "" {
		// outros tipos de webhook (discover, connection) não têm job_id: aceitamos e ignoramos
		c.JSON(http.StatusAccepted, gin.H{"ignored": true})
		return
	}
	log := h.logger.With("jobId", p.JobID, "status", p.Status, "tenant", p.Tenant)
	if !Terminal(p.Status) {
		log.Debug("webhook do hotglue: status intermediário")
		c.JSON(http.StatusAccepted, gin.H{"ignored": true, "status": p.Status})
		return
	}

	res, err := h.publisher.PublishMessage(c.Request.Context(), ToMessage(p))
	if err != nil {
		// mensagem duplicada (mesmo messageId) significa que já foi entregue: sucesso
		if domain.KindOf(err) == domain.KindConflict {
			c.JSON(http.StatusOK, gin.H{"duplicate": true})
			return
		}
		log.Error("falha ao publicar mensagem do hotglue", "error", err)
		// 5xx faz o Hotglue reenviar o webhook
		c.JSON(http.StatusBadGateway, gin.H{"error": "falha ao publicar no motor"})
		return
	}
	log.Info("job do hotglue finalizado; mensagem publicada", "messageKey", res.MessageKey)
	c.JSON(http.StatusOK, gin.H{"messageKey": res.MessageKey})
}
