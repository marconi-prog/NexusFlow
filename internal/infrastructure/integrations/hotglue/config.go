// Package hotglue integra a plataforma ao Hotglue (sincronização de dados entre sistemas).
//
// Fluxo assíncrono usado no BPMN processes/bpmn/sincronizacao-hotglue.bpmn:
//
//	worker hotglue.run-job ──POST /{env}/{flow}/{tenant}/jobs──► Hotglue
//	processo espera a mensagem "hotglue-job-finalizado" (correlationKey = job_id)
//	Hotglue ──webhook (X-Hub-Signature)──► POST /webhooks/hotglue ──publish message──► Camunda
//
// Referências: https://docs.hotglue.com/api-reference/v1/jobs/run-job
// e https://docs.hotglue.com/environment-settings/webhooks
package hotglue

import (
	"errors"
	"os"
	"strings"
	"time"
)

const DefaultBaseURL = "https://api.hotglue.com"

type Config struct {
	BaseURL string
	APIKey  string
	EnvID   string
	FlowID  string
	// WebhookSecret valida a assinatura X-Hub-Signature dos webhooks. Sem ele, o webhook é recusado.
	WebhookSecret string
	Timeout       time.Duration
}

// LoadConfig lê HOTGLUE_* do ambiente. Enabled() indica se a integração foi configurada.
func LoadConfig() Config {
	c := Config{
		BaseURL:       strings.TrimRight(os.Getenv("HOTGLUE_API_URL"), "/"),
		APIKey:        os.Getenv("HOTGLUE_API_KEY"),
		EnvID:         os.Getenv("HOTGLUE_ENV_ID"),
		FlowID:        os.Getenv("HOTGLUE_FLOW_ID"),
		WebhookSecret: os.Getenv("HOTGLUE_WEBHOOK_SECRET"),
		Timeout:       30 * time.Second,
	}
	if c.BaseURL == "" {
		c.BaseURL = DefaultBaseURL
	}
	if d, err := time.ParseDuration(os.Getenv("HOTGLUE_TIMEOUT")); err == nil && d > 0 {
		c.Timeout = d
	}
	return c
}

// Enabled indica se há credenciais para chamar a API.
func (c Config) Enabled() bool { return c.APIKey != "" && c.EnvID != "" }

func (c Config) Validate() error {
	if !c.Enabled() {
		return errors.New("hotglue: HOTGLUE_API_KEY e HOTGLUE_ENV_ID são obrigatórios")
	}
	return nil
}
