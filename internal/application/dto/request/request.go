// Package request define os corpos de requisição da API e a conversão para comandos do
// domínio. Junto com o pacote response, forma o contrato público da API.
package request

import (
	"time"

	"github.com/marconi-prog/nexusflow/internal/application/services"
	"github.com/marconi-prog/nexusflow/internal/domain"
)

const maxAwaitTimeout = 5 * time.Minute

// ---------- instâncias ----------

type StartInstance struct {
	ProcessDefinitionID      string         `json:"processDefinitionId"`
	ProcessDefinitionKey     string         `json:"processDefinitionKey"`
	ProcessDefinitionVersion int            `json:"processDefinitionVersion"`
	Variables                map[string]any `json:"variables"`
	TenantID                 string         `json:"tenantId"`
	BusinessKey              string         `json:"businessKey"`
	AwaitCompletion          bool           `json:"awaitCompletion"`
	// RequestTimeout em duração Go (ex.: "30s"); usado com awaitCompletion.
	RequestTimeout string   `json:"requestTimeout"`
	FetchVariables []string `json:"fetchVariables"`
}

func (r StartInstance) ToCommand() (domain.StartInstanceCommand, error) {
	cmd := domain.StartInstanceCommand{
		ProcessDefinitionID:      r.ProcessDefinitionID,
		ProcessDefinitionKey:     r.ProcessDefinitionKey,
		ProcessDefinitionVersion: r.ProcessDefinitionVersion,
		Variables:                r.Variables,
		TenantID:                 r.TenantID,
		BusinessKey:              r.BusinessKey,
		AwaitCompletion:          r.AwaitCompletion,
		FetchVariables:           r.FetchVariables,
	}
	if r.RequestTimeout != "" {
		d, err := time.ParseDuration(r.RequestTimeout)
		if err != nil || d <= 0 || d > maxAwaitTimeout {
			return cmd, domain.Invalid("requestTimeout inválido (máximo 5m)")
		}
		cmd.RequestTimeout = d
	}
	return cmd, nil
}

type CancelInstance struct {
	Reason string `json:"reason"`
}

// ---------- mensagens e sinais ----------

type PublishMessage struct {
	Name           string         `json:"name"`
	CorrelationKey string         `json:"correlationKey"`
	TimeToLive     string         `json:"timeToLive"`
	MessageID      string         `json:"messageId"`
	Variables      map[string]any `json:"variables"`
	TenantID       string         `json:"tenantId"`
	Correlate      bool           `json:"correlate"`
}

func (r PublishMessage) ToCommand() (domain.PublishMessageCommand, error) {
	cmd := domain.PublishMessageCommand{
		Name:           r.Name,
		CorrelationKey: r.CorrelationKey,
		MessageID:      r.MessageID,
		Variables:      r.Variables,
		TenantID:       r.TenantID,
		Correlate:      r.Correlate,
	}
	if r.TimeToLive != "" {
		d, err := time.ParseDuration(r.TimeToLive)
		if err != nil || d < 0 {
			return cmd, domain.Invalid("timeToLive inválido (ex.: 1h)")
		}
		cmd.TimeToLive = d
	}
	return cmd, nil
}

type BroadcastSignal struct {
	SignalName string         `json:"signalName"`
	Variables  map[string]any `json:"variables"`
	TenantID   string         `json:"tenantId"`
}

func (r BroadcastSignal) ToCommand() domain.BroadcastSignalCommand {
	return domain.BroadcastSignalCommand{Name: r.SignalName, Variables: r.Variables, TenantID: r.TenantID}
}

// ---------- incidentes ----------

type ResolveIncident struct {
	Retries int `json:"retries"`
}

func (r ResolveIncident) Validate() error {
	if r.Retries < 0 || r.Retries > 100 {
		return domain.Invalid("retries deve estar entre 0 e 100")
	}
	return nil
}

// ---------- tarefas humanas ----------

type AssignUserTask struct {
	Assignee      string `json:"assignee"`
	AllowOverride bool   `json:"allowOverride"`
}

type CompleteUserTask struct {
	Variables map[string]any `json:"variables"`
	Action    string         `json:"action"`
}

// ---------- instâncias: variáveis ----------

type SetVariables struct {
	Variables map[string]any `json:"variables"`
}

// ---------- pedidos (saga) ----------

type PlaceOrderItem struct {
	SKU       string  `json:"sku"`
	Quantity  int     `json:"quantity"`
	UnitPrice float64 `json:"unitPrice"` // em reais
}

type PlaceOrder struct {
	Customer   string           `json:"customer"`
	Items      []PlaceOrderItem `json:"items"`
	CardNumber string           `json:"cardNumber"`
	// DeliveryDeadline é o prazo do timer de entrega (ISO 8601, padrão PT5M).
	DeliveryDeadline string `json:"deliveryDeadline"`
}

func (r PlaceOrder) ToCommand() services.PlaceOrderCommand {
	items := make([]services.PlaceOrderItem, 0, len(r.Items))
	for _, it := range r.Items {
		items = append(items, services.PlaceOrderItem{SKU: it.SKU, Quantity: it.Quantity, UnitPrice: it.UnitPrice})
	}
	return services.PlaceOrderCommand{Customer: r.Customer, Items: items, CardNumber: r.CardNumber, DeliveryDeadline: r.DeliveryDeadline}
}

type SetStock struct {
	Available *int `json:"available"`
}

func (r SetStock) Validate() error {
	if r.Available == nil {
		return domain.Invalid("available é obrigatório")
	}
	return nil
}
