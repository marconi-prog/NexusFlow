package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/marconi-prog/nexusflow/internal/application/services"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/domain/inventory"
	"github.com/marconi-prog/nexusflow/internal/domain/order"
	"github.com/marconi-prog/nexusflow/internal/domain/payment"
	"github.com/marconi-prog/nexusflow/internal/worker"
)

// Job types da saga de pedido: devem bater com o zeebe:taskDefinition de
// processes/bpmn/pedido-saga.bpmn.
const (
	TypeOrderCreate      = "order.create"
	TypeInventoryReserve = "inventory.reserve"
	TypeInventoryRelease = "inventory.release"
	TypePaymentCharge    = "payment.charge"
	TypePaymentRefund    = "payment.refund"
	TypeOrderReject      = "order.reject"
	TypeOrderComplete    = "order.complete"
)

// Códigos de erro BPMN (bpmn:error errorCode) capturados por boundary events no modelo.
const (
	ErrorInsufficientStock = "ESTOQUE_INSUFICIENTE"
	ErrorPaymentDeclined   = "PAGAMENTO_RECUSADO"
)

// Saga devolve os handlers da saga de pedido. Cada um traduz variáveis de processo em
// chamadas ao serviço e classifica os erros:
//
//	erro de negócio (estoque, recusa) => erro BPMN, tratado pelo modelo
//	dado inválido / transição inválida => falha permanente, vira incidente
//	qualquer outro (banco, rede)       => falha técnica, o Camunda retenta
func Saga(s *services.Orders) []worker.Spec {
	return []worker.Spec{
		{Type: TypeOrderCreate, Handler: orderCreate(s)},
		{Type: TypeInventoryReserve, Handler: inventoryReserve(s), FetchVariables: []string{"pedidoId"}},
		{Type: TypeInventoryRelease, Handler: inventoryRelease(s), FetchVariables: []string{"pedidoId"}},
		{Type: TypePaymentCharge, Handler: paymentCharge(s), FetchVariables: []string{"pedidoId", "cartaoFinal"}},
		{Type: TypePaymentRefund, Handler: paymentRefund(s), FetchVariables: []string{"pedidoId"}},
		{Type: TypeOrderReject, Handler: orderReject(s), FetchVariables: []string{"pedidoId", "motivoRejeicao"}},
		{Type: TypeOrderComplete, Handler: orderComplete(s), FetchVariables: []string{"pedidoId"}},
	}
}

func orderCreate(s *services.Orders) worker.Handler {
	return func(ctx context.Context, j *worker.Job) (domain.Variables, error) {
		id := stringVar(j, "pedidoId")
		if id == "" {
			id = stringVar(j, "businessKey")
		}
		if id == "" {
			id = "PED-" + j.ProcessInstanceKey
		}
		items, err := parseItems(j.Variables["itens"])
		if err != nil {
			return nil, worker.Permanent(err)
		}
		o, err := s.Register(ctx, id, stringVar(j, "cliente"), items, j.ProcessInstanceKey)
		if err != nil {
			return nil, classify(err)
		}
		out := domain.Variables{
			"pedidoId":      o.ID,
			"total":         float64(o.TotalCents) / 100,
			"totalCentavos": o.TotalCents,
		}
		if stringVar(j, "prazoEntrega") == "" {
			out["prazoEntrega"] = services.DefaultDeliveryDeadline
		}
		return out, nil
	}
}

func inventoryReserve(s *services.Orders) worker.Handler {
	return func(ctx context.Context, j *worker.Job) (domain.Variables, error) {
		id, err := requireOrderID(j)
		if err != nil {
			return nil, err
		}
		if _, err := s.ReserveStock(ctx, id); err != nil {
			return nil, classify(err)
		}
		return domain.Variables{"estoqueReservado": true}, nil
	}
}

func inventoryRelease(s *services.Orders) worker.Handler {
	return func(ctx context.Context, j *worker.Job) (domain.Variables, error) {
		id, err := requireOrderID(j)
		if err != nil {
			return nil, err
		}
		released, err := s.ReleaseStock(ctx, id)
		if err != nil {
			return nil, classify(err)
		}
		j.Logger.Info("compensação: estoque liberado", "pedidoId", id, "released", released)
		return domain.Variables{"estoqueReservado": false}, nil
	}
}

func paymentCharge(s *services.Orders) worker.Handler {
	return func(ctx context.Context, j *worker.Job) (domain.Variables, error) {
		id, err := requireOrderID(j)
		if err != nil {
			return nil, err
		}
		p, err := s.Charge(ctx, id, stringVar(j, "cartaoFinal"))
		if err != nil {
			return nil, classify(err)
		}
		return domain.Variables{"pagamentoId": p.ID}, nil
	}
}

func paymentRefund(s *services.Orders) worker.Handler {
	return func(ctx context.Context, j *worker.Job) (domain.Variables, error) {
		id, err := requireOrderID(j)
		if err != nil {
			return nil, err
		}
		refunded, err := s.Refund(ctx, id)
		if err != nil {
			return nil, classify(err)
		}
		j.Logger.Info("compensação: pagamento estornado", "pedidoId", id, "refunded", refunded)
		return domain.Variables{"pagamentoEstornado": refunded}, nil
	}
}

func orderReject(s *services.Orders) worker.Handler {
	return func(ctx context.Context, j *worker.Job) (domain.Variables, error) {
		id, err := requireOrderID(j)
		if err != nil {
			return nil, err
		}
		reason := stringVar(j, "motivoRejeicao")
		if reason == "" {
			// único caminho sem motivo explícito: a aprovação manual reprovou o pedido
			reason = "reprovado na aprovação manual"
		}
		if err := s.Reject(ctx, id, reason); err != nil {
			return nil, classify(err)
		}
		return domain.Variables{"motivoRejeicao": reason}, nil
	}
}

func orderComplete(s *services.Orders) worker.Handler {
	return func(ctx context.Context, j *worker.Job) (domain.Variables, error) {
		id, err := requireOrderID(j)
		if err != nil {
			return nil, err
		}
		if err := s.Complete(ctx, id); err != nil {
			return nil, classify(err)
		}
		return nil, nil
	}
}

// ---------- helpers ----------

func classify(err error) error {
	var stock *inventory.InsufficientStockError
	var declined *payment.DeclinedError
	var invalid *order.ValidationError
	switch {
	case errors.As(err, &stock):
		return worker.NewBusinessError(ErrorInsufficientStock, stock.Error(), domain.Variables{"motivoRejeicao": stock.Error()})
	case errors.As(err, &declined):
		return worker.NewBusinessError(ErrorPaymentDeclined, declined.Error(), domain.Variables{"motivoRejeicao": declined.Error()})
	case errors.As(err, &invalid), errors.Is(err, order.ErrInvalidTransition), errors.Is(err, order.ErrNotFound):
		return worker.Permanent(err)
	}
	return err
}

func requireOrderID(j *worker.Job) (string, error) {
	id := stringVar(j, "pedidoId")
	if id == "" {
		return "", worker.Permanent(errors.New("variável pedidoId ausente"))
	}
	return id, nil
}

func stringVar(j *worker.Job, name string) string {
	s, _ := j.Variables[name].(string)
	return s
}

// parseItems lê a variável "itens": [{"sku": "...", "quantidade": 1, "precoUnitario": 9.9}].
func parseItems(v any) ([]order.Item, error) {
	list, ok := v.([]any)
	if !ok {
		return nil, errors.New("variável itens ausente ou não é uma lista")
	}
	items := make([]order.Item, 0, len(list))
	for i, raw := range list {
		m, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("itens[%d] não é um objeto", i)
		}
		sku, _ := m["sku"].(string)
		qty, okQ := m["quantidade"].(float64)
		price, okP := m["precoUnitario"].(float64)
		if !okQ || !okP || qty != float64(int(qty)) {
			return nil, fmt.Errorf("itens[%d]: quantidade (inteiro) e precoUnitario (número) são obrigatórios", i)
		}
		items = append(items, order.Item{SKU: sku, Quantity: int(qty), UnitPriceCents: services.ToCents(price)})
	}
	return items, nil
}
