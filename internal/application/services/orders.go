package services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/domain/inventory"
	"github.com/marconi-prog/nexusflow/internal/domain/order"
	"github.com/marconi-prog/nexusflow/internal/domain/payment"
)

// Identificadores do processo processes/bpmn/pedido-saga.bpmn.
const (
	OrderProcessID           = "pedido-saga"
	MessageDeliveryConfirmed = "entrega-confirmada"
	DefaultDeliveryDeadline  = "PT5M"
)

// Orders implementa os passos da saga de pedido (chamados pelos workers) e as operações
// de pedido expostas na API. Toda operação de passo é idempotente, porque o Camunda
// entrega jobs "pelo menos uma vez": o mesmo passo pode rodar de novo após timeout ou falha.
type Orders struct {
	orders    order.Repository
	inventory inventory.Repository
	payments  payment.Repository
	gateway   payment.Gateway
	processes *Processes
	logger    *slog.Logger
}

func NewOrders(o order.Repository, i inventory.Repository, p payment.Repository, g payment.Gateway, procs *Processes, l *slog.Logger) *Orders {
	return &Orders{orders: o, inventory: i, payments: p, gateway: g, processes: procs, logger: l}
}

// ---------- passos da saga (workers) ----------

// Register grava o pedido (job order.create).
func (s *Orders) Register(ctx context.Context, id, customer string, items []order.Item, processInstanceKey string) (*order.Order, error) {
	o, err := order.New(id, customer, items, processInstanceKey)
	if err != nil {
		return nil, err
	}
	return s.orders.Create(ctx, o)
}

// ReserveStock reserva os itens do pedido (job inventory.reserve). Os itens vêm do banco,
// não das variáveis do processo: o pedido gravado é a fonte da verdade.
func (s *Orders) ReserveStock(ctx context.Context, orderID string) (*inventory.Reservation, error) {
	o, err := s.orders.Get(ctx, orderID)
	if err != nil {
		return nil, err
	}
	lines := make([]inventory.Line, 0, len(o.Items))
	for _, it := range o.Items {
		lines = append(lines, inventory.Line{SKU: it.SKU, Quantity: it.Quantity})
	}
	lines, err = inventory.NormalizeLines(lines)
	if err != nil {
		return nil, err
	}
	return s.inventory.Reserve(ctx, orderID, lines)
}

// ReleaseStock é a compensação de ReserveStock (job inventory.release).
func (s *Orders) ReleaseStock(ctx context.Context, orderID string) (bool, error) {
	return s.inventory.Release(ctx, orderID)
}

// Charge cobra o pedido (job payment.charge). Uma recusa é gravada e devolvida como
// *payment.DeclinedError também nas reexecuções.
func (s *Orders) Charge(ctx context.Context, orderID, cardLast4 string) (*payment.Payment, error) {
	existing, err := s.payments.GetByOrder(ctx, orderID)
	switch {
	case err == nil:
		if existing.Status == payment.StatusDeclined {
			return existing, &payment.DeclinedError{Reason: existing.DeclineReason}
		}
		return existing, nil
	case !errors.Is(err, payment.ErrNotFound):
		return nil, err
	}

	o, err := s.orders.Get(ctx, orderID)
	if err != nil {
		return nil, err
	}
	p := &payment.Payment{ID: "PAG-" + uuid.NewString()[:8], OrderID: orderID, AmountCents: o.TotalCents}

	// O OrderID vai como chave de idempotência para o gateway: num provedor real, isso
	// impede cobrança dupla se dois workers chegarem aqui ao mesmo tempo.
	tx, err := s.gateway.Charge(ctx, payment.ChargeRequest{OrderID: orderID, AmountCents: o.TotalCents, CardLast4: cardLast4})
	var declined *payment.DeclinedError
	switch {
	case errors.As(err, &declined):
		p.Status, p.DeclineReason = payment.StatusDeclined, declined.Reason
	case err != nil:
		return nil, err // falha técnica: o worker retenta
	default:
		p.Status, p.TransactionID = payment.StatusCharged, tx
	}

	if err := s.payments.Create(ctx, p); err != nil {
		if !errors.Is(err, payment.ErrAlreadyExists) {
			return nil, err
		}
		return s.Charge(ctx, orderID, cardLast4) // outra execução gravou antes: usa o resultado dela
	}
	if declined != nil {
		return p, declined
	}
	if err := o.MarkPaid(); err != nil {
		return nil, err
	}
	return p, s.orders.Update(ctx, o)
}

// Refund é a compensação de Charge (job payment.refund).
func (s *Orders) Refund(ctx context.Context, orderID string) (bool, error) {
	p, err := s.payments.GetByOrder(ctx, orderID)
	if errors.Is(err, payment.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if p.Status != payment.StatusCharged {
		return false, nil
	}
	if err := s.gateway.Refund(ctx, p.TransactionID); err != nil {
		return false, err
	}
	p.Status = payment.StatusRefunded
	return true, s.payments.Update(ctx, p)
}

// Reject marca o pedido como rejeitado (job order.reject).
func (s *Orders) Reject(ctx context.Context, orderID, reason string) error {
	o, err := s.orders.Get(ctx, orderID)
	if err != nil {
		return err
	}
	if err := o.Reject(reason); err != nil {
		return err
	}
	return s.orders.Update(ctx, o)
}

// Complete conclui o pedido após a entrega (job order.complete).
func (s *Orders) Complete(ctx context.Context, orderID string) error {
	o, err := s.orders.Get(ctx, orderID)
	if err != nil {
		return err
	}
	if err := o.Complete(); err != nil {
		return err
	}
	return s.orders.Update(ctx, o)
}

// ---------- operações da API ----------

type PlaceOrderItem struct {
	SKU       string
	Quantity  int
	UnitPrice float64 // em reais
}

type PlaceOrderCommand struct {
	Customer         string
	Items            []PlaceOrderItem
	CardNumber       string
	DeliveryDeadline string // duração ISO 8601 (ex.: PT5M, P2D)
}

type PlacedOrder struct {
	OrderID            string
	ProcessInstanceKey string
}

var isoDuration = regexp.MustCompile(`^P(\d+D)?(T(\d+H)?(\d+M)?(\d+S)?)?$`)

// Place valida o pedido e inicia a saga no Camunda, usando o id do pedido como business key.
func (s *Orders) Place(ctx context.Context, cmd PlaceOrderCommand) (*PlacedOrder, error) {
	id := "PED-" + strings.ToUpper(uuid.NewString()[:8])
	items := make([]order.Item, 0, len(cmd.Items))
	vars := make([]map[string]any, 0, len(cmd.Items))
	for _, it := range cmd.Items {
		items = append(items, order.Item{SKU: it.SKU, Quantity: it.Quantity, UnitPriceCents: ToCents(it.UnitPrice)})
		vars = append(vars, map[string]any{"sku": it.SKU, "quantidade": it.Quantity, "precoUnitario": it.UnitPrice})
	}
	// valida antes de iniciar o processo, para devolver 400 em vez de criar um incidente
	if _, err := order.New(id, cmd.Customer, items, ""); err != nil {
		return nil, domain.Invalid(err.Error())
	}
	card := strings.ReplaceAll(cmd.CardNumber, " ", "")
	if len(card) < 4 || strings.Trim(card, "0123456789") != "" {
		return nil, domain.Invalid("cartao deve ter ao menos 4 dígitos")
	}
	deadline := cmd.DeliveryDeadline
	if deadline == "" {
		deadline = DefaultDeliveryDeadline
	}
	if !isoDuration.MatchString(deadline) || deadline == "P" || deadline == "PT" {
		return nil, domain.Invalid("prazoEntrega deve ser uma duração ISO 8601 (ex.: PT5M, P2D)")
	}

	inst, err := s.processes.Start(ctx, domain.StartInstanceCommand{
		ProcessDefinitionID: OrderProcessID,
		BusinessKey:         id,
		Variables: domain.Variables{
			"pedidoId":     id,
			"cliente":      cmd.Customer,
			"itens":        vars,
			"cartaoFinal":  card[len(card)-4:],
			"prazoEntrega": deadline,
		},
	})
	if err != nil {
		return nil, err
	}
	return &PlacedOrder{OrderID: id, ProcessInstanceKey: inst.ProcessInstanceKey}, nil
}

type OrderDetail struct {
	Order       *order.Order
	Payment     *payment.Payment
	Reservation *inventory.Reservation
}

func (s *Orders) Get(ctx context.Context, id string) (*OrderDetail, error) {
	o, err := s.orders.Get(ctx, id)
	if err != nil {
		return nil, mapDomainErr(err)
	}
	d := &OrderDetail{Order: o}
	if p, err := s.payments.GetByOrder(ctx, id); err == nil {
		d.Payment = p
	} else if !errors.Is(err, payment.ErrNotFound) {
		return nil, err
	}
	if r, err := s.inventory.GetReservation(ctx, id); err == nil {
		d.Reservation = r
	} else if !errors.Is(err, inventory.ErrNotFound) {
		return nil, err
	}
	return d, nil
}

func (s *Orders) List(ctx context.Context, status string, p domain.Page) ([]order.Order, error) {
	st := order.Status(strings.ToUpper(status))
	switch st {
	case "", order.StatusCreated, order.StatusPaid, order.StatusCompleted, order.StatusRejected:
	default:
		return nil, domain.Invalid("status inválido (CREATED, PAID, COMPLETED, REJECTED)")
	}
	p = p.Normalize()
	return s.orders.List(ctx, st, p.Limit, p.From)
}

// ConfirmDelivery correlaciona a mensagem que libera o passo "Aguardar confirmação de entrega".
func (s *Orders) ConfirmDelivery(ctx context.Context, id string) (*domain.MessageResult, error) {
	if _, err := s.orders.Get(ctx, id); err != nil {
		return nil, mapDomainErr(err)
	}
	return s.processes.PublishMessage(ctx, domain.PublishMessageCommand{
		Name: MessageDeliveryConfirmed, CorrelationKey: id, Correlate: true,
	})
}

func (s *Orders) Inventory(ctx context.Context) ([]inventory.Item, error) {
	return s.inventory.ListItems(ctx)
}

func (s *Orders) SetStock(ctx context.Context, sku string, available int) error {
	if available < 0 {
		return domain.Invalid("available não pode ser negativo")
	}
	return mapDomainErr(s.inventory.SetAvailable(ctx, sku, available))
}

// ToCents converte reais em centavos, arredondando (evita erro de ponto flutuante).
func ToCents(v float64) int64 { return int64(math.Round(v * 100)) }

func mapDomainErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, order.ErrNotFound), errors.Is(err, inventory.ErrNotFound):
		return domain.NotFound(err.Error())
	case errors.Is(err, order.ErrInvalidTransition):
		return domain.Conflict(err.Error())
	}
	var ve *order.ValidationError
	if errors.As(err, &ve) {
		return domain.Invalid(ve.Error())
	}
	return fmt.Errorf("operação de pedido: %w", err)
}
