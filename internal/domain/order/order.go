// Package order modela o pedido: itens, total e ciclo de vida.
//
//	CREATED ──► PAID ──► COMPLETED
//	   │          │
//	   └──────────┴──► REJECTED
package order

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Status string

const (
	StatusCreated   Status = "CREATED"
	StatusPaid      Status = "PAID"
	StatusCompleted Status = "COMPLETED"
	StatusRejected  Status = "REJECTED"
)

var (
	ErrNotFound          = errors.New("pedido não encontrado")
	ErrInvalidTransition = errors.New("transição de estado inválida")
)

// ValidationError indica dados de pedido inválidos (não adianta retentar).
type ValidationError struct{ Reason string }

func (e *ValidationError) Error() string { return "pedido inválido: " + e.Reason }

type Item struct {
	SKU            string
	Quantity       int
	UnitPriceCents int64
}

type Order struct {
	ID                 string
	Customer           string
	Items              []Item
	TotalCents         int64
	Status             Status
	RejectionReason    string
	ProcessInstanceKey string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

const maxItems = 100

// New valida os dados e calcula o total.
func New(id, customer string, items []Item, processInstanceKey string) (*Order, error) {
	id, customer = strings.TrimSpace(id), strings.TrimSpace(customer)
	switch {
	case id == "":
		return nil, &ValidationError{"id é obrigatório"}
	case customer == "":
		return nil, &ValidationError{"cliente é obrigatório"}
	case len(items) == 0:
		return nil, &ValidationError{"o pedido precisa de ao menos um item"}
	case len(items) > maxItems:
		return nil, &ValidationError{fmt.Sprintf("máximo de %d itens", maxItems)}
	}
	var total int64
	for i, it := range items {
		if strings.TrimSpace(it.SKU) == "" {
			return nil, &ValidationError{fmt.Sprintf("item %d sem sku", i+1)}
		}
		if it.Quantity <= 0 {
			return nil, &ValidationError{fmt.Sprintf("item %s: quantidade deve ser positiva", it.SKU)}
		}
		if it.UnitPriceCents < 0 {
			return nil, &ValidationError{fmt.Sprintf("item %s: preço negativo", it.SKU)}
		}
		total += int64(it.Quantity) * it.UnitPriceCents
	}
	now := time.Now().UTC()
	return &Order{
		ID: id, Customer: customer, Items: items, TotalCents: total, Status: StatusCreated,
		ProcessInstanceKey: processInstanceKey, CreatedAt: now, UpdatedAt: now,
	}, nil
}

// MarkPaid registra o pagamento. Idempotente.
func (o *Order) MarkPaid() error {
	switch o.Status {
	case StatusPaid:
		return nil
	case StatusCreated:
		o.Status, o.UpdatedAt = StatusPaid, time.Now().UTC()
		return nil
	}
	return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, o.Status, StatusPaid)
}

// Complete conclui o pedido (exige pagamento). Idempotente.
func (o *Order) Complete() error {
	switch o.Status {
	case StatusCompleted:
		return nil
	case StatusPaid:
		o.Status, o.UpdatedAt = StatusCompleted, time.Now().UTC()
		return nil
	}
	return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, o.Status, StatusCompleted)
}

// Reject rejeita o pedido. Idempotente; um pedido concluído não pode ser rejeitado.
func (o *Order) Reject(reason string) error {
	switch o.Status {
	case StatusRejected:
		return nil
	case StatusCompleted:
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, o.Status, StatusRejected)
	}
	o.Status, o.RejectionReason, o.UpdatedAt = StatusRejected, reason, time.Now().UTC()
	return nil
}

type Repository interface {
	// Create insere o pedido; se o id já existir, devolve o existente (idempotência).
	Create(ctx context.Context, o *Order) (*Order, error)
	Get(ctx context.Context, id string) (*Order, error)
	Update(ctx context.Context, o *Order) error
	List(ctx context.Context, status Status, limit, offset int) ([]Order, error)
}
