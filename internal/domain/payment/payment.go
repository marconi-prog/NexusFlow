// Package payment modela a cobrança de um pedido e o seu estorno.
package payment

import (
	"context"
	"errors"
	"time"
)

type Status string

const (
	StatusCharged  Status = "CHARGED"
	StatusDeclined Status = "DECLINED"
	StatusRefunded Status = "REFUNDED"
)

var (
	ErrNotFound      = errors.New("pagamento não encontrado")
	ErrAlreadyExists = errors.New("pedido já possui pagamento")
)

// DeclinedError é um erro de negócio: o emissor recusou a cobrança.
type DeclinedError struct{ Reason string }

func (e *DeclinedError) Error() string { return "pagamento recusado: " + e.Reason }

type Payment struct {
	ID            string
	OrderID       string
	AmountCents   int64
	Status        Status
	TransactionID string
	DeclineReason string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// ChargeRequest é o que o gateway recebe. Só os 4 últimos dígitos do cartão circulam pela
// plataforma: dado sensível não deve virar variável de processo.
type ChargeRequest struct {
	OrderID     string
	AmountCents int64
	CardLast4   string
}

// Gateway é o provedor de pagamentos. Devolve *DeclinedError para recusa e qualquer outro
// erro para falha técnica (que o worker retenta).
type Gateway interface {
	Charge(ctx context.Context, req ChargeRequest) (transactionID string, err error)
	Refund(ctx context.Context, transactionID string) error
}

type Repository interface {
	// Create grava o pagamento; ErrAlreadyExists se o pedido já tiver um.
	Create(ctx context.Context, p *Payment) error
	GetByOrder(ctx context.Context, orderID string) (*Payment, error)
	Update(ctx context.Context, p *Payment) error
}
