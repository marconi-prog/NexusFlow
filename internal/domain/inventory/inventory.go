// Package inventory modela o estoque e as reservas por pedido.
// Uma reserva é tudo-ou-nada: ou todos os itens do pedido são reservados, ou nenhum.
package inventory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type ReservationStatus string

const (
	Reserved ReservationStatus = "RESERVED"
	Released ReservationStatus = "RELEASED"
)

var ErrNotFound = errors.New("produto não encontrado")

// InsufficientStockError é um erro de negócio: o pedido não pode ser atendido.
type InsufficientStockError struct {
	SKU       string
	Requested int
	Available int
	Unknown   bool
}

func (e *InsufficientStockError) Error() string {
	if e.Unknown {
		return fmt.Sprintf("produto %s não existe", e.SKU)
	}
	return fmt.Sprintf("estoque insuficiente para %s: pedido %d, disponível %d", e.SKU, e.Requested, e.Available)
}

type Item struct {
	SKU       string
	Name      string
	Available int
}

type Line struct {
	SKU      string
	Quantity int
}

type Reservation struct {
	OrderID   string
	Lines     []Line
	Status    ReservationStatus
	CreatedAt time.Time
}

// NormalizeLines soma SKUs repetidos e ordena por SKU (ordem fixa evita deadlock no banco).
func NormalizeLines(lines []Line) ([]Line, error) {
	sum := map[string]int{}
	for _, l := range lines {
		sku := strings.TrimSpace(l.SKU)
		if sku == "" || l.Quantity <= 0 {
			return nil, fmt.Errorf("linha inválida: %+v", l)
		}
		sum[sku] += l.Quantity
	}
	out := make([]Line, 0, len(sum))
	for sku, q := range sum {
		out = append(out, Line{SKU: sku, Quantity: q})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SKU < out[j].SKU })
	return out, nil
}

type Repository interface {
	// Reserve baixa o estoque de todas as linhas atomicamente. Se o pedido já tiver
	// reserva, devolve a existente sem baixar de novo (idempotência).
	Reserve(ctx context.Context, orderID string, lines []Line) (*Reservation, error)
	// Release devolve ao estoque uma reserva ativa. Sem reserva ativa, não faz nada.
	Release(ctx context.Context, orderID string) (released bool, err error)
	GetReservation(ctx context.Context, orderID string) (*Reservation, error)
	ListItems(ctx context.Context) ([]Item, error)
	SetAvailable(ctx context.Context, sku string, available int) error
}
