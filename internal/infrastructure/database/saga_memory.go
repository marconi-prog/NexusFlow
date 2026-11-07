package database

import (
	"context"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/marconi-prog/nexusflow/internal/domain/inventory"
	"github.com/marconi-prog/nexusflow/internal/domain/order"
	"github.com/marconi-prog/nexusflow/internal/domain/payment"
)

// Repositórios em memória da saga, para testes e desenvolvimento sem banco.
// Atenção: API e worker são processos separados; sem PostgreSQL eles não enxergam os
// mesmos pedidos.

type MemoryOrders struct {
	mu     sync.Mutex
	orders map[string]order.Order
}

func NewMemoryOrders() *MemoryOrders { return &MemoryOrders{orders: map[string]order.Order{}} }

func (m *MemoryOrders) Create(_ context.Context, o *order.Order) (*order.Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.orders[o.ID]; ok {
		return copyOrder(existing), nil
	}
	m.orders[o.ID] = *copyOrder(*o)
	return copyOrder(*o), nil
}

func (m *MemoryOrders) Get(_ context.Context, id string) (*order.Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.orders[id]
	if !ok {
		return nil, order.ErrNotFound
	}
	return copyOrder(o), nil
}

func (m *MemoryOrders) Update(_ context.Context, o *order.Order) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.orders[o.ID]; !ok {
		return order.ErrNotFound
	}
	m.orders[o.ID] = *copyOrder(*o)
	return nil
}

func (m *MemoryOrders) List(_ context.Context, status order.Status, limit, offset int) ([]order.Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []order.Order{}
	for _, o := range m.orders {
		if status == "" || o.Status == status {
			out = append(out, *copyOrder(o))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if offset >= len(out) {
		return []order.Order{}, nil
	}
	return out[offset:min(offset+limit, len(out))], nil
}

func copyOrder(o order.Order) *order.Order {
	o.Items = slices.Clone(o.Items)
	return &o
}

type MemoryInventory struct {
	mu           sync.Mutex
	items        map[string]inventory.Item
	reservations map[string]inventory.Reservation
}

// NewMemoryInventory cria o estoque com os mesmos produtos semeados pela migration 0002.
func NewMemoryInventory() *MemoryInventory {
	m := &MemoryInventory{items: map[string]inventory.Item{}, reservations: map[string]inventory.Reservation{}}
	for _, it := range []inventory.Item{
		{SKU: "NOTEBOOK", Name: `Notebook 14"`, Available: 10},
		{SKU: "MONITOR", Name: `Monitor 27"`, Available: 5},
		{SKU: "TECLADO", Name: "Teclado mecânico", Available: 50},
		{SKU: "MOUSE", Name: "Mouse sem fio", Available: 100},
	} {
		m.items[it.SKU] = it
	}
	return m
}

func (m *MemoryInventory) Reserve(_ context.Context, orderID string, lines []inventory.Line) (*inventory.Reservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.reservations[orderID]; ok {
		r.Lines = slices.Clone(r.Lines)
		return &r, nil
	}
	for _, l := range lines {
		it, ok := m.items[l.SKU]
		if !ok {
			return nil, &inventory.InsufficientStockError{SKU: l.SKU, Requested: l.Quantity, Unknown: true}
		}
		if it.Available < l.Quantity {
			return nil, &inventory.InsufficientStockError{SKU: l.SKU, Requested: l.Quantity, Available: it.Available}
		}
	}
	for _, l := range lines {
		it := m.items[l.SKU]
		it.Available -= l.Quantity
		m.items[l.SKU] = it
	}
	r := inventory.Reservation{OrderID: orderID, Lines: slices.Clone(lines), Status: inventory.Reserved, CreatedAt: time.Now().UTC()}
	m.reservations[orderID] = r
	return &r, nil
}

func (m *MemoryInventory) Release(_ context.Context, orderID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.reservations[orderID]
	if !ok || r.Status != inventory.Reserved {
		return false, nil
	}
	for _, l := range r.Lines {
		it := m.items[l.SKU]
		it.Available += l.Quantity
		m.items[l.SKU] = it
	}
	r.Status = inventory.Released
	m.reservations[orderID] = r
	return true, nil
}

func (m *MemoryInventory) GetReservation(_ context.Context, orderID string) (*inventory.Reservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.reservations[orderID]
	if !ok {
		return nil, inventory.ErrNotFound
	}
	r.Lines = slices.Clone(r.Lines)
	return &r, nil
}

func (m *MemoryInventory) ListItems(context.Context) ([]inventory.Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]inventory.Item, 0, len(m.items))
	for _, it := range m.items {
		out = append(out, it)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SKU < out[j].SKU })
	return out, nil
}

func (m *MemoryInventory) SetAvailable(_ context.Context, sku string, available int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	it, ok := m.items[sku]
	if !ok {
		return inventory.ErrNotFound
	}
	it.Available = available
	m.items[sku] = it
	return nil
}

type MemoryPayments struct {
	mu       sync.Mutex
	payments map[string]payment.Payment // por pedido
}

func NewMemoryPayments() *MemoryPayments {
	return &MemoryPayments{payments: map[string]payment.Payment{}}
}

func (m *MemoryPayments) Create(_ context.Context, p *payment.Payment) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.payments[p.OrderID]; ok {
		return payment.ErrAlreadyExists
	}
	now := time.Now().UTC()
	p.CreatedAt, p.UpdatedAt = now, now
	m.payments[p.OrderID] = *p
	return nil
}

func (m *MemoryPayments) GetByOrder(_ context.Context, orderID string) (*payment.Payment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.payments[orderID]
	if !ok {
		return nil, payment.ErrNotFound
	}
	return &p, nil
}

func (m *MemoryPayments) Update(_ context.Context, p *payment.Payment) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.payments[p.OrderID]; !ok {
		return payment.ErrNotFound
	}
	p.UpdatedAt = time.Now().UTC()
	m.payments[p.OrderID] = *p
	return nil
}
