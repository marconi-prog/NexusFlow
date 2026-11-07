package database

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marconi-prog/nexusflow/internal/domain/inventory"
	"github.com/marconi-prog/nexusflow/internal/domain/order"
	"github.com/marconi-prog/nexusflow/internal/domain/payment"
)

// Pool expõe o pool para os repositórios de domínio compartilharem as conexões.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// ---------- pedidos ----------

type OrderRepository struct{ pool *pgxpool.Pool }

var _ order.Repository = (*OrderRepository)(nil)

func NewOrderRepository(pool *pgxpool.Pool) *OrderRepository { return &OrderRepository{pool: pool} }

type itemRow struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unitPriceCents"`
}

func (r *OrderRepository) Create(ctx context.Context, o *order.Order) (*order.Order, error) {
	rows := make([]itemRow, 0, len(o.Items))
	for _, it := range o.Items {
		rows = append(rows, itemRow(it))
	}
	items, err := json.Marshal(rows)
	if err != nil {
		return nil, err
	}
	_, err = r.pool.Exec(ctx, `INSERT INTO orders
		(id, customer, items, total_cents, status, rejection_reason, process_instance_key, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (id) DO NOTHING`,
		o.ID, o.Customer, items, o.TotalCents, o.Status, o.RejectionReason, o.ProcessInstanceKey, o.CreatedAt, o.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return r.Get(ctx, o.ID)
}

const orderColumns = `id, customer, items, total_cents, status, rejection_reason, process_instance_key, created_at, updated_at`

func scanOrder(row pgx.Row) (*order.Order, error) {
	var o order.Order
	var items []byte
	err := row.Scan(&o.ID, &o.Customer, &items, &o.TotalCents, &o.Status, &o.RejectionReason, &o.ProcessInstanceKey, &o.CreatedAt, &o.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, order.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var rows []itemRow
	if err := json.Unmarshal(items, &rows); err != nil {
		return nil, err
	}
	for _, it := range rows {
		o.Items = append(o.Items, order.Item(it))
	}
	o.CreatedAt, o.UpdatedAt = o.CreatedAt.UTC(), o.UpdatedAt.UTC()
	return &o, nil
}

func (r *OrderRepository) Get(ctx context.Context, id string) (*order.Order, error) {
	return scanOrder(r.pool.QueryRow(ctx, "SELECT "+orderColumns+" FROM orders WHERE id=$1", id))
}

func (r *OrderRepository) Update(ctx context.Context, o *order.Order) error {
	tag, err := r.pool.Exec(ctx, `UPDATE orders SET status=$2, rejection_reason=$3, updated_at=$4 WHERE id=$1`,
		o.ID, o.Status, o.RejectionReason, o.UpdatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return order.ErrNotFound
	}
	return nil
}

func (r *OrderRepository) List(ctx context.Context, status order.Status, limit, offset int) ([]order.Order, error) {
	rows, err := r.pool.Query(ctx, "SELECT "+orderColumns+` FROM orders
		WHERE ($1 = '' OR status = $1) ORDER BY created_at DESC LIMIT $2 OFFSET $3`, string(status), limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []order.Order{}
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *o)
	}
	return out, rows.Err()
}

// ---------- estoque ----------

type InventoryRepository struct{ pool *pgxpool.Pool }

var _ inventory.Repository = (*InventoryRepository)(nil)

func NewInventoryRepository(pool *pgxpool.Pool) *InventoryRepository {
	return &InventoryRepository{pool: pool}
}

// Reserve roda em uma transação: cada UPDATE condicional bloqueia a linha do produto e só
// baixa se houver saldo. Qualquer falta desfaz tudo (rollback). As linhas chegam ordenadas
// por SKU, então duas reservas concorrentes travam os produtos na mesma ordem (sem deadlock).
func (r *InventoryRepository) Reserve(ctx context.Context, orderID string, lines []inventory.Line) (*inventory.Reservation, error) {
	var res *inventory.Reservation
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		// serializa reservas do mesmo pedido (reentrega do job em paralelo)
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", "reserva:"+orderID); err != nil {
			return err
		}
		existing, err := getReservation(ctx, tx, orderID)
		if err == nil {
			res = existing
			return nil
		}
		if !errors.Is(err, inventory.ErrNotFound) {
			return err
		}
		for _, l := range lines {
			tag, err := tx.Exec(ctx, `UPDATE inventory_items SET available = available - $2 WHERE sku=$1 AND available >= $2`, l.SKU, l.Quantity)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 1 {
				continue
			}
			stockErr := &inventory.InsufficientStockError{SKU: l.SKU, Requested: l.Quantity}
			if err := tx.QueryRow(ctx, "SELECT available FROM inventory_items WHERE sku=$1", l.SKU).Scan(&stockErr.Available); errors.Is(err, pgx.ErrNoRows) {
				stockErr.Unknown = true
			} else if err != nil {
				return err
			}
			return stockErr
		}
		now := time.Now().UTC()
		for _, l := range lines {
			if _, err := tx.Exec(ctx, `INSERT INTO inventory_reservations (order_id, sku, quantity, status, created_at, updated_at)
				VALUES ($1,$2,$3,$4,$5,$5)`, orderID, l.SKU, l.Quantity, inventory.Reserved, now); err != nil {
				return err
			}
		}
		res = &inventory.Reservation{OrderID: orderID, Lines: lines, Status: inventory.Reserved, CreatedAt: now}
		return nil
	})
	return res, err
}

func (r *InventoryRepository) Release(ctx context.Context, orderID string) (bool, error) {
	released := false
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `UPDATE inventory_reservations SET status=$2, updated_at=now()
			WHERE order_id=$1 AND status=$3 RETURNING sku, quantity`, orderID, inventory.Released, inventory.Reserved)
		if err != nil {
			return err
		}
		lines, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (inventory.Line, error) {
			var l inventory.Line
			return l, r.Scan(&l.SKU, &l.Quantity)
		})
		if err != nil {
			return err
		}
		for _, l := range lines {
			if _, err := tx.Exec(ctx, "UPDATE inventory_items SET available = available + $2 WHERE sku=$1", l.SKU, l.Quantity); err != nil {
				return err
			}
		}
		released = len(lines) > 0
		return nil
	})
	return released, err
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func getReservation(ctx context.Context, q querier, orderID string) (*inventory.Reservation, error) {
	rows, err := q.Query(ctx, `SELECT sku, quantity, status, created_at FROM inventory_reservations
		WHERE order_id=$1 ORDER BY sku`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := &inventory.Reservation{OrderID: orderID}
	for rows.Next() {
		var l inventory.Line
		if err := rows.Scan(&l.SKU, &l.Quantity, &res.Status, &res.CreatedAt); err != nil {
			return nil, err
		}
		res.Lines = append(res.Lines, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(res.Lines) == 0 {
		return nil, inventory.ErrNotFound
	}
	res.CreatedAt = res.CreatedAt.UTC()
	return res, nil
}

func (r *InventoryRepository) GetReservation(ctx context.Context, orderID string) (*inventory.Reservation, error) {
	return getReservation(ctx, r.pool, orderID)
}

func (r *InventoryRepository) ListItems(ctx context.Context) ([]inventory.Item, error) {
	rows, err := r.pool.Query(ctx, "SELECT sku, name, available FROM inventory_items ORDER BY sku")
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (inventory.Item, error) {
		var it inventory.Item
		return it, r.Scan(&it.SKU, &it.Name, &it.Available)
	})
}

func (r *InventoryRepository) SetAvailable(ctx context.Context, sku string, available int) error {
	tag, err := r.pool.Exec(ctx, "UPDATE inventory_items SET available=$2 WHERE sku=$1", sku, available)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return inventory.ErrNotFound
	}
	return nil
}

// ---------- pagamentos ----------

type PaymentRepository struct{ pool *pgxpool.Pool }

var _ payment.Repository = (*PaymentRepository)(nil)

func NewPaymentRepository(pool *pgxpool.Pool) *PaymentRepository {
	return &PaymentRepository{pool: pool}
}

func (r *PaymentRepository) Create(ctx context.Context, p *payment.Payment) error {
	now := time.Now().UTC()
	p.CreatedAt, p.UpdatedAt = now, now
	_, err := r.pool.Exec(ctx, `INSERT INTO payments
		(id, order_id, amount_cents, status, transaction_id, decline_reason, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$7)`,
		p.ID, p.OrderID, p.AmountCents, p.Status, p.TransactionID, p.DeclineReason, now)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation em order_id
		return payment.ErrAlreadyExists
	}
	return err
}

func (r *PaymentRepository) GetByOrder(ctx context.Context, orderID string) (*payment.Payment, error) {
	var p payment.Payment
	err := r.pool.QueryRow(ctx, `SELECT id, order_id, amount_cents, status, transaction_id, decline_reason, created_at, updated_at
		FROM payments WHERE order_id=$1`, orderID).
		Scan(&p.ID, &p.OrderID, &p.AmountCents, &p.Status, &p.TransactionID, &p.DeclineReason, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, payment.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	p.CreatedAt, p.UpdatedAt = p.CreatedAt.UTC(), p.UpdatedAt.UTC()
	return &p, nil
}

func (r *PaymentRepository) Update(ctx context.Context, p *payment.Payment) error {
	p.UpdatedAt = time.Now().UTC()
	tag, err := r.pool.Exec(ctx, `UPDATE payments SET status=$2, updated_at=$3 WHERE id=$1`, p.ID, p.Status, p.UpdatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return payment.ErrNotFound
	}
	return nil
}
