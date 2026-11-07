// Package app concentra a montagem de dependências compartilhada pelos binários.
package app

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/config"
	"github.com/marconi-prog/nexusflow/internal/domain/inventory"
	"github.com/marconi-prog/nexusflow/internal/domain/order"
	"github.com/marconi-prog/nexusflow/internal/domain/payment"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/camunda"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/database"
)

// Version é sobrescrita no build: -ldflags "-X github.com/marconi-prog/nexusflow/internal/app.Version=1.2.3"
var Version = "dev"

func NewLogger(level, service string) *slog.Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	l := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lv})).With("service", service, "version", Version)
	slog.SetDefault(l)
	return l
}

// Persistence agrupa os repositórios. Com DATABASE_URL usa PostgreSQL (compartilhado
// entre API e workers); sem ele, memória (cada processo com o seu estado).
type Persistence struct {
	Store     ports.Store
	Orders    order.Repository
	Inventory inventory.Repository
	Payments  payment.Repository
	close     func()
}

func (p *Persistence) Close() { p.close() }

func NewPersistence(ctx context.Context, cfg config.DatabaseConfig, logger *slog.Logger) (*Persistence, error) {
	if cfg.URL == "" {
		logger.Warn("DATABASE_URL não definido: usando memória. API e workers NÃO compartilham pedidos nem eventos; use PostgreSQL para rodar a saga de ponta a ponta")
		return &Persistence{
			Store: database.NewMemoryStore(), Orders: database.NewMemoryOrders(),
			Inventory: database.NewMemoryInventory(), Payments: database.NewMemoryPayments(),
			close: func() {},
		}, nil
	}
	st, err := database.New(ctx, cfg.URL, cfg.MaxConns, logger)
	if err != nil {
		return nil, err
	}
	pool := st.Pool()
	return &Persistence{
		Store: st, Orders: database.NewOrderRepository(pool),
		Inventory: database.NewInventoryRepository(pool), Payments: database.NewPaymentRepository(pool),
		close: st.Close,
	}, nil
}

func NewEngine(cfg config.CamundaConfig, logger *slog.Logger) (*camunda.Client, error) {
	return camunda.New(cfg, logger.With("component", "camunda"))
}
