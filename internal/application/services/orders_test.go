package services_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/marconi-prog/nexusflow/internal/application/services"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/domain/inventory"
	"github.com/marconi-prog/nexusflow/internal/domain/order"
	"github.com/marconi-prog/nexusflow/internal/domain/payment"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/database"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/observability"
	"github.com/marconi-prog/nexusflow/tests/fakes"
)

type sagaEnv struct {
	svc       *services.Orders
	inventory *database.MemoryInventory
	engine    *fakes.Fake
}

func newSaga(t *testing.T) *sagaEnv {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	st := database.NewMemoryStore()
	eng := fakes.New()
	inv := database.NewMemoryInventory()
	procs := services.NewProcesses(eng, st, observability.NewRecorder(st, "test", log), log)
	svc := services.NewOrders(database.NewMemoryOrders(), inv, database.NewMemoryPayments(), payment.NewSimulatedGateway(), procs, log)
	return &sagaEnv{svc: svc, inventory: inv, engine: eng}
}

func available(t *testing.T, inv *database.MemoryInventory, sku string) int {
	t.Helper()
	items, _ := inv.ListItems(context.Background())
	for _, it := range items {
		if it.SKU == sku {
			return it.Available
		}
	}
	t.Fatalf("sku %s não existe", sku)
	return 0
}

func register(t *testing.T, e *sagaEnv, id string, items ...order.Item) *order.Order {
	t.Helper()
	o, err := e.svc.Register(context.Background(), id, "Ana", items, "1")
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestHappyPathIsIdempotent(t *testing.T) {
	e := newSaga(t)
	ctx := context.Background()
	o := register(t, e, "P1", order.Item{SKU: "MOUSE", Quantity: 2, UnitPriceCents: 12550})
	if o.TotalCents != 25100 {
		t.Fatalf("total: %d", o.TotalCents)
	}
	// o Camunda pode reentregar jobs: cada passo rodado duas vezes não pode duplicar efeito
	for range 2 {
		if _, err := e.svc.ReserveStock(ctx, "P1"); err != nil {
			t.Fatal(err)
		}
		if _, err := e.svc.Charge(ctx, "P1", "1111"); err != nil {
			t.Fatal(err)
		}
	}
	if got := available(t, e.inventory, "MOUSE"); got != 98 {
		t.Fatalf("estoque baixado duas vezes: %d", got)
	}
	if err := e.svc.Complete(ctx, "P1"); err != nil {
		t.Fatal(err)
	}
	d, _ := e.svc.Get(ctx, "P1")
	if d.Order.Status != order.StatusCompleted || d.Payment.Status != payment.StatusCharged {
		t.Fatalf("%+v %+v", d.Order, d.Payment)
	}
}

func TestInsufficientStockReservesNothing(t *testing.T) {
	e := newSaga(t)
	register(t, e, "P2", order.Item{SKU: "MOUSE", Quantity: 1, UnitPriceCents: 1}, order.Item{SKU: "MONITOR", Quantity: 99, UnitPriceCents: 1})
	_, err := e.svc.ReserveStock(context.Background(), "P2")
	var stock *inventory.InsufficientStockError
	if !errors.As(err, &stock) || stock.SKU != "MONITOR" {
		t.Fatalf("esperava InsufficientStockError de MONITOR: %v", err)
	}
	if got := available(t, e.inventory, "MOUSE"); got != 100 {
		t.Fatalf("reserva parcial não pode acontecer: MOUSE=%d", got)
	}
}

func TestDeclinedPaymentIsStableAndCompensationReleasesStock(t *testing.T) {
	e := newSaga(t)
	ctx := context.Background()
	register(t, e, "P3", order.Item{SKU: "TECLADO", Quantity: 3, UnitPriceCents: 10000})
	if _, err := e.svc.ReserveStock(ctx, "P3"); err != nil {
		t.Fatal(err)
	}
	for range 2 { // a reexecução devolve a mesma recusa
		_, err := e.svc.Charge(ctx, "P3", payment.CardDeclined)
		var d *payment.DeclinedError
		if !errors.As(err, &d) {
			t.Fatalf("esperava recusa: %v", err)
		}
	}
	for range 2 {
		if _, err := e.svc.ReleaseStock(ctx, "P3"); err != nil {
			t.Fatal(err)
		}
	}
	if got := available(t, e.inventory, "TECLADO"); got != 50 {
		t.Fatalf("estoque não voltou (ou voltou em dobro): %d", got)
	}
	if refunded, _ := e.svc.Refund(ctx, "P3"); refunded {
		t.Fatal("pagamento recusado não deve ser estornado")
	}
	if err := e.svc.Reject(ctx, "P3", "recusado"); err != nil {
		t.Fatal(err)
	}
}

func TestUnstableGatewayFailsTechnicallyThenSucceeds(t *testing.T) {
	e := newSaga(t)
	ctx := context.Background()
	register(t, e, "P4", order.Item{SKU: "MOUSE", Quantity: 1, UnitPriceCents: 100})
	for i := 1; i <= 2; i++ {
		_, err := e.svc.Charge(ctx, "P4", payment.CardUnstable)
		var d *payment.DeclinedError
		if err == nil || errors.As(err, &d) {
			t.Fatalf("tentativa %d: esperava falha técnica, veio %v", i, err)
		}
	}
	if _, err := e.svc.Charge(ctx, "P4", payment.CardUnstable); err != nil {
		t.Fatalf("terceira tentativa deveria aprovar: %v", err)
	}
	if refunded, err := e.svc.Refund(ctx, "P4"); err != nil || !refunded {
		t.Fatalf("estorno: %v %v", refunded, err)
	}
}

func TestOrderTransitions(t *testing.T) {
	o, err := order.New("P5", "Ana", []order.Item{{SKU: "A", Quantity: 1, UnitPriceCents: 1}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Complete(); !errors.Is(err, order.ErrInvalidTransition) {
		t.Fatalf("concluir sem pagar deveria falhar: %v", err)
	}
	_ = o.MarkPaid()
	_ = o.Complete()
	if err := o.Reject("x"); !errors.Is(err, order.ErrInvalidTransition) {
		t.Fatalf("rejeitar pedido concluído deveria falhar: %v", err)
	}
	var ve *order.ValidationError
	if _, err := order.New("P6", "Ana", []order.Item{{SKU: "A", Quantity: 0}}, ""); !errors.As(err, &ve) {
		t.Fatalf("quantidade zero deveria ser inválida: %v", err)
	}
}

func TestPlaceStartsSagaWithBusinessKeyAndOnlyCardLast4(t *testing.T) {
	e := newSaga(t)
	placed, err := e.svc.Place(context.Background(), services.PlaceOrderCommand{
		Customer: "Ana", CardNumber: "4111 1111 1111 1234",
		Items: []services.PlaceOrderItem{{SKU: "MOUSE", Quantity: 1, UnitPrice: 9.9}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd := e.engine.CallsTo("StartInstance")[0].Args.(domain.StartInstanceCommand)
	if cmd.ProcessDefinitionID != services.OrderProcessID || cmd.BusinessKey != placed.OrderID {
		t.Fatalf("%+v", cmd)
	}
	if cmd.Variables["cartaoFinal"] != "1234" || cmd.Variables["prazoEntrega"] != services.DefaultDeliveryDeadline {
		t.Fatalf("variáveis: %v", cmd.Variables)
	}
	for k, v := range cmd.Variables {
		if s, ok := v.(string); ok && len(s) >= 16 {
			t.Fatalf("número completo do cartão vazou na variável %s", k)
		}
	}
	_, err = e.svc.Place(context.Background(), services.PlaceOrderCommand{Customer: "Ana", CardNumber: "1234", DeliveryDeadline: "5 minutos",
		Items: []services.PlaceOrderItem{{SKU: "MOUSE", Quantity: 1}}})
	if domain.KindOf(err) != domain.KindInvalid {
		t.Fatalf("prazo inválido deveria dar 400: %v", err)
	}
}
