package database

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/domain"
)

// Requer NEXUSFLOW_TEST_DATABASE_URL apontando para um banco descartável.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("NEXUSFLOW_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("NEXUSFLOW_TEST_DATABASE_URL não definido")
	}
	ctx := context.Background()
	s, err := New(ctx, url, 5, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.pool.Exec(ctx, "TRUNCATE events, idempotency_keys, process_instances RESTART IDENTITY")
	t.Cleanup(s.Close)
	return s
}

func TestMigrationsAreIdempotent(t *testing.T) {
	s := newTestStore(t)
	if err := s.migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEventsAndListen(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got := make(chan ports.Event, 10)
	go func() { _ = s.Listen(ctx, func(e ports.Event) { got <- e }) }()
	time.Sleep(200 * time.Millisecond) // aguarda o LISTEN

	big := make([]byte, 9000)
	for i := range big {
		big[i] = 'x'
	}
	evs := []ports.Event{
		{Type: ports.EventJobCompleted, JobType: "pagar", DurationMs: 100, ProcessInstanceKey: "1"},
		{Type: ports.EventJobCompleted, JobType: "pagar", DurationMs: 300, ProcessInstanceKey: "1"},
		{Type: ports.EventJobFailed, JobType: "pagar", ProcessInstanceKey: "2", Data: map[string]any{"big": string(big)}},
	}
	for i := range evs {
		if err := s.AppendEvent(ctx, &evs[i]); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 3 {
		select {
		case e := <-got:
			if e.ID != evs[i].ID || e.Type != evs[i].Type {
				t.Fatalf("notificação %d: %+v", i, e)
			}
			if i == 2 && e.Data["big"] != string(big) {
				t.Fatal("payload grande deveria ser buscado por referência")
			}
		case <-ctx.Done():
			t.Fatal("notificação não recebida")
		}
	}

	list, err := s.ListEvents(ctx, ports.EventFilter{ProcessInstanceKey: "1"}, domain.Page{})
	if err != nil || len(list) != 2 || list[0].ID != evs[1].ID {
		t.Fatalf("list: %v %+v", err, list)
	}
	after, _ := s.ListEvents(ctx, ports.EventFilter{AfterID: evs[0].ID}, domain.Page{})
	if len(after) != 2 || after[0].ID != evs[1].ID {
		t.Fatalf("after: %+v", after)
	}

	stats, err := s.JobStats(ctx, time.Now().Add(-time.Hour))
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats: %v %+v", err, stats)
	}
	if st := stats[0]; st.Completed != 2 || st.Failed != 1 || st.AvgDurationMs != 200 {
		t.Fatalf("stats: %+v", st)
	}
	n, err := s.PurgeEvents(ctx, time.Now().Add(time.Minute))
	if err != nil || n != 3 {
		t.Fatalf("purge: %v %d", err, n)
	}
}

func TestIdempotencyLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rec := ports.IdempotencyRecord{Scope: "a", Key: "k", RequestHash: "h1"}

	if ex, err := s.ReserveIdempotency(ctx, rec); err != nil || ex != nil {
		t.Fatalf("reserva: %v %v", ex, err)
	}
	if ex, err := s.ReserveIdempotency(ctx, rec); !errors.Is(err, ports.ErrDuplicate) || ex.StatusCode != 0 {
		t.Fatalf("em andamento: %v %v", ex, err)
	}
	if err := s.CompleteIdempotency(ctx, "a", "k", 201, []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	ex, err := s.ReserveIdempotency(ctx, rec)
	if !errors.Is(err, ports.ErrDuplicate) || ex.StatusCode != 201 || string(ex.Response) != `{"ok":true}` {
		t.Fatalf("concluído: %+v %v", ex, err)
	}
	rec.RequestHash = "h2"
	if _, err := s.ReserveIdempotency(ctx, rec); !errors.Is(err, ports.ErrHashMismatch) {
		t.Fatalf("hash diferente: %v", err)
	}
	if err := s.ReleaseIdempotency(ctx, "a", "k"); err != nil {
		t.Fatal(err)
	}
	if ex, err := s.ReserveIdempotency(ctx, rec); err != nil || ex != nil {
		t.Fatalf("após liberar: %v %v", ex, err)
	}
}

func TestInstanceRegistry(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, k := range []string{"1", "2"} {
		if err := s.SaveInstance(ctx, ports.InstanceRecord{ProcessInstanceKey: k, BusinessKey: "PED-1", ProcessDefinitionID: "pedido"}); err != nil {
			t.Fatal(err)
		}
	}
	recs, err := s.FindInstancesByBusinessKey(ctx, "PED-1")
	if err != nil || len(recs) != 2 {
		t.Fatalf("%v %+v", err, recs)
	}
}
