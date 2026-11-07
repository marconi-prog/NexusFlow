package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/database"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/observability"
	"github.com/marconi-prog/nexusflow/tests/fakes"
)

func runWorker(t *testing.T, fake *fakes.Fake, st ports.Store, spec Spec, until func() bool) {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	w := New(fake, observability.NewRecorder(st, "worker", log), Options{
		Name: "test", MaxJobs: 10, Concurrency: 4, JobTimeout: time.Minute, PollInterval: 5 * time.Millisecond,
	}, log)
	w.Register(spec)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	deadline := time.Now().Add(3 * time.Second)
	for !until() {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("timeout aguardando o worker")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
}

func job(key string, retries int) domain.Job {
	return domain.Job{Key: key, Type: "t", ProcessInstanceKey: "100", Retries: retries, Variables: domain.Variables{}, CustomHeaders: map[string]string{}}
}

func TestWorkerOutcomes(t *testing.T) {
	fake := fakes.New()
	fake.JobQueue["t"] = []domain.Job{job("ok", 3), job("biz", 3), job("tech", 3), job("perm", 3), job("panic", 1)}
	st := database.NewMemoryStore()

	handler := func(ctx context.Context, j *Job) (domain.Variables, error) {
		switch j.Key {
		case "ok":
			return domain.Variables{"done": true}, nil
		case "biz":
			return nil, NewBusinessError("SEM_SALDO", "saldo insuficiente", nil)
		case "tech":
			return nil, errors.New("timeout no serviço")
		case "perm":
			return nil, Permanent(errors.New("dados inválidos"))
		default:
			panic("boom")
		}
	}
	runWorker(t, fake, st, Spec{Type: "t", Handler: handler}, func() bool {
		return len(fake.CallsTo("CompleteJob"))+len(fake.CallsTo("ThrowError"))+len(fake.CallsTo("FailJob")) == 5
	})

	if c := fake.CallsTo("CompleteJob"); len(c) != 1 || c[0].Key != "ok" || c[0].Args.(domain.Variables)["done"] != true {
		t.Fatalf("complete: %+v", c)
	}
	if c := fake.CallsTo("ThrowError"); len(c) != 1 || c[0].Args.(domain.ThrowErrorCommand).ErrorCode != "SEM_SALDO" {
		t.Fatalf("throw: %+v", c)
	}
	fails := map[string]domain.FailJobCommand{}
	for _, c := range fake.CallsTo("FailJob") {
		fails[c.Key] = c.Args.(domain.FailJobCommand)
	}
	if f := fails["tech"]; f.Retries != 2 || f.RetryBackOff <= 0 {
		t.Errorf("falha técnica deveria decrementar retries com backoff: %+v", f)
	}
	if f := fails["perm"]; f.Retries != 0 {
		t.Errorf("falha permanente deveria zerar retries: %+v", f)
	}
	if f := fails["panic"]; f.Retries != 0 || f.ErrorMessage == "" {
		t.Errorf("panic deveria virar falha: %+v", f)
	}

	stats, _ := st.JobStats(context.Background(), time.Now().Add(-time.Hour))
	if len(stats) != 1 || stats[0].Completed != 1 || stats[0].Failed != 3 || stats[0].ErrorsThrown != 1 {
		t.Fatalf("estatísticas: %+v", stats)
	}
}

func TestWorkerRespectsConcurrency(t *testing.T) {
	fake := fakes.New()
	for i := range 20 {
		fake.JobQueue["t"] = append(fake.JobQueue["t"], job(string(rune('a'+i)), 3))
	}
	var cur, peak atomic.Int32
	handler := func(ctx context.Context, j *Job) (domain.Variables, error) {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		cur.Add(-1)
		return nil, nil
	}
	runWorker(t, fake, database.NewMemoryStore(), Spec{Type: "t", Handler: handler, Concurrency: 3}, func() bool {
		return len(fake.CallsTo("CompleteJob")) == 20
	})
	if peak.Load() > 3 {
		t.Fatalf("concorrência excedida: %d", peak.Load())
	}
}

func TestRetryBackoffHeader(t *testing.T) {
	j := job("x", 3)
	j.CustomHeaders["retryBackoff"] = "42s"
	if d := retryBackoff(j); d != 42*time.Second {
		t.Fatalf("got %v", d)
	}
	j = job("x", 1)
	if d := retryBackoff(j); d != 20*time.Second {
		t.Fatalf("got %v", d)
	}
}

func TestOnlyRejectsUnknownTypes(t *testing.T) {
	w := New(fakes.New(), nil, Options{}, slog.New(slog.DiscardHandler))
	w.Register(Spec{Type: "a", Handler: func(context.Context, *Job) (domain.Variables, error) { return nil, nil }})
	if err := w.Only([]string{"b"}); err == nil {
		t.Fatal("esperava erro para tipo desconhecido")
	}
}
