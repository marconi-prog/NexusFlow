package database

import (
	"context"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/domain"
)

// MemoryStore é uma implementação em memória para desenvolvimento e testes.
// Não compartilha estado entre processos (api e worker).
type MemoryStore struct {
	mu        sync.RWMutex
	seq       int64
	events    []ports.Event
	idem      map[string]*ports.IdempotencyRecord
	instances map[string]ports.InstanceRecord
	listeners map[int]chan ports.Event
	nextL     int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		idem:      map[string]*ports.IdempotencyRecord{},
		instances: map[string]ports.InstanceRecord{},
		listeners: map[int]chan ports.Event{},
	}
}

func (m *MemoryStore) AppendEvent(_ context.Context, e *ports.Event) error {
	m.mu.Lock()
	m.seq++
	e.ID = m.seq
	if e.OccurredAt.IsZero() {
		e.OccurredAt = time.Now().UTC()
	}
	m.events = append(m.events, *e)
	ls := make([]chan ports.Event, 0, len(m.listeners))
	for _, l := range m.listeners {
		ls = append(ls, l)
	}
	m.mu.Unlock()
	for _, l := range ls {
		select {
		case l <- *e:
		default: // listener lento: descarta para não bloquear produtores
		}
	}
	return nil
}

func (m *MemoryStore) ListEvents(_ context.Context, f ports.EventFilter, p domain.Page) ([]ports.Event, error) {
	p = p.Normalize()
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []ports.Event
	for _, e := range slices.Backward(m.events) {
		if !matches(e, f) {
			continue
		}
		out = append(out, e)
	}
	if f.AfterID > 0 { // stream: ordem cronológica
		slices.Reverse(out)
	}
	if p.From >= len(out) {
		return []ports.Event{}, nil
	}
	end := min(p.From+p.Limit, len(out))
	return out[p.From:end], nil
}

func matches(e ports.Event, f ports.EventFilter) bool {
	if len(f.Types) > 0 && !slices.Contains(f.Types, e.Type) {
		return false
	}
	if f.ProcessInstanceKey != "" && e.ProcessInstanceKey != f.ProcessInstanceKey {
		return false
	}
	if f.ProcessDefinitionID != "" && e.ProcessDefinitionID != f.ProcessDefinitionID {
		return false
	}
	if f.JobType != "" && e.JobType != f.JobType {
		return false
	}
	if f.Since != nil && e.OccurredAt.Before(*f.Since) {
		return false
	}
	if f.Until != nil && e.OccurredAt.After(*f.Until) {
		return false
	}
	if f.AfterID > 0 && e.ID <= f.AfterID {
		return false
	}
	return true
}

func (m *MemoryStore) CountEvents(_ context.Context, since time.Time) ([]ports.EventCount, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	counts := map[string]int64{}
	for _, e := range m.events {
		if !e.OccurredAt.Before(since) {
			counts[e.Type]++
		}
	}
	out := make([]ports.EventCount, 0, len(counts))
	for t, c := range counts {
		out = append(out, ports.EventCount{Type: t, Count: c})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out, nil
}

func (m *MemoryStore) JobStats(_ context.Context, since time.Time) ([]ports.JobStat, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	type acc struct {
		stat      ports.JobStat
		durations []float64
	}
	byType := map[string]*acc{}
	for _, e := range m.events {
		if e.JobType == "" || e.OccurredAt.Before(since) {
			continue
		}
		a := byType[e.JobType]
		if a == nil {
			a = &acc{stat: ports.JobStat{JobType: e.JobType}}
			byType[e.JobType] = a
		}
		switch e.Type {
		case ports.EventJobCompleted:
			a.stat.Completed++
			a.durations = append(a.durations, float64(e.DurationMs))
		case ports.EventJobFailed:
			a.stat.Failed++
		case ports.EventJobErrorThrown:
			a.stat.ErrorsThrown++
		}
	}
	out := make([]ports.JobStat, 0, len(byType))
	for _, a := range byType {
		if n := len(a.durations); n > 0 {
			sort.Float64s(a.durations)
			var sum float64
			for _, d := range a.durations {
				sum += d
			}
			a.stat.AvgDurationMs = sum / float64(n)
			a.stat.P95DurationMs = a.durations[min(n-1, int(float64(n)*0.95))]
		}
		out = append(out, a.stat)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JobType < out[j].JobType })
	return out, nil
}

func (m *MemoryStore) PurgeEvents(_ context.Context, before time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := m.events[:0]
	var n int64
	for _, e := range m.events {
		if e.OccurredAt.Before(before) {
			n++
			continue
		}
		kept = append(kept, e)
	}
	m.events = kept
	return n, nil
}

func (m *MemoryStore) Listen(ctx context.Context, fn func(ports.Event)) error {
	ch := make(chan ports.Event, 256)
	m.mu.Lock()
	id := m.nextL
	m.nextL++
	m.listeners[id] = ch
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.listeners, id)
		m.mu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-ch:
			fn(e)
		}
	}
}

func idemKey(scope, key string) string { return scope + "\x00" + key }

func (m *MemoryStore) ReserveIdempotency(_ context.Context, rec ports.IdempotencyRecord) (*ports.IdempotencyRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := idemKey(rec.Scope, rec.Key)
	if existing, ok := m.idem[k]; ok {
		cp := *existing
		if existing.RequestHash != rec.RequestHash {
			return &cp, ports.ErrHashMismatch
		}
		return &cp, ports.ErrDuplicate
	}
	rec.CreatedAt = time.Now().UTC()
	m.idem[k] = &rec
	return nil, nil
}

func (m *MemoryStore) CompleteIdempotency(_ context.Context, scope, key string, status int, response []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.idem[idemKey(scope, key)]
	if !ok {
		return ports.ErrNotFound
	}
	r.StatusCode = status
	r.Response = append([]byte(nil), response...)
	return nil
}

func (m *MemoryStore) ReleaseIdempotency(_ context.Context, scope, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.idem, idemKey(scope, key))
	return nil
}

func (m *MemoryStore) PurgeIdempotency(_ context.Context, before time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for k, r := range m.idem {
		if r.CreatedAt.Before(before) {
			delete(m.idem, k)
			n++
		}
	}
	return n, nil
}

func (m *MemoryStore) SaveInstance(_ context.Context, r ports.InstanceRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.StartedAt.IsZero() {
		r.StartedAt = time.Now().UTC()
	}
	m.instances[r.ProcessInstanceKey] = r
	return nil
}

func (m *MemoryStore) FindInstancesByBusinessKey(_ context.Context, bk string) ([]ports.InstanceRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []ports.InstanceRecord{}
	for _, r := range m.instances {
		if r.BusinessKey == bk {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out, nil
}

func (m *MemoryStore) Ping(context.Context) error { return nil }
func (m *MemoryStore) Close()                     {}
