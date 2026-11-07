package services

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/observability"
)

type Monitoring struct {
	engine ports.Engine
	store  ports.Store
	audit  *observability.Recorder
	logger *slog.Logger
}

func NewMonitoring(e ports.Engine, s ports.Store, a *observability.Recorder, l *slog.Logger) *Monitoring {
	return &Monitoring{engine: e, store: s, audit: a, logger: l}
}

type ComponentHealth struct {
	Status  string // UP | DOWN
	Detail  string
	Latency string
}

type Health struct {
	Status     string
	Components map[string]ComponentHealth
}

// Readiness verifica as dependências (motor e banco) em paralelo.
func (m *Monitoring) Readiness(ctx context.Context) Health {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	h := Health{Status: "UP", Components: map[string]ComponentHealth{}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	check := func(name string, fn func(context.Context) error) {
		defer wg.Done()
		start := time.Now()
		err := fn(ctx)
		c := ComponentHealth{Status: "UP", Latency: time.Since(start).Round(time.Millisecond).String()}
		if err != nil {
			c.Status, c.Detail = "DOWN", err.Error()
		}
		mu.Lock()
		h.Components[name] = c
		if err != nil {
			h.Status = "DOWN"
		}
		mu.Unlock()
	}
	wg.Add(2)
	go check("engine", func(ctx context.Context) error {
		_, err := m.engine.Topology(ctx)
		if err != nil {
			observability.EngineUp.Set(0)
		} else {
			observability.EngineUp.Set(1)
		}
		return err
	})
	go check("database", m.store.Ping)
	wg.Wait()
	return h
}

type Overview struct {
	GeneratedAt      time.Time
	Window           string
	Engine           *domain.Topology
	EngineError      string
	ActiveInstances  int
	InstancesWithInc int
	ActiveIncidents  int
	OpenUserTasks    int
	Events           []ports.EventCount
	Jobs             []ports.JobStat
	Partial          bool
	Errors           []string
}

// Overview monta o painel consolidado. Falhas parciais não derrubam a resposta: os campos
// afetados ficam zerados e Partial=true.
func (m *Monitoring) Overview(ctx context.Context, window time.Duration) *Overview {
	if window <= 0 {
		window = 24 * time.Hour
	}
	since := time.Now().Add(-window)
	o := &Overview{GeneratedAt: time.Now().UTC(), Window: window.String(), Events: []ports.EventCount{}, Jobs: []ports.JobStat{}}
	var mu sync.Mutex
	fail := func(what string, err error) {
		mu.Lock()
		o.Partial = true
		o.Errors = append(o.Errors, what+": "+err.Error())
		mu.Unlock()
	}
	g, ctx := errgroup.WithContext(ctx)
	one := domain.Page{Limit: 1}
	g.Go(func() error {
		t, err := m.engine.Topology(ctx)
		if err != nil {
			mu.Lock()
			o.EngineError = err.Error()
			mu.Unlock()
			fail("topology", err)
			return nil
		}
		o.Engine = t
		return nil
	})
	g.Go(func() error {
		r, err := m.engine.SearchInstances(ctx, domain.ProcessInstanceFilter{State: domain.InstanceActive}, one, nil)
		if err != nil {
			fail("activeInstances", err)
			return nil
		}
		o.ActiveInstances = r.Total
		return nil
	})
	g.Go(func() error {
		yes := true
		r, err := m.engine.SearchInstances(ctx, domain.ProcessInstanceFilter{State: domain.InstanceActive, HasIncident: &yes}, one, nil)
		if err != nil {
			fail("instancesWithIncident", err)
			return nil
		}
		o.InstancesWithInc = r.Total
		return nil
	})
	g.Go(func() error {
		r, err := m.engine.SearchIncidents(ctx, domain.IncidentFilter{State: "ACTIVE"}, one)
		if err != nil {
			fail("activeIncidents", err)
			return nil
		}
		o.ActiveIncidents = r.Total
		return nil
	})
	g.Go(func() error {
		r, err := m.engine.SearchUserTasks(ctx, domain.UserTaskFilter{State: "CREATED"}, one)
		if err != nil {
			fail("openUserTasks", err)
			return nil
		}
		o.OpenUserTasks = r.Total
		return nil
	})
	g.Go(func() error {
		r, err := m.store.CountEvents(ctx, since)
		if err != nil {
			fail("events", err)
			return nil
		}
		o.Events = r
		return nil
	})
	g.Go(func() error {
		r, err := m.store.JobStats(ctx, since)
		if err != nil {
			fail("jobs", err)
			return nil
		}
		o.Jobs = r
		return nil
	})
	_ = g.Wait()
	return o
}

func (m *Monitoring) Topology(ctx context.Context) (*domain.Topology, error) {
	return m.engine.Topology(ctx)
}

func (m *Monitoring) Events(ctx context.Context, f ports.EventFilter, p domain.Page) ([]ports.Event, error) {
	return m.store.ListEvents(ctx, f, p)
}

func (m *Monitoring) JobStats(ctx context.Context, window time.Duration) ([]ports.JobStat, error) {
	return m.store.JobStats(ctx, time.Now().Add(-window))
}

// WatchIncidents varre periodicamente os incidentes ativos, atualiza a métrica e grava um
// evento incident.detected para cada incidente novo. O conjunto de vistos fica em memória;
// com várias réplicas da API, cada uma emite os próprios eventos — habilite em uma só
// (MONITOR_INCIDENT_POLL_INTERVAL=0 nas demais) se quiser evitar duplicidade.
func (m *Monitoring) WatchIncidents(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	seen := map[string]bool{}
	first := true
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		m.scanIncidents(ctx, seen, first)
		first = false
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (m *Monitoring) scanIncidents(ctx context.Context, seen map[string]bool, first bool) {
	const pageSize = 200
	current := map[string]bool{}
	total := 0
	for from := 0; ; from += pageSize {
		r, err := m.engine.SearchIncidents(ctx, domain.IncidentFilter{State: "ACTIVE"}, domain.Page{From: from, Limit: pageSize})
		if err != nil {
			if ctx.Err() == nil {
				m.logger.Warn("varredura de incidentes falhou", "error", err)
			}
			return
		}
		total = r.Total
		for _, inc := range r.Items {
			current[inc.Key] = true
			if seen[inc.Key] || first {
				continue
			}
			m.audit.Record(ctx, ports.Event{
				Type: ports.EventIncidentDetected, IncidentKey: inc.Key, JobKey: inc.JobKey,
				ProcessInstanceKey: inc.ProcessInstanceKey, ProcessDefinitionID: inc.ProcessDefinitionID,
				Data: map[string]any{"errorType": inc.ErrorType, "errorMessage": inc.ErrorMessage, "elementId": inc.ElementID},
			})
		}
		if len(r.Items) < pageSize || from+pageSize >= 10_000 {
			break
		}
	}
	observability.IncidentsActive.Set(float64(total))
	// mantém apenas os ainda ativos para que reincidências sejam notificadas
	for k := range seen {
		delete(seen, k)
	}
	for k := range current {
		seen[k] = true
	}
}

// RunRetention remove eventos e chaves de idempotência antigas uma vez por hora.
func (m *Monitoring) RunRetention(ctx context.Context, eventRetention, idemRetention time.Duration) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		if eventRetention > 0 {
			if n, err := m.store.PurgeEvents(ctx, time.Now().Add(-eventRetention)); err != nil {
				if ctx.Err() != nil {
					return // encerramento da aplicação, não é falha
				}
				m.logger.Warn("retenção de eventos falhou", "error", err)
			} else if n > 0 {
				m.logger.Info("eventos antigos removidos", "count", n)
			}
		}
		if idemRetention > 0 {
			if _, err := m.store.PurgeIdempotency(ctx, time.Now().Add(-idemRetention)); err != nil && ctx.Err() == nil {
				m.logger.Warn("retenção de idempotência falhou", "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
