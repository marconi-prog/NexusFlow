// Package worker implementa job workers escaláveis horizontalmente: cada réplica faz long-polling
// no motor, executa handlers com concorrência limitada e reporta o resultado (complete, fail,
// BPMN error). Várias réplicas podem rodar em paralelo; o motor garante que cada job é
// entregue a um worker por vez (até o timeout de ativação expirar).
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"time"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/observability"
)

// Handler executa um job e devolve as variáveis a mesclar no processo.
// Erros:
//   - *BusinessError  => lança um erro BPMN (capturável por boundary event no modelo)
//   - Permanent(err)  => falha sem retentativas (cria incidente imediatamente)
//   - qualquer outro  => falha com retries-1 e backoff exponencial
type Handler func(ctx context.Context, job *Job) (domain.Variables, error)

// Job é o job ativado, com logger contextualizado.
type Job struct {
	domain.Job
	Logger *slog.Logger
}

// BusinessError representa um erro de negócio modelado no BPMN (error boundary event).
type BusinessError struct {
	Code      string
	Message   string
	Variables domain.Variables
}

func (e *BusinessError) Error() string {
	return fmt.Sprintf("erro de negócio %s: %s", e.Code, e.Message)
}

func NewBusinessError(code, msg string, vars domain.Variables) error {
	return &BusinessError{Code: code, Message: msg, Variables: vars}
}

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent marca o erro como não recuperável por retentativa.
func Permanent(err error) error { return permanentError{err} }

// Spec registra um handler para um tipo de job.
type Spec struct {
	Type           string
	Handler        Handler
	FetchVariables []string      // vazio => todas
	Timeout        time.Duration // tempo de lock do job (0 => padrão do worker)
	Concurrency    int           // 0 => padrão do worker
}

type Options struct {
	Name           string
	MaxJobs        int
	Concurrency    int
	JobTimeout     time.Duration
	PollInterval   time.Duration
	RequestTimeout time.Duration
	ShutdownGrace  time.Duration
}

type Worker struct {
	engine ports.Engine
	audit  *observability.Recorder
	opts   Options
	logger *slog.Logger
	specs  map[string]Spec
}

func New(e ports.Engine, rec *observability.Recorder, opts Options, logger *slog.Logger) *Worker {
	if opts.ShutdownGrace == 0 {
		opts.ShutdownGrace = 30 * time.Second
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = time.Second
	}
	return &Worker{engine: e, audit: rec, opts: opts, logger: logger, specs: map[string]Spec{}}
}

func (w *Worker) Register(s Spec) {
	if s.Type == "" || s.Handler == nil {
		panic("worker: Spec requer Type e Handler")
	}
	if _, dup := w.specs[s.Type]; dup {
		panic("worker: tipo registrado duas vezes: " + s.Type)
	}
	w.specs[s.Type] = s
}

// Types devolve os tipos registrados.
func (w *Worker) Types() []string {
	out := make([]string, 0, len(w.specs))
	for t := range w.specs {
		out = append(out, t)
	}
	return out
}

// Only restringe o worker aos tipos informados (vazio => sem restrição).
func (w *Worker) Only(types []string) error {
	if len(types) == 0 {
		return nil
	}
	keep := map[string]Spec{}
	for _, t := range types {
		s, ok := w.specs[t]
		if !ok {
			return fmt.Errorf("tipo de job sem handler registrado: %s", t)
		}
		keep[t] = s
	}
	w.specs = keep
	return nil
}

// Run bloqueia até ctx ser cancelado; então para de ativar jobs e aguarda os em execução
// (até ShutdownGrace).
func (w *Worker) Run(ctx context.Context) error {
	if len(w.specs) == 0 {
		return errors.New("worker: nenhum handler registrado")
	}
	var pollers sync.WaitGroup
	inflight := &sync.WaitGroup{}
	// contexto dos jobs sobrevive ao cancelamento para permitir drenagem
	jobsCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()

	for _, s := range w.specs {
		pollers.Add(1)
		go func(s Spec) {
			defer pollers.Done()
			w.poll(ctx, jobsCtx, s, inflight)
		}(s)
		w.logger.Info("worker registrado", "type", s.Type)
	}
	<-ctx.Done()
	pollers.Wait()

	done := make(chan struct{})
	go func() { inflight.Wait(); close(done) }()
	select {
	case <-done:
		w.logger.Info("todos os jobs em execução finalizaram")
	case <-time.After(w.opts.ShutdownGrace):
		w.logger.Warn("tempo de drenagem esgotado; jobs restantes serão reativados após o timeout no motor")
		cancelJobs()
	}
	return nil
}

func (w *Worker) poll(ctx, jobsCtx context.Context, s Spec, inflight *sync.WaitGroup) {
	conc := s.Concurrency
	if conc <= 0 {
		conc = w.opts.Concurrency
	}
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = w.opts.JobTimeout
	}
	sem := make(chan struct{}, conc)
	log := w.logger.With("type", s.Type)
	backoff := time.Duration(0)

	for ctx.Err() == nil {
		// espera ao menos uma vaga antes de pedir jobs
		select {
		case sem <- struct{}{}:
			<-sem
		case <-ctx.Done():
			return
		}
		free := conc - len(sem)
		max := min(w.opts.MaxJobs, free)
		if max <= 0 {
			continue
		}

		jobs, err := w.engine.ActivateJobs(ctx, domain.ActivateJobsCommand{
			Type: s.Type, Worker: w.opts.Name, Timeout: timeout, MaxJobs: max,
			FetchVariables: s.FetchVariables, RequestTimeout: w.opts.RequestTimeout,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			observability.JobActivationErrors.WithLabelValues(s.Type).Inc()
			backoff = nextBackoff(backoff)
			log.Warn("falha ao ativar jobs", "error", err, "retryIn", backoff)
			sleep(ctx, backoff)
			continue
		}
		backoff = 0
		if len(jobs) == 0 {
			sleep(ctx, w.opts.PollInterval)
			continue
		}
		for _, j := range jobs {
			sem <- struct{}{}
			inflight.Add(1)
			go func(j domain.Job) {
				defer func() { <-sem; inflight.Done() }()
				w.execute(jobsCtx, s, j)
			}(j)
		}
	}
}

func (w *Worker) execute(ctx context.Context, s Spec, j domain.Job) {
	log := w.logger.With("type", j.Type, "jobKey", j.Key, "processInstanceKey", j.ProcessInstanceKey, "elementId", j.ElementID)
	observability.JobsInFlight.WithLabelValues(j.Type).Inc()
	defer observability.JobsInFlight.WithLabelValues(j.Type).Dec()

	hctx := ctx
	if j.Deadline > 0 {
		var cancel context.CancelFunc
		hctx, cancel = context.WithDeadline(ctx, time.UnixMilli(j.Deadline))
		defer cancel()
	}

	start := time.Now()
	vars, err := safeCall(hctx, s.Handler, &Job{Job: j, Logger: log})
	elapsed := time.Since(start)
	observability.JobDuration.WithLabelValues(j.Type).Observe(elapsed.Seconds())

	// o resultado é reportado mesmo se o deadline do handler estourou
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()

	base := ports.Event{
		JobKey: j.Key, JobType: j.Type, ProcessInstanceKey: j.ProcessInstanceKey,
		ProcessDefinitionID: j.ProcessDefinitionID, DurationMs: elapsed.Milliseconds(), Actor: w.opts.Name,
	}

	var be *BusinessError
	switch {
	case err == nil:
		if cerr := w.engine.CompleteJob(rctx, j.Key, vars); cerr != nil {
			observability.JobsHandled.WithLabelValues(j.Type, "report_error").Inc()
			log.Error("falha ao completar job (será reentregue após o timeout)", "error", cerr)
			return
		}
		observability.JobsHandled.WithLabelValues(j.Type, "completed").Inc()
		log.Debug("job completado", "duration", elapsed)
		base.Type = ports.EventJobCompleted
		base.Data = map[string]any{"elementId": j.ElementID}

	case errors.As(err, &be):
		if terr := w.engine.ThrowError(rctx, j.Key, domain.ThrowErrorCommand{ErrorCode: be.Code, ErrorMessage: be.Message, Variables: be.Variables}); terr != nil {
			observability.JobsHandled.WithLabelValues(j.Type, "report_error").Inc()
			log.Error("falha ao lançar erro BPMN", "error", terr)
			return
		}
		observability.JobsHandled.WithLabelValues(j.Type, "bpmn_error").Inc()
		log.Info("erro de negócio lançado", "code", be.Code, "message", be.Message)
		base.Type = ports.EventJobErrorThrown
		base.Data = map[string]any{"elementId": j.ElementID, "errorCode": be.Code, "errorMessage": be.Message}

	default:
		retries := j.Retries - 1
		if _, ok := errors.AsType[permanentError](err); ok {
			retries = 0
		}
		retries = max(retries, 0)
		backoff := retryBackoff(j)
		if retries == 0 {
			backoff = 0
		}
		if ferr := w.engine.FailJob(rctx, j.Key, domain.FailJobCommand{Retries: retries, ErrorMessage: err.Error(), RetryBackOff: backoff}); ferr != nil {
			observability.JobsHandled.WithLabelValues(j.Type, "report_error").Inc()
			log.Error("falha ao reportar falha do job", "error", ferr, "cause", err)
			return
		}
		observability.JobsHandled.WithLabelValues(j.Type, "failed").Inc()
		log.Warn("job falhou", "error", err, "retriesLeft", retries, "retryBackoff", backoff)
		base.Type = ports.EventJobFailed
		base.Data = map[string]any{"elementId": j.ElementID, "error": err.Error(), "retriesLeft": retries}
	}

	if w.audit != nil {
		w.audit.Record(rctx, base)
	}
}

func safeCall(ctx context.Context, h Handler, j *Job) (vars domain.Variables, err error) {
	defer func() {
		if r := recover(); r != nil {
			j.Logger.Error("panic no handler", "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("panic no handler: %v", r)
		}
	}()
	return h(ctx, j)
}

// retryBackoff usa o header "retryBackoff" (duração Go, ex. "30s") do service task, ou um
// backoff exponencial baseado nas retentativas restantes (supondo 3 retentativas iniciais).
func retryBackoff(j domain.Job) time.Duration {
	if h := j.CustomHeaders["retryBackoff"]; h != "" {
		if d, err := time.ParseDuration(h); err == nil {
			return d
		}
	}
	used := max(0, 3-j.Retries)
	d := 5 * time.Second << used
	return min(d, 5*time.Minute)
}

func nextBackoff(cur time.Duration) time.Duration {
	if cur == 0 {
		return 500 * time.Millisecond
	}
	next := min(cur*2, 30*time.Second)
	return next + time.Duration(rand.IntN(250))*time.Millisecond
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
