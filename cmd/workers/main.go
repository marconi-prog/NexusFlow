// Comando worker: executa os job workers (service tasks) dos processos.
// Escale horizontalmente rodando várias réplicas; use WORKER_JOB_TYPES para dedicar réplicas
// a tipos específicos de job.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/marconi-prog/nexusflow/internal/app"
	"github.com/marconi-prog/nexusflow/internal/application/services"
	"github.com/marconi-prog/nexusflow/internal/config"
	"github.com/marconi-prog/nexusflow/internal/domain/payment"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/observability"
	"github.com/marconi-prog/nexusflow/internal/worker"
	"github.com/marconi-prog/nexusflow/internal/worker/handlers"
)

func main() {
	if err := run(); err != nil {
		slog.Error("nexusflow-worker encerrado com erro", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := app.NewLogger(cfg.LogLevel, "nexusflow-worker")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := app.NewPersistence(ctx, cfg.Database, logger)
	if err != nil {
		return err
	}
	defer db.Close()

	eng, err := app.NewEngine(cfg.Camunda, logger)
	if err != nil {
		return err
	}

	rec := observability.NewRecorder(db.Store, "worker", logger)
	w := worker.New(eng, rec, worker.Options{
		Name:           cfg.Worker.Name,
		MaxJobs:        cfg.Worker.MaxJobs,
		Concurrency:    cfg.Worker.Concurrency,
		JobTimeout:     cfg.Worker.JobTimeout,
		PollInterval:   cfg.Worker.PollInterval,
		RequestTimeout: cfg.Worker.RequestTimeout,
		ShutdownGrace:  cfg.Worker.ShutdownGrace,
	}, logger)

	// Handlers genéricos da plataforma
	w.Register(handlers.Log())
	w.Register(handlers.HTTP(handlers.HTTPOptions{AllowedHosts: cfg.Worker.HTTPAllowedHosts}))
	if len(cfg.Worker.HTTPAllowedHosts) == 0 {
		logger.Warn("WORKER_HTTP_ALLOWED_HOSTS vazio: o handler nexusflow.http recusará todos os destinos")
	}

	// Saga de pedido (processes/bpmn/pedido-saga.bpmn). Para adicionar seus próprios
	// processos, registre os job types aqui do mesmo jeito.
	orders := services.NewOrders(db.Orders, db.Inventory, db.Payments, payment.NewSimulatedGateway(),
		services.NewProcesses(eng, db.Store, rec, logger), logger)
	for _, spec := range handlers.Saga(orders) {
		w.Register(spec)
	}

	if err := w.Only(cfg.Worker.JobTypes); err != nil {
		return err
	}

	metricsSrv := &http.Server{Addr: ":" + cfg.Worker.MetricsPort, Handler: workerMux(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("servidor de métricas falhou", "error", err)
		}
	}()
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(sctx)
	}()

	logger.Info("NexusFlow worker iniciado", "name", cfg.Worker.Name, "types", w.Types(), "concurrency", cfg.Worker.Concurrency)
	return w.Run(ctx)
}

func workerMux() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"UP","service":"nexusflow-worker"}`))
	})
	return mux
}
