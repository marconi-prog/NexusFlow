// Comando api: API REST de execução, monitoramento e orquestração de processos.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/marconi-prog/nexusflow/internal/app"
	"github.com/marconi-prog/nexusflow/internal/application/services"
	"github.com/marconi-prog/nexusflow/internal/config"
	"github.com/marconi-prog/nexusflow/internal/domain/payment"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/observability"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/handlers"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/routes"
)

func main() {
	if err := run(); err != nil {
		slog.Error("nexusflow-api encerrado com erro", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := app.NewLogger(cfg.LogLevel, "nexusflow-api")
	if len(cfg.Auth.APIKeys) == 0 {
		logger.Warn("API_KEYS vazio: autenticação DESABILITADA (apenas para desenvolvimento)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := app.NewPersistence(ctx, cfg.Database, logger)
	if err != nil {
		return err
	}
	defer db.Close()
	st := db.Store

	eng, err := app.NewEngine(cfg.Camunda, logger)
	if err != nil {
		return err
	}

	rec := observability.NewRecorder(st, "api", logger)
	mon := services.NewMonitoring(eng, st, rec, logger)
	hub := handlers.NewHub(logger)
	procs := services.NewProcesses(eng, st, rec, logger)
	orders := services.NewOrders(db.Orders, db.Inventory, db.Payments, payment.NewSimulatedGateway(), procs, logger)

	router := routes.New(handlers.Deps{
		Config:     cfg,
		Logger:     logger,
		Store:      st,
		Processes:  procs,
		Orders:     orders,
		Incidents:  services.NewIncidents(eng, rec, logger),
		Tasks:      services.NewTasks(eng, rec),
		Monitoring: mon,
		Hub:        hub,
		Version:    app.Version,
	})

	var bg sync.WaitGroup
	bgCtx, cancelBg := context.WithCancel(ctx)
	defer cancelBg()
	bg.Go(func() { hub.Run(bgCtx, st) })
	bg.Go(func() { mon.WatchIncidents(bgCtx, cfg.Monitor.IncidentPollInterval) })
	bg.Go(func() { mon.RunRetention(bgCtx, cfg.Monitor.EventRetention, cfg.Monitor.IdempotencyRetention) })

	srv := &http.Server{
		Addr:              ":" + cfg.HTTP.Port,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       cfg.HTTP.ReadTimeout,
		WriteTimeout:      cfg.HTTP.WriteTimeout,
		IdleTimeout:       120 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("NexusFlow API iniciada", "port", cfg.HTTP.Port, "camunda", cfg.Camunda.BaseURL, "env", cfg.Env)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	logger.Info("encerrando API")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer cancel()
	cancelBg() // encerra hub (e as conexões SSE) antes de aguardar as requisições
	err = srv.Shutdown(shutdownCtx)
	bg.Wait()
	return err
}
