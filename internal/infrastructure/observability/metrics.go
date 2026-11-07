// Package metrics expõe as métricas Prometheus da plataforma.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const ns = "nexusflow"

var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "http_requests_total", Help: "Requisições HTTP atendidas.",
	}, []string{"method", "route", "status"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "http_request_duration_seconds", Help: "Latência das requisições HTTP.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})

	InstancesStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "process_instances_started_total", Help: "Instâncias de processo iniciadas pela plataforma.",
	}, []string{"process"})

	InstancesCancelled = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "process_instances_cancelled_total", Help: "Instâncias canceladas pela plataforma.",
	})

	MessagesPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "messages_published_total", Help: "Mensagens publicadas/correlacionadas.",
	}, []string{"name"})

	JobsHandled = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "jobs_handled_total", Help: "Jobs processados pelos workers por resultado.",
	}, []string{"type", "outcome"})

	JobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "job_duration_seconds", Help: "Tempo de execução dos handlers de job.",
		Buckets: []float64{.005, .01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300},
	}, []string{"type"})

	JobsInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "jobs_in_flight", Help: "Jobs em execução no momento.",
	}, []string{"type"})

	JobActivationErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "job_activation_errors_total", Help: "Falhas ao ativar jobs no motor.",
	}, []string{"type"})

	IncidentsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "incidents_active", Help: "Incidentes ativos detectados na última varredura.",
	})

	EngineUp = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "engine_up", Help: "1 se o motor de orquestração respondeu à última verificação.",
	})

	EventsRecorded = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "events_recorded_total", Help: "Eventos gravados na trilha de auditoria.",
	}, []string{"type"})

	EventRecordErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "event_record_errors_total", Help: "Falhas ao gravar eventos de auditoria.",
	})
)
