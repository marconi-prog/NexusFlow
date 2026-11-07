// Package store define a persistência própria da plataforma: trilha de eventos (auditoria e
// monitoramento), chaves de idempotência e registro de business keys das instâncias.
// O estado dos processos em si continua sendo do Camunda.
package ports

import (
	"context"
	"errors"
	"time"

	"github.com/marconi-prog/nexusflow/internal/domain"
)

// Tipos de evento emitidos pela plataforma.
const (
	EventDeploymentCreated  = "deployment.created"
	EventInstanceStarted    = "instance.started"
	EventInstanceCancelled  = "instance.cancelled"
	EventMessagePublished   = "message.published"
	EventSignalBroadcast    = "signal.broadcast"
	EventIncidentDetected   = "incident.detected"
	EventIncidentResolved   = "incident.resolved"
	EventJobCompleted       = "job.completed"
	EventJobFailed          = "job.failed"
	EventJobErrorThrown     = "job.error_thrown"
	EventUserTaskAssigned   = "usertask.assigned"
	EventUserTaskUnassigned = "usertask.unassigned"
	EventUserTaskCompleted  = "usertask.completed"
	EventJobRetriesUpdated  = "job.retries_updated"
	EventVariablesUpdated   = "instance.variables_updated"
)

type Event struct {
	ID                  int64          `json:"id"`
	Type                string         `json:"type"`
	Source              string         `json:"source"`
	Actor               string         `json:"actor,omitempty"`
	OccurredAt          time.Time      `json:"occurredAt"`
	ProcessInstanceKey  string         `json:"processInstanceKey,omitempty"`
	ProcessDefinitionID string         `json:"processDefinitionId,omitempty"`
	JobKey              string         `json:"jobKey,omitempty"`
	JobType             string         `json:"jobType,omitempty"`
	IncidentKey         string         `json:"incidentKey,omitempty"`
	DurationMs          int64          `json:"durationMs,omitempty"`
	Data                map[string]any `json:"data,omitempty"`
}

type EventFilter struct {
	Types               []string
	ProcessInstanceKey  string
	ProcessDefinitionID string
	JobType             string
	Since               *time.Time
	Until               *time.Time
	AfterID             int64
}

// JobStat agrega execuções de jobs por tipo em uma janela.
type JobStat struct {
	JobType       string
	Completed     int64
	Failed        int64
	ErrorsThrown  int64
	AvgDurationMs float64
	P95DurationMs float64
}

// EventCount conta eventos por tipo.
type EventCount struct {
	Type  string
	Count int64
}

// IdempotencyRecord guarda a resposta de uma operação para repeti-la em retentativas do cliente.
type IdempotencyRecord struct {
	Scope       string
	Key         string
	RequestHash string
	StatusCode  int // 0 => em andamento
	Response    []byte
	CreatedAt   time.Time
}

// InstanceRecord liga uma instância do Camunda a uma business key e ao autor.
type InstanceRecord struct {
	ProcessInstanceKey  string
	BusinessKey         string
	ProcessDefinitionID string
	TenantID            string
	StartedBy           string
	StartedAt           time.Time
}

var (
	ErrNotFound     = errors.New("store: não encontrado")
	ErrDuplicate    = errors.New("store: duplicado")
	ErrHashMismatch = errors.New("store: chave de idempotência reutilizada com corpo diferente")
)

type Store interface {
	AppendEvent(ctx context.Context, e *Event) error
	ListEvents(ctx context.Context, f EventFilter, p domain.Page) ([]Event, error)
	CountEvents(ctx context.Context, since time.Time) ([]EventCount, error)
	JobStats(ctx context.Context, since time.Time) ([]JobStat, error)
	PurgeEvents(ctx context.Context, before time.Time) (int64, error)
	// Listen bloqueia entregando novos eventos (de qualquer processo da plataforma) a fn.
	Listen(ctx context.Context, fn func(Event)) error

	// ReserveIdempotency cria um registro "em andamento". Se já existir, retorna o registro
	// existente e ErrDuplicate (ou ErrHashMismatch se o hash for diferente).
	ReserveIdempotency(ctx context.Context, rec IdempotencyRecord) (*IdempotencyRecord, error)
	CompleteIdempotency(ctx context.Context, scope, key string, status int, response []byte) error
	ReleaseIdempotency(ctx context.Context, scope, key string) error
	PurgeIdempotency(ctx context.Context, before time.Time) (int64, error)

	SaveInstance(ctx context.Context, r InstanceRecord) error
	FindInstancesByBusinessKey(ctx context.Context, businessKey string) ([]InstanceRecord, error)

	Ping(ctx context.Context) error
	Close()
}
