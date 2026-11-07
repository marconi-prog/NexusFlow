// Package engine define a porta (interface) entre a plataforma e o motor de orquestração.
// A implementação padrão é o adapter para a Orchestration Cluster REST API do Camunda 8
// (pacote engine/camunda); outras implementações (fakes de teste) satisfazem a mesma interface.
package ports

import (
	"context"

	"github.com/marconi-prog/nexusflow/internal/domain"
)

type Engine interface {
	Topology(ctx context.Context) (*domain.Topology, error)

	Deploy(ctx context.Context, tenantID string, resources []domain.Resource) (*domain.Deployment, error)
	SearchProcessDefinitions(ctx context.Context, f domain.ProcessDefinitionFilter, p domain.Page) (*domain.Result[domain.ProcessDefinition], error)
	GetProcessDefinition(ctx context.Context, key string) (*domain.ProcessDefinition, error)
	GetProcessDefinitionXML(ctx context.Context, key string) (string, error)

	StartInstance(ctx context.Context, cmd domain.StartInstanceCommand) (*domain.StartedInstance, error)
	CancelInstance(ctx context.Context, key string) error
	GetInstance(ctx context.Context, key string) (*domain.ProcessInstance, error)
	SearchInstances(ctx context.Context, f domain.ProcessInstanceFilter, p domain.Page, s []domain.Sort) (*domain.Result[domain.ProcessInstance], error)
	SearchVariables(ctx context.Context, processInstanceKey string, p domain.Page) (*domain.Result[domain.Variable], error)
	// SetVariables cria/atualiza variáveis no escopo informado (a chave da instância = escopo global).
	SetVariables(ctx context.Context, elementInstanceKey string, vars domain.Variables, local bool) error

	PublishMessage(ctx context.Context, cmd domain.PublishMessageCommand) (*domain.MessageResult, error)
	BroadcastSignal(ctx context.Context, cmd domain.BroadcastSignalCommand) (*domain.SignalResult, error)

	SearchIncidents(ctx context.Context, f domain.IncidentFilter, p domain.Page) (*domain.Result[domain.Incident], error)
	GetIncident(ctx context.Context, key string) (*domain.Incident, error)
	ResolveIncident(ctx context.Context, key string) error
	UpdateJobRetries(ctx context.Context, jobKey string, retries int) error

	ActivateJobs(ctx context.Context, cmd domain.ActivateJobsCommand) ([]domain.Job, error)
	CompleteJob(ctx context.Context, jobKey string, vars domain.Variables) error
	FailJob(ctx context.Context, jobKey string, cmd domain.FailJobCommand) error
	ThrowError(ctx context.Context, jobKey string, cmd domain.ThrowErrorCommand) error

	SearchUserTasks(ctx context.Context, f domain.UserTaskFilter, p domain.Page) (*domain.Result[domain.UserTask], error)
	GetUserTask(ctx context.Context, key string) (*domain.UserTask, error)
	AssignUserTask(ctx context.Context, key, assignee string, allowOverride bool) error
	UnassignUserTask(ctx context.Context, key string) error
	CompleteUserTask(ctx context.Context, key string, vars domain.Variables, action string) error
}
