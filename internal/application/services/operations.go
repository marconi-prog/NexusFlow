package services

import (
	"context"
	"log/slog"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/auth"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/observability"
)

// Incidents trata incidentes (falhas que pararam um fluxo e exigem intervenção).
type Incidents struct {
	engine ports.Engine
	audit  *observability.Recorder
	logger *slog.Logger
}

func NewIncidents(e ports.Engine, a *observability.Recorder, l *slog.Logger) *Incidents {
	return &Incidents{engine: e, audit: a, logger: l}
}

func (s *Incidents) Search(ctx context.Context, f domain.IncidentFilter, p domain.Page) (*domain.Result[domain.Incident], error) {
	return s.engine.SearchIncidents(ctx, f, p)
}

func (s *Incidents) Get(ctx context.Context, key string) (*domain.Incident, error) {
	return s.engine.GetIncident(ctx, key)
}

// Resolve resolve o incidente. Para incidentes de job sem retentativas, devolve retentativas
// ao job antes (sem isso o motor recriaria o incidente imediatamente).
func (s *Incidents) Resolve(ctx context.Context, key string, retries int) (*domain.Incident, error) {
	inc, err := s.engine.GetIncident(ctx, key)
	if err != nil {
		return nil, err
	}
	if inc.State != "" && inc.State != "ACTIVE" {
		return nil, domain.Conflict("incidente não está ativo (estado: " + inc.State + ")")
	}
	if retries <= 0 {
		retries = 1
	}
	if inc.JobKey != "" && (inc.ErrorType == "JOB_NO_RETRIES" || inc.ErrorType == "") {
		if err := s.engine.UpdateJobRetries(ctx, inc.JobKey, retries); err != nil {
			return nil, err
		}
		s.audit.Record(ctx, ports.Event{
			Type: ports.EventJobRetriesUpdated, Actor: auth.ActorID(ctx), JobKey: inc.JobKey,
			ProcessInstanceKey: inc.ProcessInstanceKey, ProcessDefinitionID: inc.ProcessDefinitionID,
			Data: map[string]any{"retries": retries, "incidentKey": key},
		})
	}
	if err := s.engine.ResolveIncident(ctx, key); err != nil {
		return nil, err
	}
	s.audit.Record(ctx, ports.Event{
		Type: ports.EventIncidentResolved, Actor: auth.ActorID(ctx), IncidentKey: key,
		ProcessInstanceKey: inc.ProcessInstanceKey, ProcessDefinitionID: inc.ProcessDefinitionID,
		Data: map[string]any{"errorType": inc.ErrorType, "elementId": inc.ElementID},
	})
	return inc, nil
}

// Tasks trata tarefas humanas (user tasks).
type Tasks struct {
	engine ports.Engine
	audit  *observability.Recorder
}

func NewTasks(e ports.Engine, a *observability.Recorder) *Tasks { return &Tasks{engine: e, audit: a} }

func (s *Tasks) Search(ctx context.Context, f domain.UserTaskFilter, p domain.Page) (*domain.Result[domain.UserTask], error) {
	return s.engine.SearchUserTasks(ctx, f, p)
}

func (s *Tasks) Get(ctx context.Context, key string) (*domain.UserTask, error) {
	return s.engine.GetUserTask(ctx, key)
}

func (s *Tasks) Assign(ctx context.Context, key, assignee string, allowOverride bool) error {
	if assignee == "" {
		return domain.Invalid("assignee é obrigatório")
	}
	if err := s.engine.AssignUserTask(ctx, key, assignee, allowOverride); err != nil {
		return err
	}
	s.record(ctx, ports.EventUserTaskAssigned, key, map[string]any{"assignee": assignee})
	return nil
}

func (s *Tasks) Unassign(ctx context.Context, key string) error {
	if err := s.engine.UnassignUserTask(ctx, key); err != nil {
		return err
	}
	s.record(ctx, ports.EventUserTaskUnassigned, key, nil)
	return nil
}

func (s *Tasks) Complete(ctx context.Context, key string, vars domain.Variables, action string) error {
	if err := s.engine.CompleteUserTask(ctx, key, vars, action); err != nil {
		return err
	}
	s.record(ctx, ports.EventUserTaskCompleted, key, map[string]any{"action": action})
	return nil
}

func (s *Tasks) record(ctx context.Context, typ, key string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	data["userTaskKey"] = key
	e := ports.Event{Type: typ, Actor: auth.ActorID(ctx), Data: data}
	// enriquece com a instância quando possível (melhor esforço)
	if t, err := s.engine.GetUserTask(ctx, key); err == nil {
		e.ProcessInstanceKey = t.ProcessInstanceKey
		e.ProcessDefinitionID = t.ProcessDefinitionID
	}
	s.audit.Record(ctx, e)
}
