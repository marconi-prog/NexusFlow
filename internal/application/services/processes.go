// Package service contém os casos de uso da plataforma, orquestrando motor, persistência e auditoria.
package services

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/auth"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/observability"
)

const MaxResourceSize = 10 << 20 // 10 MiB por arquivo

var allowedResourceExt = map[string]bool{".bpmn": true, ".dmn": true, ".form": true}

type Processes struct {
	engine ports.Engine
	store  ports.Store
	audit  *observability.Recorder
	logger *slog.Logger
}

func NewProcesses(e ports.Engine, s ports.Store, a *observability.Recorder, l *slog.Logger) *Processes {
	return &Processes{engine: e, store: s, audit: a, logger: l}
}

func (s *Processes) Deploy(ctx context.Context, tenantID string, resources []domain.Resource) (*domain.Deployment, error) {
	if len(resources) == 0 {
		return nil, domain.Invalid("envie ao menos um arquivo .bpmn, .dmn ou .form")
	}
	for _, r := range resources {
		ext := strings.ToLower(filepath.Ext(r.Name))
		if !allowedResourceExt[ext] {
			return nil, domain.Invalid("extensão não suportada: " + r.Name)
		}
		if len(r.Content) == 0 {
			return nil, domain.Invalid("arquivo vazio: " + r.Name)
		}
		if len(r.Content) > MaxResourceSize {
			return nil, domain.Invalid("arquivo excede 10 MiB: " + r.Name)
		}
	}
	d, err := s.engine.Deploy(ctx, tenantID, resources)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(resources))
	for _, r := range resources {
		names = append(names, r.Name)
	}
	for _, pd := range d.Processes {
		s.audit.Record(ctx, ports.Event{
			Type: ports.EventDeploymentCreated, Actor: auth.ActorID(ctx), ProcessDefinitionID: pd.ID,
			Data: map[string]any{"deploymentKey": d.Key, "processDefinitionKey": pd.Key, "version": pd.Version, "resources": names},
		})
	}
	if len(d.Processes) == 0 {
		s.audit.Record(ctx, ports.Event{
			Type: ports.EventDeploymentCreated, Actor: auth.ActorID(ctx),
			Data: map[string]any{"deploymentKey": d.Key, "resources": names},
		})
	}
	return d, nil
}

func (s *Processes) SearchDefinitions(ctx context.Context, f domain.ProcessDefinitionFilter, p domain.Page) (*domain.Result[domain.ProcessDefinition], error) {
	return s.engine.SearchProcessDefinitions(ctx, f, p)
}

func (s *Processes) GetDefinition(ctx context.Context, key string) (*domain.ProcessDefinition, error) {
	return s.engine.GetProcessDefinition(ctx, key)
}

func (s *Processes) GetDefinitionXML(ctx context.Context, key string) (string, error) {
	return s.engine.GetProcessDefinitionXML(ctx, key)
}

func (s *Processes) Start(ctx context.Context, cmd domain.StartInstanceCommand) (*domain.StartedInstance, error) {
	if err := cmd.Validate(); err != nil {
		return nil, err
	}
	if len(cmd.BusinessKey) > 256 {
		return nil, domain.Invalid("businessKey excede 256 caracteres")
	}
	if cmd.BusinessKey != "" {
		// a business key também fica disponível ao processo como variável
		if cmd.Variables == nil {
			cmd.Variables = domain.Variables{}
		}
		if _, exists := cmd.Variables["businessKey"]; !exists {
			cmd.Variables["businessKey"] = cmd.BusinessKey
		}
	}

	inst, err := s.engine.StartInstance(ctx, cmd)
	if err != nil {
		return nil, err
	}
	observability.InstancesStarted.WithLabelValues(inst.ProcessDefinitionID).Inc()

	rec := ports.InstanceRecord{
		ProcessInstanceKey: inst.ProcessInstanceKey, BusinessKey: cmd.BusinessKey,
		ProcessDefinitionID: inst.ProcessDefinitionID, TenantID: inst.TenantID, StartedBy: auth.ActorID(ctx),
	}
	if err := s.store.SaveInstance(context.WithoutCancel(ctx), rec); err != nil {
		s.logger.Error("falha ao registrar instância", "processInstanceKey", inst.ProcessInstanceKey, "error", err)
	}
	s.audit.Record(ctx, ports.Event{
		Type: ports.EventInstanceStarted, Actor: auth.ActorID(ctx),
		ProcessInstanceKey: inst.ProcessInstanceKey, ProcessDefinitionID: inst.ProcessDefinitionID,
		Data: map[string]any{
			"processDefinitionKey": inst.ProcessDefinitionKey, "version": inst.ProcessDefinitionVersion,
			"businessKey": cmd.BusinessKey, "awaitCompletion": cmd.AwaitCompletion,
		},
	})
	return inst, nil
}

func (s *Processes) Cancel(ctx context.Context, key, reason string) error {
	inst, err := s.engine.GetInstance(ctx, key)
	if err != nil && domain.KindOf(err) != domain.KindNotFound {
		return err
	}
	// se a instância ainda não foi exportada para a busca, seguimos e deixamos o motor decidir
	if inst != nil && inst.State != domain.InstanceActive {
		return domain.Conflict("a instância não está ativa (estado: " + string(inst.State) + ")")
	}
	if err := s.engine.CancelInstance(ctx, key); err != nil {
		return err
	}
	observability.InstancesCancelled.Inc()
	ev := ports.Event{Type: ports.EventInstanceCancelled, Actor: auth.ActorID(ctx), ProcessInstanceKey: key, Data: map[string]any{"reason": reason}}
	if inst != nil {
		ev.ProcessDefinitionID = inst.ProcessDefinitionID
	}
	s.audit.Record(ctx, ev)
	return nil
}

// SetVariables altera variáveis globais da instância. Uso típico: corrigir o dado que
// causou um incidente antes de resolvê-lo.
func (s *Processes) SetVariables(ctx context.Context, key string, vars domain.Variables) error {
	if len(vars) == 0 {
		return domain.Invalid("variables não pode ser vazio")
	}
	if err := s.engine.SetVariables(ctx, key, vars, false); err != nil {
		return err
	}
	names := make([]string, 0, len(vars))
	for k := range vars {
		names = append(names, k)
	}
	s.audit.Record(ctx, ports.Event{
		Type: ports.EventVariablesUpdated, Actor: auth.ActorID(ctx), ProcessInstanceKey: key,
		Data: map[string]any{"variables": names},
	})
	return nil
}

// InstanceDetail agrega a visão completa de uma instância para monitoramento.
type InstanceDetail struct {
	Instance  *domain.ProcessInstance
	Incidents []domain.Incident
	UserTasks []domain.UserTask
	Timeline  []ports.Event
}

func (s *Processes) Get(ctx context.Context, key string) (*InstanceDetail, error) {
	inst, err := s.engine.GetInstance(ctx, key)
	if err != nil {
		return nil, err
	}
	detail := &InstanceDetail{Instance: inst, Incidents: []domain.Incident{}, UserTasks: []domain.UserTask{}}

	if inst.HasIncident {
		inc, err := s.engine.SearchIncidents(ctx, domain.IncidentFilter{ProcessInstanceKey: key, State: "ACTIVE"}, domain.Page{Limit: 100})
		if err != nil {
			s.logger.Warn("falha ao buscar incidentes da instância", "key", key, "error", err)
		} else {
			detail.Incidents = inc.Items
		}
	}
	if inst.State == domain.InstanceActive {
		tasks, err := s.engine.SearchUserTasks(ctx, domain.UserTaskFilter{ProcessInstanceKey: key, State: "CREATED"}, domain.Page{Limit: 100})
		if err != nil {
			s.logger.Warn("falha ao buscar tarefas da instância", "key", key, "error", err)
		} else {
			detail.UserTasks = tasks.Items
		}
	}
	tl, err := s.store.ListEvents(ctx, ports.EventFilter{ProcessInstanceKey: key}, domain.Page{Limit: 200})
	if err != nil {
		s.logger.Warn("falha ao buscar timeline", "key", key, "error", err)
		tl = []ports.Event{}
	}
	detail.Timeline = tl
	return detail, nil
}

func (s *Processes) Search(ctx context.Context, f domain.ProcessInstanceFilter, p domain.Page, sort []domain.Sort) (*domain.Result[domain.ProcessInstance], error) {
	if f.State != "" && !f.State.Valid() {
		return nil, domain.Invalid("state inválido (ACTIVE, COMPLETED, TERMINATED)")
	}
	if len(sort) == 0 {
		sort = []domain.Sort{{Field: "startDate", Order: "DESC"}}
	}
	return s.engine.SearchInstances(ctx, f, p, sort)
}

func (s *Processes) Variables(ctx context.Context, key string, p domain.Page) (*domain.Result[domain.Variable], error) {
	return s.engine.SearchVariables(ctx, key, p)
}

func (s *Processes) FindByBusinessKey(ctx context.Context, bk string) ([]ports.InstanceRecord, error) {
	if bk == "" {
		return nil, domain.Invalid("businessKey é obrigatório")
	}
	return s.store.FindInstancesByBusinessKey(ctx, bk)
}

func (s *Processes) PublishMessage(ctx context.Context, cmd domain.PublishMessageCommand) (*domain.MessageResult, error) {
	if err := cmd.Validate(); err != nil {
		return nil, err
	}
	res, err := s.engine.PublishMessage(ctx, cmd)
	if err != nil {
		return nil, err
	}
	observability.MessagesPublished.WithLabelValues(cmd.Name).Inc()
	s.audit.Record(ctx, ports.Event{
		Type: ports.EventMessagePublished, Actor: auth.ActorID(ctx), ProcessInstanceKey: res.ProcessInstanceKey,
		Data: map[string]any{"name": cmd.Name, "correlationKey": cmd.CorrelationKey, "messageKey": res.MessageKey, "correlate": cmd.Correlate},
	})
	return res, nil
}

func (s *Processes) BroadcastSignal(ctx context.Context, cmd domain.BroadcastSignalCommand) (*domain.SignalResult, error) {
	if cmd.Name == "" {
		return nil, domain.Invalid("signalName é obrigatório")
	}
	res, err := s.engine.BroadcastSignal(ctx, cmd)
	if err != nil {
		return nil, err
	}
	s.audit.Record(ctx, ports.Event{
		Type: ports.EventSignalBroadcast, Actor: auth.ActorID(ctx),
		Data: map[string]any{"signalName": cmd.Name, "signalKey": res.SignalKey},
	})
	return res, nil
}
