// Package fakes fornece um ports.Engine em memória para testes.
package fakes

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/domain"
)

var _ ports.Engine = (*Fake)(nil)

// Call registra uma chamada de comando feita ao fake.
type Call struct {
	Op   string
	Key  string
	Args any
}

type Fake struct {
	mu        sync.Mutex
	seq       atomic.Int64
	Calls     []Call
	Instances map[string]*domain.ProcessInstance
	Incidents map[string]*domain.Incident
	Tasks     map[string]*domain.UserTask
	// JobQueue é consumida por ActivateJobs, por tipo.
	JobQueue map[string][]domain.Job
	// Err, se definido, é retornado por todas as operações.
	Err error
	// TopologyErr afeta apenas Topology.
	TopologyErr error
}

func New() *Fake {
	f := &Fake{
		Instances: map[string]*domain.ProcessInstance{},
		Incidents: map[string]*domain.Incident{},
		Tasks:     map[string]*domain.UserTask{},
		JobQueue:  map[string][]domain.Job{},
	}
	f.seq.Store(2251799813685000)
	return f
}

func (f *Fake) next() string { return strconv.FormatInt(f.seq.Add(1), 10) }

func (f *Fake) record(op, key string, args any) {
	f.mu.Lock()
	f.Calls = append(f.Calls, Call{Op: op, Key: key, Args: args})
	f.mu.Unlock()
}

// CallsTo devolve as chamadas a uma operação.
func (f *Fake) CallsTo(op string) []Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Call
	for _, c := range f.Calls {
		if c.Op == op {
			out = append(out, c)
		}
	}
	return out
}

func (f *Fake) Topology(context.Context) (*domain.Topology, error) {
	if f.TopologyErr != nil {
		return nil, f.TopologyErr
	}
	if f.Err != nil {
		return nil, f.Err
	}
	return &domain.Topology{ClusterSize: 1, PartitionsCount: 1, ReplicationFactor: 1, GatewayVersion: "fake"}, nil
}

func (f *Fake) Deploy(_ context.Context, tenant string, rs []domain.Resource) (*domain.Deployment, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	d := &domain.Deployment{Key: f.next(), TenantID: tenant}
	for _, r := range rs {
		d.Processes = append(d.Processes, domain.DeployedDefinition{ID: r.Name, Key: f.next(), Version: 1, ResourceName: r.Name})
	}
	f.record("Deploy", d.Key, rs)
	return d, nil
}

func (f *Fake) SearchProcessDefinitions(context.Context, domain.ProcessDefinitionFilter, domain.Page) (*domain.Result[domain.ProcessDefinition], error) {
	if f.Err != nil {
		return nil, f.Err
	}
	return &domain.Result[domain.ProcessDefinition]{Items: []domain.ProcessDefinition{}}, nil
}

func (f *Fake) GetProcessDefinition(_ context.Context, key string) (*domain.ProcessDefinition, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	return nil, domain.NotFound("definição não encontrada")
}

func (f *Fake) GetProcessDefinitionXML(context.Context, string) (string, error) {
	return "", domain.NotFound("definição não encontrada")
}

func (f *Fake) StartInstance(_ context.Context, cmd domain.StartInstanceCommand) (*domain.StartedInstance, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	key := f.next()
	id := cmd.ProcessDefinitionID
	if id == "" {
		id = "process-" + cmd.ProcessDefinitionKey
	}
	f.mu.Lock()
	f.Instances[key] = &domain.ProcessInstance{Key: key, ProcessDefinitionID: id, ProcessDefinitionKey: "1", ProcessDefinitionVersion: 1, State: domain.InstanceActive}
	f.mu.Unlock()
	f.record("StartInstance", key, cmd)
	return &domain.StartedInstance{ProcessInstanceKey: key, ProcessDefinitionID: id, ProcessDefinitionKey: "1", ProcessDefinitionVersion: 1, Variables: cmd.Variables}, nil
}

func (f *Fake) CancelInstance(_ context.Context, key string) error {
	if f.Err != nil {
		return f.Err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	inst, ok := f.Instances[key]
	if !ok {
		return domain.NotFound("instância não encontrada")
	}
	inst.State = domain.InstanceTerminated
	f.Calls = append(f.Calls, Call{Op: "CancelInstance", Key: key})
	return nil
}

func (f *Fake) GetInstance(_ context.Context, key string) (*domain.ProcessInstance, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	inst, ok := f.Instances[key]
	if !ok {
		return nil, domain.NotFound("instância não encontrada")
	}
	cp := *inst
	return &cp, nil
}

func (f *Fake) SearchInstances(_ context.Context, flt domain.ProcessInstanceFilter, _ domain.Page, _ []domain.Sort) (*domain.Result[domain.ProcessInstance], error) {
	if f.Err != nil {
		return nil, f.Err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := &domain.Result[domain.ProcessInstance]{Items: []domain.ProcessInstance{}}
	for _, i := range f.Instances {
		if flt.State != "" && i.State != flt.State {
			continue
		}
		if flt.HasIncident != nil && i.HasIncident != *flt.HasIncident {
			continue
		}
		out.Items = append(out.Items, *i)
	}
	out.Total = len(out.Items)
	return out, nil
}

func (f *Fake) SearchVariables(context.Context, string, domain.Page) (*domain.Result[domain.Variable], error) {
	return &domain.Result[domain.Variable]{Items: []domain.Variable{}}, f.Err
}

func (f *Fake) SetVariables(_ context.Context, key string, vars domain.Variables, local bool) error {
	if f.Err != nil {
		return f.Err
	}
	f.record("SetVariables", key, vars)
	return nil
}

func (f *Fake) PublishMessage(_ context.Context, cmd domain.PublishMessageCommand) (*domain.MessageResult, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	f.record("PublishMessage", "", cmd)
	return &domain.MessageResult{MessageKey: f.next()}, nil
}

func (f *Fake) BroadcastSignal(_ context.Context, cmd domain.BroadcastSignalCommand) (*domain.SignalResult, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	f.record("BroadcastSignal", "", cmd)
	return &domain.SignalResult{SignalKey: f.next()}, nil
}

func (f *Fake) SearchIncidents(_ context.Context, flt domain.IncidentFilter, _ domain.Page) (*domain.Result[domain.Incident], error) {
	if f.Err != nil {
		return nil, f.Err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := &domain.Result[domain.Incident]{Items: []domain.Incident{}}
	for _, i := range f.Incidents {
		if flt.State != "" && i.State != flt.State {
			continue
		}
		if flt.ProcessInstanceKey != "" && i.ProcessInstanceKey != flt.ProcessInstanceKey {
			continue
		}
		out.Items = append(out.Items, *i)
	}
	out.Total = len(out.Items)
	return out, nil
}

func (f *Fake) GetIncident(_ context.Context, key string) (*domain.Incident, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	i, ok := f.Incidents[key]
	if !ok {
		return nil, domain.NotFound("incidente não encontrado")
	}
	cp := *i
	return &cp, nil
}

func (f *Fake) ResolveIncident(_ context.Context, key string) error {
	if f.Err != nil {
		return f.Err
	}
	f.mu.Lock()
	if i, ok := f.Incidents[key]; ok {
		i.State = "RESOLVED"
	}
	f.Calls = append(f.Calls, Call{Op: "ResolveIncident", Key: key})
	f.mu.Unlock()
	return nil
}

func (f *Fake) UpdateJobRetries(_ context.Context, jobKey string, retries int) error {
	if f.Err != nil {
		return f.Err
	}
	f.record("UpdateJobRetries", jobKey, retries)
	return nil
}

func (f *Fake) ActivateJobs(_ context.Context, cmd domain.ActivateJobsCommand) ([]domain.Job, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	q := f.JobQueue[cmd.Type]
	n := min(len(q), cmd.MaxJobs)
	jobs := append([]domain.Job(nil), q[:n]...)
	f.JobQueue[cmd.Type] = q[n:]
	return jobs, nil
}

func (f *Fake) CompleteJob(_ context.Context, jobKey string, vars domain.Variables) error {
	f.record("CompleteJob", jobKey, vars)
	return f.Err
}

func (f *Fake) FailJob(_ context.Context, jobKey string, cmd domain.FailJobCommand) error {
	f.record("FailJob", jobKey, cmd)
	return f.Err
}

func (f *Fake) ThrowError(_ context.Context, jobKey string, cmd domain.ThrowErrorCommand) error {
	f.record("ThrowError", jobKey, cmd)
	return f.Err
}

func (f *Fake) SearchUserTasks(_ context.Context, flt domain.UserTaskFilter, _ domain.Page) (*domain.Result[domain.UserTask], error) {
	if f.Err != nil {
		return nil, f.Err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := &domain.Result[domain.UserTask]{Items: []domain.UserTask{}}
	for _, t := range f.Tasks {
		if flt.State != "" && t.State != flt.State {
			continue
		}
		if flt.ProcessInstanceKey != "" && t.ProcessInstanceKey != flt.ProcessInstanceKey {
			continue
		}
		out.Items = append(out.Items, *t)
	}
	out.Total = len(out.Items)
	return out, nil
}

func (f *Fake) GetUserTask(_ context.Context, key string) (*domain.UserTask, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.Tasks[key]
	if !ok {
		return nil, domain.NotFound("tarefa não encontrada")
	}
	cp := *t
	return &cp, nil
}

func (f *Fake) AssignUserTask(_ context.Context, key, assignee string, _ bool) error {
	f.record("AssignUserTask", key, assignee)
	return f.Err
}

func (f *Fake) UnassignUserTask(_ context.Context, key string) error {
	f.record("UnassignUserTask", key, nil)
	return f.Err
}

func (f *Fake) CompleteUserTask(_ context.Context, key string, vars domain.Variables, _ string) error {
	f.record("CompleteUserTask", key, vars)
	return f.Err
}
