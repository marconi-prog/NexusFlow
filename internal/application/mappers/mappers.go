package mappers

import (
	"github.com/marconi-prog/nexusflow/internal/application/dto/response"
	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/application/services"
	"github.com/marconi-prog/nexusflow/internal/auth"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/domain/inventory"
	"github.com/marconi-prog/nexusflow/internal/domain/order"
)

// ---------- genéricos ----------

// MapSlice converte uma lista, sempre devolvendo slice não nulo (serializa como []).
func MapSlice[T, U any](in []T, fn func(T) U) []U {
	out := make([]U, 0, len(in))
	for _, it := range in {
		out = append(out, fn(it))
	}
	return out
}

func ToPage[T, U any](r *domain.Result[T], fn func(T) U) response.Page[U] {
	return response.Page[U]{Items: MapSlice(r.Items, fn), Total: r.Total}
}

func ToPrincipal(p auth.Principal) response.Principal {
	return response.Principal{ID: p.ID, Role: string(p.Role)}
}

// ---------- deployments / definições ----------

func ToDeployment(d *domain.Deployment) response.Deployment {
	res := response.Deployment{
		DeploymentKey: d.Key,
		TenantID:      d.TenantID,
		Processes:     MapSlice(d.Processes, toDeployedResource),
	}
	if len(d.Decisions) > 0 {
		res.Decisions = MapSlice(d.Decisions, toDeployedResource)
	}
	if len(d.Forms) > 0 {
		res.Forms = MapSlice(d.Forms, toDeployedResource)
	}
	return res
}

func toDeployedResource(d domain.DeployedDefinition) response.DeployedResource {
	return response.DeployedResource{ID: d.ID, Key: d.Key, Version: d.Version, ResourceName: d.ResourceName}
}

func ToProcessDefinition(d domain.ProcessDefinition) response.ProcessDefinition {
	return response.ProcessDefinition{
		ProcessDefinitionKey: d.Key,
		ProcessDefinitionID:  d.ID,
		Name:                 d.Name,
		Version:              d.Version,
		VersionTag:           d.VersionTag,
		ResourceName:         d.ResourceName,
		TenantID:             d.TenantID,
	}
}

// ---------- instâncias ----------

func ToStartedInstance(i *domain.StartedInstance) response.StartedInstance {
	return response.StartedInstance{
		ProcessInstanceKey:       i.ProcessInstanceKey,
		ProcessDefinitionKey:     i.ProcessDefinitionKey,
		ProcessDefinitionID:      i.ProcessDefinitionID,
		ProcessDefinitionVersion: i.ProcessDefinitionVersion,
		TenantID:                 i.TenantID,
		Variables:                i.Variables,
	}
}

func ToProcessInstance(i domain.ProcessInstance) response.ProcessInstance {
	return response.ProcessInstance{
		ProcessInstanceKey:       i.Key,
		ProcessDefinitionKey:     i.ProcessDefinitionKey,
		ProcessDefinitionID:      i.ProcessDefinitionID,
		ProcessDefinitionName:    i.ProcessDefinitionName,
		ProcessDefinitionVersion: i.ProcessDefinitionVersion,
		State:                    string(i.State),
		StartDate:                i.StartDate,
		EndDate:                  i.EndDate,
		HasIncident:              i.HasIncident,
		ParentProcessInstanceKey: i.ParentInstanceKey,
		TenantID:                 i.TenantID,
	}
}

func ToInstanceDetail(d *services.InstanceDetail) response.InstanceDetail {
	return response.InstanceDetail{
		Instance:  ToProcessInstance(*d.Instance),
		Incidents: MapSlice(d.Incidents, ToIncident),
		UserTasks: MapSlice(d.UserTasks, ToUserTask),
		Timeline:  MapSlice(d.Timeline, ToEvent),
	}
}

func ToInstanceRecord(r ports.InstanceRecord) response.InstanceRecord {
	return response.InstanceRecord{
		ProcessInstanceKey:  r.ProcessInstanceKey,
		BusinessKey:         r.BusinessKey,
		ProcessDefinitionID: r.ProcessDefinitionID,
		TenantID:            r.TenantID,
		StartedBy:           r.StartedBy,
		StartedAt:           r.StartedAt,
	}
}

func ToVariable(v domain.Variable) response.Variable {
	return response.Variable{
		Name: v.Name, Value: v.Value, ScopeKey: v.ScopeKey,
		ProcessInstanceKey: v.ProcessInstanceKey, Truncated: v.Truncated,
	}
}

// ---------- mensagens / sinais ----------

func ToMessage(m *domain.MessageResult) response.Message {
	return response.Message{MessageKey: m.MessageKey, ProcessInstanceKey: m.ProcessInstanceKey, TenantID: m.TenantID}
}

func ToSignal(s *domain.SignalResult) response.Signal {
	return response.Signal{SignalKey: s.SignalKey, TenantID: s.TenantID}
}

// ---------- incidentes ----------

func ToIncident(i domain.Incident) response.Incident {
	return response.Incident{
		IncidentKey:          i.Key,
		ProcessInstanceKey:   i.ProcessInstanceKey,
		ProcessDefinitionKey: i.ProcessDefinitionKey,
		ProcessDefinitionID:  i.ProcessDefinitionID,
		ErrorType:            i.ErrorType,
		ErrorMessage:         i.ErrorMessage,
		ElementID:            i.ElementID,
		ElementInstanceKey:   i.ElementInstanceKey,
		JobKey:               i.JobKey,
		State:                i.State,
		CreationTime:         i.CreationTime,
		TenantID:             i.TenantID,
	}
}

// ---------- tarefas humanas ----------

func ToUserTask(t domain.UserTask) response.UserTask {
	return response.UserTask{
		UserTaskKey:         t.Key,
		Name:                t.Name,
		State:               t.State,
		Assignee:            t.Assignee,
		CandidateGroups:     t.CandidateGroups,
		CandidateUsers:      t.CandidateUsers,
		ElementID:           t.ElementID,
		ProcessInstanceKey:  t.ProcessInstanceKey,
		ProcessDefinitionID: t.ProcessDefinitionID,
		FormKey:             t.FormKey,
		CreationDate:        t.CreationDate,
		DueDate:             t.DueDate,
		Priority:            t.Priority,
		TenantID:            t.TenantID,
	}
}

// ---------- monitoramento ----------

func ToEvent(e ports.Event) response.Event {
	return response.Event{
		ID:                  e.ID,
		Type:                e.Type,
		Source:              e.Source,
		Actor:               e.Actor,
		OccurredAt:          e.OccurredAt,
		ProcessInstanceKey:  e.ProcessInstanceKey,
		ProcessDefinitionID: e.ProcessDefinitionID,
		JobKey:              e.JobKey,
		JobType:             e.JobType,
		IncidentKey:         e.IncidentKey,
		DurationMs:          e.DurationMs,
		Data:                e.Data,
	}
}

func ToEventCount(c ports.EventCount) response.EventCount {
	return response.EventCount{Type: c.Type, Count: c.Count}
}

func ToJobStat(s ports.JobStat) response.JobStat {
	return response.JobStat{
		JobType: s.JobType, Completed: s.Completed, Failed: s.Failed, ErrorsThrown: s.ErrorsThrown,
		AvgDurationMs: s.AvgDurationMs, P95DurationMs: s.P95DurationMs,
	}
}

func ToTopology(t *domain.Topology) response.Topology {
	return response.Topology{
		ClusterSize:       t.ClusterSize,
		PartitionsCount:   t.PartitionsCount,
		ReplicationFactor: t.ReplicationFactor,
		GatewayVersion:    t.GatewayVersion,
		Brokers: MapSlice(t.Brokers, func(b domain.Broker) response.Broker {
			return response.Broker{
				NodeID: b.NodeID, Host: b.Host, Port: b.Port, Version: b.Version,
				Partitions: MapSlice(b.Partitions, func(p domain.Partition) response.Partition {
					return response.Partition{PartitionID: p.PartitionID, Role: p.Role, Health: p.Health}
				}),
			}
		}),
	}
}

func ToOverview(o *services.Overview) response.Overview {
	res := response.Overview{
		GeneratedAt:           o.GeneratedAt,
		Window:                o.Window,
		EngineError:           o.EngineError,
		ActiveInstances:       o.ActiveInstances,
		InstancesWithIncident: o.InstancesWithInc,
		ActiveIncidents:       o.ActiveIncidents,
		OpenUserTasks:         o.OpenUserTasks,
		Events:                MapSlice(o.Events, ToEventCount),
		Jobs:                  MapSlice(o.Jobs, ToJobStat),
		Partial:               o.Partial,
		Errors:                o.Errors,
	}
	if o.Engine != nil {
		t := ToTopology(o.Engine)
		res.Engine = &t
	}
	return res
}

func ToHealth(h services.Health) response.Health {
	res := response.Health{Status: h.Status, Components: make(map[string]response.ComponentHealth, len(h.Components))}
	for name, c := range h.Components {
		res.Components[name] = response.ComponentHealth{Status: c.Status, Detail: c.Detail, Latency: c.Latency}
	}
	return res
}

// ---------- pedidos (saga) ----------

func cents(v int64) float64 { return float64(v) / 100 }

func ToPlacedOrder(p *services.PlacedOrder) response.PlacedOrder {
	return response.PlacedOrder{OrderID: p.OrderID, ProcessInstanceKey: p.ProcessInstanceKey}
}

func ToOrder(o order.Order) response.Order {
	return response.Order{
		OrderID:  o.ID,
		Customer: o.Customer,
		Items: MapSlice(o.Items, func(it order.Item) response.OrderItem {
			return response.OrderItem{SKU: it.SKU, Quantity: it.Quantity, UnitPrice: cents(it.UnitPriceCents)}
		}),
		Total:              cents(o.TotalCents),
		Status:             string(o.Status),
		RejectionReason:    o.RejectionReason,
		ProcessInstanceKey: o.ProcessInstanceKey,
		CreatedAt:          o.CreatedAt,
		UpdatedAt:          o.UpdatedAt,
	}
}

func ToOrderDetail(d *services.OrderDetail) response.OrderDetail {
	res := response.OrderDetail{Order: ToOrder(*d.Order)}
	if p := d.Payment; p != nil {
		res.Payment = &response.Payment{
			PaymentID: p.ID, Amount: cents(p.AmountCents), Status: string(p.Status),
			TransactionID: p.TransactionID, DeclineReason: p.DeclineReason, UpdatedAt: p.UpdatedAt,
		}
	}
	if r := d.Reservation; r != nil {
		res.Reservation = &response.Reservation{
			Status: string(r.Status),
			Lines: MapSlice(r.Lines, func(l inventory.Line) response.ReservationLine {
				return response.ReservationLine{SKU: l.SKU, Quantity: l.Quantity}
			}),
		}
	}
	return res
}

func ToInventoryItem(it inventory.Item) response.InventoryItem {
	return response.InventoryItem{SKU: it.SKU, Name: it.Name, Available: it.Available}
}
