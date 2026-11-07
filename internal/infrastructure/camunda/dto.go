package camunda

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"

	"github.com/marconi-prog/nexusflow/internal/domain"
)

// flexKey aceita chaves como string (8.8+) ou número (versões anteriores).
type flexKey string

func (k *flexKey) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || string(b) == "null" {
		*k = ""
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*k = flexKey(s)
		return nil
	}
	*k = flexKey(string(b))
	return nil
}

// flexTime aceita os formatos de data usados pelas diferentes versões da API.
type flexTime struct{ t *time.Time }

var timeLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.000-0700",
	"2006-01-02T15:04:05-0700",
	"2006-01-02T15:04:05.000Z0700",
}

func (f *flexTime) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil || s == "" {
		return nil
	}
	for _, l := range timeLayouts {
		if t, err := time.Parse(l, s); err == nil {
			f.t = &t
			return nil
		}
	}
	return nil
}

type searchResponse[T any] struct {
	Items []T `json:"items"`
	Page  struct {
		TotalItems int `json:"totalItems"`
	} `json:"page"`
}

func mapResult[T, U any](in searchResponse[T], fn func(T) U) *domain.Result[U] {
	out := &domain.Result[U]{Items: make([]U, 0, len(in.Items)), Total: in.Page.TotalItems}
	for _, it := range in.Items {
		out.Items = append(out.Items, fn(it))
	}
	return out
}

// ---------- deployment ----------

type deployResponse struct {
	DeploymentKey flexKey `json:"deploymentKey"`
	TenantID      string  `json:"tenantId"`
	Deployments   []struct {
		ProcessDefinition *struct {
			ID           string  `json:"processDefinitionId"`
			Version      int     `json:"processDefinitionVersion"`
			ResourceName string  `json:"resourceName"`
			Key          flexKey `json:"processDefinitionKey"`
		} `json:"processDefinition"`
		DecisionDefinition *struct {
			ID           string  `json:"decisionDefinitionId"`
			Version      int     `json:"version"`
			ResourceName string  `json:"resourceName"`
			Key          flexKey `json:"decisionDefinitionKey"`
		} `json:"decisionDefinition"`
		Form *struct {
			ID           string  `json:"formId"`
			Version      int     `json:"version"`
			ResourceName string  `json:"resourceName"`
			Key          flexKey `json:"formKey"`
		} `json:"form"`
	} `json:"deployments"`
}

func (d deployResponse) toDomain() *domain.Deployment {
	out := &domain.Deployment{Key: string(d.DeploymentKey), TenantID: d.TenantID, Processes: []domain.DeployedDefinition{}}
	for _, it := range d.Deployments {
		if pd := it.ProcessDefinition; pd != nil {
			out.Processes = append(out.Processes, domain.DeployedDefinition{ID: pd.ID, Key: string(pd.Key), Version: pd.Version, ResourceName: pd.ResourceName})
		}
		if dd := it.DecisionDefinition; dd != nil {
			out.Decisions = append(out.Decisions, domain.DeployedDefinition{ID: dd.ID, Key: string(dd.Key), Version: dd.Version, ResourceName: dd.ResourceName})
		}
		if f := it.Form; f != nil {
			out.Forms = append(out.Forms, domain.DeployedDefinition{ID: f.ID, Key: string(f.Key), Version: f.Version, ResourceName: f.ResourceName})
		}
	}
	return out
}

// ---------- definições / instâncias ----------

type processDefinitionDTO struct {
	Key          flexKey `json:"processDefinitionKey"`
	ID           string  `json:"processDefinitionId"`
	Name         string  `json:"name"`
	Version      int     `json:"version"`
	VersionTag   string  `json:"versionTag"`
	ResourceName string  `json:"resourceName"`
	TenantID     string  `json:"tenantId"`
}

func (d processDefinitionDTO) toDomain() domain.ProcessDefinition {
	return domain.ProcessDefinition{Key: string(d.Key), ID: d.ID, Name: d.Name, Version: d.Version, VersionTag: d.VersionTag, ResourceName: d.ResourceName, TenantID: d.TenantID}
}

type startedInstanceDTO struct {
	ProcessInstanceKey       flexKey          `json:"processInstanceKey"`
	ProcessDefinitionKey     flexKey          `json:"processDefinitionKey"`
	ProcessDefinitionID      string           `json:"processDefinitionId"`
	ProcessDefinitionVersion int              `json:"processDefinitionVersion"`
	TenantID                 string           `json:"tenantId"`
	Variables                domain.Variables `json:"variables"`
}

func (d startedInstanceDTO) toDomain() *domain.StartedInstance {
	return &domain.StartedInstance{
		ProcessInstanceKey: string(d.ProcessInstanceKey), ProcessDefinitionKey: string(d.ProcessDefinitionKey),
		ProcessDefinitionID: d.ProcessDefinitionID, ProcessDefinitionVersion: d.ProcessDefinitionVersion,
		TenantID: d.TenantID, Variables: d.Variables,
	}
}

type processInstanceDTO struct {
	Key                      flexKey  `json:"processInstanceKey"`
	ProcessDefinitionKey     flexKey  `json:"processDefinitionKey"`
	ProcessDefinitionID      string   `json:"processDefinitionId"`
	ProcessDefinitionName    string   `json:"processDefinitionName"`
	ProcessDefinitionVersion int      `json:"processDefinitionVersion"`
	State                    string   `json:"state"`
	StartDate                flexTime `json:"startDate"`
	EndDate                  flexTime `json:"endDate"`
	HasIncident              bool     `json:"hasIncident"`
	ParentKey                flexKey  `json:"parentProcessInstanceKey"`
	TenantID                 string   `json:"tenantId"`
}

func (d processInstanceDTO) toDomain() domain.ProcessInstance {
	state := domain.InstanceState(strings.ToUpper(d.State))
	if state == "CANCELED" {
		state = domain.InstanceTerminated
	}
	return domain.ProcessInstance{
		Key: string(d.Key), ProcessDefinitionKey: string(d.ProcessDefinitionKey), ProcessDefinitionID: d.ProcessDefinitionID,
		ProcessDefinitionName: d.ProcessDefinitionName, ProcessDefinitionVersion: d.ProcessDefinitionVersion,
		State: state, StartDate: d.StartDate.t, EndDate: d.EndDate.t, HasIncident: d.HasIncident,
		ParentInstanceKey: string(d.ParentKey), TenantID: d.TenantID,
	}
}

type variableDTO struct {
	Name               string  `json:"name"`
	Value              string  `json:"value"`
	ScopeKey           flexKey `json:"scopeKey"`
	ProcessInstanceKey flexKey `json:"processInstanceKey"`
	Truncated          bool    `json:"isTruncated"`
}

func (d variableDTO) toDomain() domain.Variable {
	return domain.Variable{Name: d.Name, Value: d.Value, ScopeKey: string(d.ScopeKey), ProcessInstanceKey: string(d.ProcessInstanceKey), Truncated: d.Truncated}
}

type messageDTO struct {
	MessageKey         flexKey `json:"messageKey"`
	ProcessInstanceKey flexKey `json:"processInstanceKey"`
	TenantID           string  `json:"tenantId"`
}

// ---------- incidentes ----------

type incidentDTO struct {
	Key                  flexKey  `json:"incidentKey"`
	ProcessInstanceKey   flexKey  `json:"processInstanceKey"`
	ProcessDefinitionKey flexKey  `json:"processDefinitionKey"`
	ProcessDefinitionID  string   `json:"processDefinitionId"`
	ErrorType            string   `json:"errorType"`
	ErrorMessage         string   `json:"errorMessage"`
	ElementID            string   `json:"elementId"`
	ElementInstanceKey   flexKey  `json:"elementInstanceKey"`
	JobKey               flexKey  `json:"jobKey"`
	State                string   `json:"state"`
	CreationTime         flexTime `json:"creationTime"`
	TenantID             string   `json:"tenantId"`
}

func (d incidentDTO) toDomain() domain.Incident {
	return domain.Incident{
		Key: string(d.Key), ProcessInstanceKey: string(d.ProcessInstanceKey), ProcessDefinitionKey: string(d.ProcessDefinitionKey),
		ProcessDefinitionID: d.ProcessDefinitionID, ErrorType: d.ErrorType, ErrorMessage: d.ErrorMessage,
		ElementID: d.ElementID, ElementInstanceKey: string(d.ElementInstanceKey), JobKey: string(d.JobKey),
		State: d.State, CreationTime: d.CreationTime.t, TenantID: d.TenantID,
	}
}

// ---------- jobs ----------

type jobDTO struct {
	Key                      flexKey           `json:"jobKey"`
	Type                     string            `json:"type"`
	ProcessInstanceKey       flexKey           `json:"processInstanceKey"`
	ProcessDefinitionID      string            `json:"processDefinitionId"`
	ProcessDefinitionKey     flexKey           `json:"processDefinitionKey"`
	ProcessDefinitionVersion int               `json:"processDefinitionVersion"`
	ElementID                string            `json:"elementId"`
	ElementInstanceKey       flexKey           `json:"elementInstanceKey"`
	Retries                  int               `json:"retries"`
	Deadline                 int64             `json:"deadline"`
	Worker                   string            `json:"worker"`
	Variables                domain.Variables  `json:"variables"`
	CustomHeaders            map[string]string `json:"customHeaders"`
	TenantID                 string            `json:"tenantId"`
}

func (d jobDTO) toDomain() domain.Job {
	if d.Variables == nil {
		d.Variables = domain.Variables{}
	}
	if d.CustomHeaders == nil {
		d.CustomHeaders = map[string]string{}
	}
	return domain.Job{
		Key: string(d.Key), Type: d.Type, ProcessInstanceKey: string(d.ProcessInstanceKey),
		ProcessDefinitionID: d.ProcessDefinitionID, ProcessDefinitionKey: string(d.ProcessDefinitionKey),
		ProcessDefinitionVersion: d.ProcessDefinitionVersion, ElementID: d.ElementID,
		ElementInstanceKey: string(d.ElementInstanceKey), Retries: d.Retries, Deadline: d.Deadline,
		Worker: d.Worker, Variables: d.Variables, CustomHeaders: d.CustomHeaders, TenantID: d.TenantID,
	}
}

// ---------- user tasks ----------

type userTaskDTO struct {
	Key                 flexKey  `json:"userTaskKey"`
	Name                string   `json:"name"`
	State               string   `json:"state"`
	Assignee            string   `json:"assignee"`
	CandidateGroups     []string `json:"candidateGroups"`
	CandidateUsers      []string `json:"candidateUsers"`
	ElementID           string   `json:"elementId"`
	ProcessInstanceKey  flexKey  `json:"processInstanceKey"`
	ProcessDefinitionID string   `json:"processDefinitionId"`
	FormKey             flexKey  `json:"formKey"`
	CreationDate        flexTime `json:"creationDate"`
	DueDate             flexTime `json:"dueDate"`
	Priority            int      `json:"priority"`
	TenantID            string   `json:"tenantId"`
}

func (d userTaskDTO) toDomain() domain.UserTask {
	return domain.UserTask{
		Key: string(d.Key), Name: d.Name, State: d.State, Assignee: d.Assignee,
		CandidateGroups: d.CandidateGroups, CandidateUsers: d.CandidateUsers, ElementID: d.ElementID,
		ProcessInstanceKey: string(d.ProcessInstanceKey), ProcessDefinitionID: d.ProcessDefinitionID,
		FormKey: string(d.FormKey), CreationDate: d.CreationDate.t, DueDate: d.DueDate.t,
		Priority: d.Priority, TenantID: d.TenantID,
	}
}
