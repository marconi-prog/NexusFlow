// Package domain contém os tipos de negócio da plataforma, independentes do motor de orquestração.
package domain

import (
	"errors"
	"fmt"
	"time"
)

// Variables representa as variáveis de processo (JSON objeto).
type Variables map[string]any

// Page descreve paginação por offset usada nas buscas.
type Page struct {
	From  int
	Limit int
}

func (p Page) Normalize() Page {
	if p.Limit <= 0 {
		p.Limit = 50
	}
	if p.Limit > 500 {
		p.Limit = 500
	}
	if p.From < 0 {
		p.From = 0
	}
	return p
}

// Sort ordena resultados de busca.
type Sort struct {
	Field string
	Order string // ASC | DESC
}

// Result é um resultado paginado genérico.
type Result[T any] struct {
	Items []T
	Total int
}

// ---------- Deployments / definições ----------

type Resource struct {
	Name    string
	Content []byte
}

type Deployment struct {
	Key       string
	TenantID  string
	Processes []DeployedDefinition
	Decisions []DeployedDefinition
	Forms     []DeployedDefinition
}

type DeployedDefinition struct {
	ID           string
	Key          string
	Version      int
	ResourceName string
}

type ProcessDefinition struct {
	Key          string
	ID           string
	Name         string
	Version      int
	VersionTag   string
	ResourceName string
	TenantID     string
}

type ProcessDefinitionFilter struct {
	ID       string
	Name     string
	Version  int
	Latest   bool
	TenantID string
}

// ---------- Instâncias ----------

type InstanceState string

const (
	InstanceActive     InstanceState = "ACTIVE"
	InstanceCompleted  InstanceState = "COMPLETED"
	InstanceTerminated InstanceState = "TERMINATED"
)

func (s InstanceState) Valid() bool {
	switch s {
	case InstanceActive, InstanceCompleted, InstanceTerminated:
		return true
	}
	return false
}

type StartInstanceCommand struct {
	ProcessDefinitionID      string
	ProcessDefinitionKey     string
	ProcessDefinitionVersion int
	Variables                Variables
	TenantID                 string
	// AwaitCompletion bloqueia até o processo terminar (fluxos curtos/síncronos).
	AwaitCompletion bool
	RequestTimeout  time.Duration
	FetchVariables  []string
	BusinessKey     string
}

func (c StartInstanceCommand) Validate() error {
	if c.ProcessDefinitionID == "" && c.ProcessDefinitionKey == "" {
		return Invalid("processDefinitionId ou processDefinitionKey é obrigatório")
	}
	if c.ProcessDefinitionID != "" && c.ProcessDefinitionKey != "" {
		return Invalid("informe apenas processDefinitionId ou processDefinitionKey")
	}
	return nil
}

type StartedInstance struct {
	ProcessInstanceKey       string
	ProcessDefinitionKey     string
	ProcessDefinitionID      string
	ProcessDefinitionVersion int
	TenantID                 string
	Variables                Variables
}

type ProcessInstance struct {
	Key                      string
	ProcessDefinitionKey     string
	ProcessDefinitionID      string
	ProcessDefinitionName    string
	ProcessDefinitionVersion int
	State                    InstanceState
	StartDate                *time.Time
	EndDate                  *time.Time
	HasIncident              bool
	ParentInstanceKey        string
	TenantID                 string
}

type ProcessInstanceFilter struct {
	ProcessDefinitionID  string
	ProcessDefinitionKey string
	State                InstanceState
	HasIncident          *bool
	ParentInstanceKey    string
	TenantID             string
}

type Variable struct {
	Name               string
	Value              string // valor serializado em JSON
	ScopeKey           string
	ProcessInstanceKey string
	Truncated          bool
}

// ---------- Mensagens e sinais ----------

type PublishMessageCommand struct {
	Name           string
	CorrelationKey string
	TimeToLive     time.Duration
	MessageID      string
	Variables      Variables
	TenantID       string
	// Correlate=true exige correlação imediata com uma instância (falha se não houver).
	Correlate bool
}

func (c PublishMessageCommand) Validate() error {
	if c.Name == "" {
		return Invalid("name é obrigatório")
	}
	return nil
}

type MessageResult struct {
	MessageKey         string
	ProcessInstanceKey string
	TenantID           string
}

type BroadcastSignalCommand struct {
	Name      string
	Variables Variables
	TenantID  string
}

type SignalResult struct {
	SignalKey string
	TenantID  string
}

// ---------- Incidentes ----------

type Incident struct {
	Key                  string
	ProcessInstanceKey   string
	ProcessDefinitionKey string
	ProcessDefinitionID  string
	ErrorType            string
	ErrorMessage         string
	ElementID            string
	ElementInstanceKey   string
	JobKey               string
	State                string
	CreationTime         *time.Time
	TenantID             string
}

type IncidentFilter struct {
	ProcessInstanceKey  string
	ProcessDefinitionID string
	State               string
	ErrorType           string
	TenantID            string
}

// ---------- Jobs (service tasks) ----------

type Job struct {
	Key                      string
	Type                     string
	ProcessInstanceKey       string
	ProcessDefinitionID      string
	ProcessDefinitionKey     string
	ProcessDefinitionVersion int
	ElementID                string
	ElementInstanceKey       string
	Retries                  int
	Deadline                 int64
	Worker                   string
	Variables                Variables
	CustomHeaders            map[string]string
	TenantID                 string
}

type ActivateJobsCommand struct {
	Type           string
	Worker         string
	Timeout        time.Duration
	MaxJobs        int
	FetchVariables []string
	RequestTimeout time.Duration
	TenantIDs      []string
}

type FailJobCommand struct {
	Retries      int
	ErrorMessage string
	RetryBackOff time.Duration
	Variables    Variables
}

type ThrowErrorCommand struct {
	ErrorCode    string
	ErrorMessage string
	Variables    Variables
}

// ---------- User tasks ----------

type UserTask struct {
	Key                 string
	Name                string
	State               string
	Assignee            string
	CandidateGroups     []string
	CandidateUsers      []string
	ElementID           string
	ProcessInstanceKey  string
	ProcessDefinitionID string
	FormKey             string
	CreationDate        *time.Time
	DueDate             *time.Time
	Priority            int
	TenantID            string
}

type UserTaskFilter struct {
	State              string
	Assignee           string
	CandidateGroup     string
	CandidateUser      string
	ProcessInstanceKey string
	ElementID          string
	TenantID           string
}

// ---------- Topologia ----------

type Topology struct {
	ClusterSize       int
	PartitionsCount   int
	ReplicationFactor int
	GatewayVersion    string
	Brokers           []Broker
}

type Broker struct {
	NodeID     int
	Host       string
	Port       int
	Version    string
	Partitions []Partition
}

type Partition struct {
	PartitionID int
	Role        string
	Health      string
}

// ---------- Erros ----------

// Kind classifica erros para mapeamento em protocolo (HTTP) e decisões de retry.
type Kind int

const (
	KindInternal Kind = iota
	KindInvalid
	KindNotFound
	KindConflict
	KindUnauthorized
	KindForbidden
	KindUnavailable
	KindTimeout
)

type Error struct {
	Kind    Kind
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *Error) Unwrap() error { return e.Err }

func NewError(kind Kind, msg string, err error) *Error {
	return &Error{Kind: kind, Message: msg, Err: err}
}

func Invalid(msg string) *Error  { return &Error{Kind: KindInvalid, Message: msg} }
func NotFound(msg string) *Error { return &Error{Kind: KindNotFound, Message: msg} }
func Conflict(msg string) *Error { return &Error{Kind: KindConflict, Message: msg} }

// KindOf extrai a classificação de um erro (KindInternal se não classificado).
func KindOf(err error) Kind {
	if de, ok := errors.AsType[*Error](err); ok {
		return de.Kind
	}
	return KindInternal
}
