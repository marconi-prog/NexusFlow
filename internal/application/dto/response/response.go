package response

import "time"

// ---------- genéricos ----------

// Page é um resultado paginado.
type Page[T any] struct {
	Items []T `json:"items"`
	Total int `json:"total"`
}

// List é uma lista sem total.
type List[T any] struct {
	Items []T `json:"items"`
}

// Problem segue RFC 7807 (application/problem+json).
type Problem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Detail    string `json:"detail,omitempty"`
	Instance  string `json:"instance,omitempty"`
	RequestID string `json:"requestId,omitempty"`
}

type Principal struct {
	ID   string `json:"id"`
	Role string `json:"role"`
}

// ---------- deployments / definições ----------

type Deployment struct {
	DeploymentKey string             `json:"deploymentKey"`
	TenantID      string             `json:"tenantId,omitempty"`
	Processes     []DeployedResource `json:"processes"`
	Decisions     []DeployedResource `json:"decisions,omitempty"`
	Forms         []DeployedResource `json:"forms,omitempty"`
}

type DeployedResource struct {
	ID           string `json:"id"`
	Key          string `json:"key"`
	Version      int    `json:"version"`
	ResourceName string `json:"resourceName,omitempty"`
}

type ProcessDefinition struct {
	ProcessDefinitionKey string `json:"processDefinitionKey"`
	ProcessDefinitionID  string `json:"processDefinitionId"`
	Name                 string `json:"name,omitempty"`
	Version              int    `json:"version"`
	VersionTag           string `json:"versionTag,omitempty"`
	ResourceName         string `json:"resourceName,omitempty"`
	TenantID             string `json:"tenantId,omitempty"`
}

// ---------- instâncias ----------

type StartedInstance struct {
	ProcessInstanceKey       string         `json:"processInstanceKey"`
	ProcessDefinitionKey     string         `json:"processDefinitionKey"`
	ProcessDefinitionID      string         `json:"processDefinitionId"`
	ProcessDefinitionVersion int            `json:"processDefinitionVersion"`
	TenantID                 string         `json:"tenantId,omitempty"`
	Variables                map[string]any `json:"variables,omitempty"`
}

type ProcessInstance struct {
	ProcessInstanceKey       string     `json:"processInstanceKey"`
	ProcessDefinitionKey     string     `json:"processDefinitionKey"`
	ProcessDefinitionID      string     `json:"processDefinitionId"`
	ProcessDefinitionName    string     `json:"processDefinitionName,omitempty"`
	ProcessDefinitionVersion int        `json:"processDefinitionVersion"`
	State                    string     `json:"state"`
	StartDate                *time.Time `json:"startDate,omitempty"`
	EndDate                  *time.Time `json:"endDate,omitempty"`
	HasIncident              bool       `json:"hasIncident"`
	ParentProcessInstanceKey string     `json:"parentProcessInstanceKey,omitempty"`
	TenantID                 string     `json:"tenantId,omitempty"`
}

type InstanceDetail struct {
	Instance  ProcessInstance `json:"instance"`
	Incidents []Incident      `json:"incidents"`
	UserTasks []UserTask      `json:"userTasks"`
	Timeline  []Event         `json:"timeline"`
}

type InstanceRecord struct {
	ProcessInstanceKey  string    `json:"processInstanceKey"`
	BusinessKey         string    `json:"businessKey,omitempty"`
	ProcessDefinitionID string    `json:"processDefinitionId"`
	TenantID            string    `json:"tenantId,omitempty"`
	StartedBy           string    `json:"startedBy,omitempty"`
	StartedAt           time.Time `json:"startedAt"`
}

type Variable struct {
	Name               string `json:"name"`
	Value              string `json:"value"` // valor serializado em JSON
	ScopeKey           string `json:"scopeKey,omitempty"`
	ProcessInstanceKey string `json:"processInstanceKey,omitempty"`
	Truncated          bool   `json:"isTruncated,omitempty"`
}

// ---------- mensagens / sinais ----------

type Message struct {
	MessageKey         string `json:"messageKey"`
	ProcessInstanceKey string `json:"processInstanceKey,omitempty"`
	TenantID           string `json:"tenantId,omitempty"`
}

type Signal struct {
	SignalKey string `json:"signalKey"`
	TenantID  string `json:"tenantId,omitempty"`
}

// ---------- incidentes ----------

type Incident struct {
	IncidentKey          string     `json:"incidentKey"`
	ProcessInstanceKey   string     `json:"processInstanceKey"`
	ProcessDefinitionKey string     `json:"processDefinitionKey,omitempty"`
	ProcessDefinitionID  string     `json:"processDefinitionId,omitempty"`
	ErrorType            string     `json:"errorType"`
	ErrorMessage         string     `json:"errorMessage"`
	ElementID            string     `json:"elementId,omitempty"`
	ElementInstanceKey   string     `json:"elementInstanceKey,omitempty"`
	JobKey               string     `json:"jobKey,omitempty"`
	State                string     `json:"state"`
	CreationTime         *time.Time `json:"creationTime,omitempty"`
	TenantID             string     `json:"tenantId,omitempty"`
}

type ResolveIncident struct {
	Resolved bool     `json:"resolved"`
	Incident Incident `json:"incident"`
}

// ---------- tarefas humanas ----------

type UserTask struct {
	UserTaskKey         string     `json:"userTaskKey"`
	Name                string     `json:"name,omitempty"`
	State               string     `json:"state"`
	Assignee            string     `json:"assignee,omitempty"`
	CandidateGroups     []string   `json:"candidateGroups,omitempty"`
	CandidateUsers      []string   `json:"candidateUsers,omitempty"`
	ElementID           string     `json:"elementId"`
	ProcessInstanceKey  string     `json:"processInstanceKey"`
	ProcessDefinitionID string     `json:"processDefinitionId"`
	FormKey             string     `json:"formKey,omitempty"`
	CreationDate        *time.Time `json:"creationDate,omitempty"`
	DueDate             *time.Time `json:"dueDate,omitempty"`
	Priority            int        `json:"priority,omitempty"`
	TenantID            string     `json:"tenantId,omitempty"`
}

// ---------- monitoramento ----------

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

type EventCount struct {
	Type  string `json:"type"`
	Count int64  `json:"count"`
}

type JobStat struct {
	JobType       string  `json:"jobType"`
	Completed     int64   `json:"completed"`
	Failed        int64   `json:"failed"`
	ErrorsThrown  int64   `json:"errorsThrown"`
	AvgDurationMs float64 `json:"avgDurationMs"`
	P95DurationMs float64 `json:"p95DurationMs"`
}

type JobStats struct {
	Window string    `json:"window"`
	Items  []JobStat `json:"items"`
}

type Topology struct {
	ClusterSize       int      `json:"clusterSize"`
	PartitionsCount   int      `json:"partitionsCount"`
	ReplicationFactor int      `json:"replicationFactor"`
	GatewayVersion    string   `json:"gatewayVersion"`
	Brokers           []Broker `json:"brokers"`
}

type Broker struct {
	NodeID     int         `json:"nodeId"`
	Host       string      `json:"host"`
	Port       int         `json:"port"`
	Version    string      `json:"version"`
	Partitions []Partition `json:"partitions"`
}

type Partition struct {
	PartitionID int    `json:"partitionId"`
	Role        string `json:"role"`
	Health      string `json:"health"`
}

type Overview struct {
	GeneratedAt           time.Time    `json:"generatedAt"`
	Window                string       `json:"window"`
	Engine                *Topology    `json:"engine,omitempty"`
	EngineError           string       `json:"engineError,omitempty"`
	ActiveInstances       int          `json:"activeInstances"`
	InstancesWithIncident int          `json:"instancesWithIncident"`
	ActiveIncidents       int          `json:"activeIncidents"`
	OpenUserTasks         int          `json:"openUserTasks"`
	Events                []EventCount `json:"events"`
	Jobs                  []JobStat    `json:"jobs"`
	Partial               bool         `json:"partial"`
	Errors                []string     `json:"errors,omitempty"`
}

type Health struct {
	Status     string                     `json:"status"`
	Components map[string]ComponentHealth `json:"components"`
}

type ComponentHealth struct {
	Status  string `json:"status"`
	Detail  string `json:"detail,omitempty"`
	Latency string `json:"latency,omitempty"`
}

// ---------- pedidos (saga) ----------

type PlacedOrder struct {
	OrderID            string `json:"orderId"`
	ProcessInstanceKey string `json:"processInstanceKey"`
}

type OrderItem struct {
	SKU       string  `json:"sku"`
	Quantity  int     `json:"quantity"`
	UnitPrice float64 `json:"unitPrice"`
}

type Order struct {
	OrderID            string      `json:"orderId"`
	Customer           string      `json:"customer"`
	Items              []OrderItem `json:"items"`
	Total              float64     `json:"total"`
	Status             string      `json:"status"`
	RejectionReason    string      `json:"rejectionReason,omitempty"`
	ProcessInstanceKey string      `json:"processInstanceKey,omitempty"`
	CreatedAt          time.Time   `json:"createdAt"`
	UpdatedAt          time.Time   `json:"updatedAt"`
}

type Payment struct {
	PaymentID     string    `json:"paymentId"`
	Amount        float64   `json:"amount"`
	Status        string    `json:"status"`
	TransactionID string    `json:"transactionId,omitempty"`
	DeclineReason string    `json:"declineReason,omitempty"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

type ReservationLine struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type Reservation struct {
	Status string            `json:"status"`
	Lines  []ReservationLine `json:"lines"`
}

type OrderDetail struct {
	Order       Order        `json:"order"`
	Payment     *Payment     `json:"payment,omitempty"`
	Reservation *Reservation `json:"reservation,omitempty"`
}

type InventoryItem struct {
	SKU       string `json:"sku"`
	Name      string `json:"name"`
	Available int    `json:"available"`
}
