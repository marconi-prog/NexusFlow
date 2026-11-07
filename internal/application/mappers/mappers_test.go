package mappers

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/marconi-prog/nexusflow/internal/application/dto/request"
	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/application/services"
	"github.com/marconi-prog/nexusflow/internal/domain"
)

// Estes testes travam o contrato JSON da API: se falharem, a mudança quebra clientes.
func assertJSON(t *testing.T, v any, want string) {
	t.Helper()
	got, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var g, w any
	_ = json.Unmarshal(got, &g)
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("JSON esperado inválido: %v", err)
	}
	gb, _ := json.Marshal(g)
	wb, _ := json.Marshal(w)
	if string(gb) != string(wb) {
		t.Fatalf("contrato JSON mudou\n got: %s\nwant: %s", gb, wb)
	}
}

func TestProcessInstanceContract(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	r := ToProcessInstance(domain.ProcessInstance{
		Key: "1", ProcessDefinitionKey: "2", ProcessDefinitionID: "pedido", ProcessDefinitionVersion: 3,
		State: domain.InstanceActive, StartDate: &start, HasIncident: true, ParentInstanceKey: "9",
	})
	assertJSON(t, r, `{"processInstanceKey":"1","processDefinitionKey":"2","processDefinitionId":"pedido",
		"processDefinitionVersion":3,"state":"ACTIVE","startDate":"2026-01-02T03:04:05Z","hasIncident":true,
		"parentProcessInstanceKey":"9"}`)
}

func TestPageNeverSerializesNullItems(t *testing.T) {
	r := ToPage(&domain.Result[domain.Incident]{Items: nil, Total: 0}, ToIncident)
	assertJSON(t, r, `{"items":[],"total":0}`)
}

func TestInstanceDetailContract(t *testing.T) {
	d := &services.InstanceDetail{
		Instance:  &domain.ProcessInstance{Key: "1", State: domain.InstanceCompleted},
		Incidents: []domain.Incident{{Key: "5", ProcessInstanceKey: "1", ErrorType: "JOB_NO_RETRIES", ErrorMessage: "x", State: "ACTIVE"}},
		Timeline:  []ports.Event{{ID: 7, Type: ports.EventInstanceStarted, Source: "api", OccurredAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}},
	}
	assertJSON(t, ToInstanceDetail(d), `{
		"instance":{"processInstanceKey":"1","processDefinitionKey":"","processDefinitionId":"","processDefinitionVersion":0,"state":"COMPLETED","hasIncident":false},
		"incidents":[{"incidentKey":"5","processInstanceKey":"1","errorType":"JOB_NO_RETRIES","errorMessage":"x","state":"ACTIVE"}],
		"userTasks":[],
		"timeline":[{"id":7,"type":"instance.started","source":"api","occurredAt":"2026-01-01T00:00:00Z"}]}`)
}

func TestDeploymentContract(t *testing.T) {
	r := ToDeployment(&domain.Deployment{Key: "9", Processes: []domain.DeployedDefinition{{ID: "pedido", Key: "10", Version: 2, ResourceName: "pedido.bpmn"}}})
	assertJSON(t, r, `{"deploymentKey":"9","processes":[{"id":"pedido","key":"10","version":2,"resourceName":"pedido.bpmn"}]}`)
}

func TestRequestToCommand(t *testing.T) {
	cmd, err := request.StartInstance{ProcessDefinitionID: "p", AwaitCompletion: true, RequestTimeout: "30s"}.ToCommand()
	if err != nil || cmd.RequestTimeout != 30*time.Second || !cmd.AwaitCompletion {
		t.Fatalf("%+v %v", cmd, err)
	}
	if _, err := (request.StartInstance{RequestTimeout: "1h"}).ToCommand(); domain.KindOf(err) != domain.KindInvalid {
		t.Fatalf("timeout acima do limite deveria ser inválido: %v", err)
	}
	if _, err := (request.PublishMessage{Name: "m", TimeToLive: "abc"}).ToCommand(); domain.KindOf(err) != domain.KindInvalid {
		t.Fatalf("TTL inválido: %v", err)
	}
	if c := (request.BroadcastSignal{SignalName: "s"}).ToCommand(); c.Name != "s" {
		t.Fatalf("%+v", c)
	}
	if err := (request.ResolveIncident{Retries: 101}).Validate(); err == nil {
		t.Fatal("retries > 100 deveria falhar")
	}
}
