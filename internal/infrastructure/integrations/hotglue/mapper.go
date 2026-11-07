package hotglue

import (
	"time"

	"github.com/marconi-prog/nexusflow/internal/domain"
)

// MessageJobFinished é a mensagem BPMN que o webhook publica quando um job termina.
const MessageJobFinished = "hotglue-job-finalizado"

// messageTTL mantém a mensagem no Camunda caso o webhook chegue antes de o processo
// abrir a assinatura (o job pode terminar muito rápido).
const messageTTL = time.Hour

// LifecyclePayload é o corpo do webhook de ciclo de vida de job (campos usados).
type LifecyclePayload struct {
	JobID       string `json:"job_id"`
	EnvID       string `json:"env_id"`
	FlowID      string `json:"flow_id"`
	JobName     string `json:"job_name"`
	Tenant      string `json:"tenant"`
	Status      string `json:"status"`
	Error       string `json:"error"`
	LastUpdated string `json:"last_updated"`
	Metrics     struct {
		RecordCount int64 `json:"recordCount"`
	} `json:"metrics"`
}

// ToMessage converte o webhook na mensagem que destrava o processo. O messageId inclui o
// status: reentregas do mesmo webhook não publicam a mensagem duas vezes.
func ToMessage(p LifecyclePayload) domain.PublishMessageCommand {
	return domain.PublishMessageCommand{
		Name:           MessageJobFinished,
		CorrelationKey: p.JobID,
		MessageID:      "hotglue-" + p.JobID + "-" + p.Status,
		TimeToLive:     messageTTL,
		Variables: domain.Variables{
			"hotglueStatus":    p.Status,
			"hotglueSucesso":   Succeeded(p.Status),
			"hotglueErro":      p.Error,
			"hotglueRegistros": p.Metrics.RecordCount,
		},
	}
}

// JobVariables converte o job criado nas variáveis de saída do worker.
func JobVariables(j *Job) domain.Variables {
	return domain.Variables{"hotglueJobId": j.JobID, "hotglueJobName": j.JobName, "hotglueStatus": j.Status}
}
