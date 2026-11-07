package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/integrations/hotglue"
	"github.com/marconi-prog/nexusflow/internal/worker"
)

const TypeHotglueRunJob = "hotglue.run-job"

// HotglueRunJob dispara um job no Hotglue para a variável "tenant" (ou o header "tenant" do
// service task) e devolve hotglueJobId, usado como correlationKey da mensagem de término.
func HotglueRunJob(c *hotglue.Client) worker.Spec {
	return worker.Spec{
		Type:           TypeHotglueRunJob,
		FetchVariables: []string{"tenant", "hotglueState"},
		Handler: func(ctx context.Context, j *worker.Job) (domain.Variables, error) {
			tenant := stringVar(j, "tenant")
			if tenant == "" {
				tenant = j.CustomHeaders["tenant"]
			}
			if tenant == "" {
				return nil, worker.Permanent(errors.New("informe a variável ou o header 'tenant'"))
			}
			state, _ := j.Variables["hotglueState"].(map[string]any)
			job, err := c.RunJob(ctx, tenant, hotglue.RunJobRequest{
				JobName:       fmt.Sprintf("nexusflow-%s", j.ProcessInstanceKey),
				ExternalJobID: j.ElementInstanceKey,
				State:         state,
			})
			if err != nil {
				var api *hotglue.APIError
				if errors.As(err, &api) && !api.Retryable() {
					return nil, worker.Permanent(err) // 4xx: configuração errada, vira incidente
				}
				return nil, err // rede, 5xx, 429 ou job ativo/enfileirado: tenta de novo
			}
			j.Logger.Info("job do hotglue disparado", "hotglueJobId", job.JobID, "tenant", tenant)
			return hotglue.JobVariables(job), nil
		},
	}
}
