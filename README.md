# NexusFlow

Plataforma em Go para **executar, monitorar e orquestrar processos de negócio distribuídos** sobre o **Camunda 8**.
Feita para estudar Camunda com um backend de verdade: arquitetura em camadas, workers idempotentes,
saga com compensação, integração assíncrona por webhook e observabilidade.

> Fora deste repositório: o cluster Camunda 8 e a infraestrutura. A aplicação só precisa das
> variáveis de [`.env.example`](.env.example). Testado contra **Camunda 8.10** (REST API v2).

## Estrutura

```
cmd/
  api/                      API REST (N réplicas)
  workers/                  job workers (N réplicas; WORKER_JOB_TYPES dedica réplicas por tipo)
internal/
  domain/                   regras de negócio puras, sem Camunda nem banco
    order/ payment/ inventory/   modelos da saga de pedido
  application/
    ports/                  interfaces: Engine (motor) e Store (persistência)
    services/               casos de uso: processos, incidentes, tarefas, monitoramento, pedidos
    dto/request, dto/response    contrato JSON da API
    mappers/                domínio → response
  infrastructure/
    camunda/                adapter da Orchestration Cluster REST API v2 (+ DTOs do Camunda)
    database/               PostgreSQL (e implementações em memória para dev/testes)
    observability/          métricas Prometheus e trilha de auditoria
    integrations/hotglue/   client, config, webhook_handler e mapper do Hotglue
  interfaces/http/
    routes/ handlers/ middleware/ validators/ respond/
  worker/                   framework de job workers; handlers/ = um handler por job type
migrations/                 SQL versionado, embutido no binário e aplicado na subida
processes/bpmn/             modelos BPMN (abra no Camunda Modeler)
scripts/                    cenarios-saga.sh: exercita todos os caminhos da saga
tests/fakes/                motor falso para testes
```

A regra das camadas: `domain` não importa nada do projeto; `application` só conhece `domain` e as
interfaces de `ports`; `infrastructure` e `interfaces` implementam e expõem.

## Executando

```bash
cp .env.example .env          # ajuste CAMUNDA_* e DATABASE_URL (Postgres é necessário para a saga)
make run-api                  # API em :8080 (aplica as migrations)
make run-worker               # workers (métricas em :9091)

# deploy dos processos
curl -X POST localhost:8080/api/v1/deployments -H "X-API-Key: $KEY" \
  -F resources=@processes/bpmn/pedido-saga.bpmn -F resources=@processes/bpmn/sincronizacao-hotglue.bpmn

make test                     # testes unitários (sem Camunda e sem banco)
make test-integration DB=postgres://user:pass@host:5432/db_descartavel
```

## Roteiro de estudo: saga de pedido

[`processes/bpmn/pedido-saga.bpmn`](processes/bpmn/pedido-saga.bpmn) usa quase tudo o que importa no Camunda:

```
Registrar ─► Reservar estoque ─► precisa aprovação? ─► Cobrar ─► Aguardar entrega ─► Concluir
               │ ⚡ESTOQUE_INSUFICIENTE   │ >R$1000            │ ⚡PAGAMENTO_RECUSADO   │ ⏱ prazo
               │ ↺ Liberar estoque        └► Aprovar (humano)   │ ↺ Estornar             │
               └──────────────────────────────┴─────────────────┴────────────────────────┴─► Desfazer operações (compensação) ─► Rejeitar
```

| Conceito | Onde ver | Como provocar |
|---|---|---|
| Service task + job worker | todas as tarefas; `internal/worker/handlers/saga.go` | qualquer pedido |
| Erro BPMN (boundary error) | Reservar estoque / Cobrar | estoque insuficiente; cartão final `0002` |
| **Compensação** (desfazer) | Liberar estoque / Estornar pagamento | cartão `0002`, aprovação reprovada, prazo vencido |
| Retries com backoff | `payment.charge` (header `retryBackoff`) | cartão final `0003` (falha 2x e aprova) |
| Incidente | `order.create` com dado inválido | cenário 7 do script, ou cartão `0004` (gateway sempre fora) |
| Gateway exclusivo (FEEL) | `=total > 1000` | item de R$ 4500 |
| Tarefa humana | Aprovar pedido (grupo `financeiro`) | complete com `{"aprovado": true/false}` |
| Correlação de mensagem | Aguardar entrega (`correlationKey = pedidoId`) | `POST /orders/:id/delivery-confirmation` |
| Timer boundary | prazo de entrega | `"deliveryDeadline": "PT10S"` |
| Output mapping | boundaries de erro e timer | leva `motivoRejeicao` ao escopo do processo |
| Idempotência | todos os handlers | o Camunda entrega jobs "pelo menos uma vez" |

```bash
# fazer um pedido (dispara a saga; o id do pedido vira a business key)
curl -X POST localhost:8080/api/v1/orders -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"customer":"Ana","items":[{"sku":"MOUSE","quantity":2,"unitPrice":125}],"cardNumber":"4111111111111111"}'

curl localhost:8080/api/v1/orders/PED-XXXX -H "X-API-Key: $KEY"                         # pedido + pagamento + reserva
curl -X POST localhost:8080/api/v1/orders/PED-XXXX/delivery-confirmation -H "X-API-Key: $KEY"
curl -N "localhost:8080/api/v1/events/stream" -H "X-API-Key: $KEY"                        # tudo em tempo real

NEXUSFLOW_API_KEY=$KEY ./scripts/cenarios-saga.sh        # os 7 cenários de uma vez
```

Incidente na prática: o job falha sem retries → `GET /incidents` → corrija o dado com
`PUT /process-instances/:key/variables` → `POST /incidents/:key/resolution {"retries":1}`.

Só os 4 últimos dígitos do cartão entram nas variáveis do processo: dado sensível não deve ficar visível no Operate.

## Integração Hotglue

[`processes/bpmn/sincronizacao-hotglue.bpmn`](processes/bpmn/sincronizacao-hotglue.bpmn) mostra o padrão
**comando assíncrono + espera por mensagem**:

1. o worker `hotglue.run-job` chama `POST /{env}/{flow}/{tenant}/jobs` e devolve `hotglueJobId`;
2. o processo espera a mensagem `hotglue-job-finalizado` (correlationKey = `hotglueJobId`), com timeout de 2h;
3. o Hotglue chama `POST /webhooks/hotglue`; a assinatura `X-Hub-Signature` (HMAC-SHA1) é validada e a mensagem é publicada com TTL de 1h (o webhook pode chegar antes de o processo começar a esperar) e `messageId` único (reentregas não duplicam);
4. sucesso encerra; falha ou timeout abre a tarefa "Analisar falha" para o grupo `integracoes`.

Configure `HOTGLUE_*` e cadastre `https://<sua-api>/webhooks/hotglue` como webhook de ciclo de vida de job no Hotglue,
com o mesmo `HOTGLUE_WEBHOOK_SECRET`. Sem `HOTGLUE_API_KEY`, o worker não é registrado; sem o segredo, o webhook responde 503.

## API (`/api/v1`)

Autenticação: `Authorization: Bearer <chave>` ou `X-API-Key`. Papéis: `viewer` < `operator` < `admin`.
Erros seguem RFC 7807 com `requestId`. `Idempotency-Key` é aceita nos POST de instâncias, pedidos, mensagens e sinais.

| Recurso | Rotas |
|---|---|
| Definições | `POST /deployments` (admin) · `GET /process-definitions[?latest=true]` · `GET /process-definitions/:key[/xml]` |
| Instâncias | `POST /process-instances` · `GET /process-instances[?state&hasIncident&businessKey]` · `GET /process-instances/:key` (detalhe + timeline) · `GET/PUT /process-instances/:key/variables` · `GET /process-instances/:key/events` · `POST /process-instances/:key/cancellation` |
| Mensagens/sinais | `POST /messages` (`correlate:true` exige correlação imediata) · `POST /signals` |
| Incidentes | `GET /incidents[?state=ACTIVE\|RESOLVED\|ALL]` · `GET /incidents/:key` · `POST /incidents/:key/resolution` |
| Tarefas humanas | `GET /user-tasks` · `GET /user-tasks/:key` · `POST /user-tasks/:key/assignment` · `DELETE /user-tasks/:key/assignee` · `POST /user-tasks/:key/completion` |
| Pedidos (saga) | `POST /orders` · `GET /orders[?status]` · `GET /orders/:id` · `POST /orders/:id/delivery-confirmation` · `GET /inventory` · `PUT /inventory/:sku` (admin) |
| Monitoramento | `GET /monitoring/overview` · `GET /monitoring/topology` · `GET /monitoring/jobs` · `GET /events` · `GET /events/stream` (SSE, retoma via `Last-Event-ID`) |

Sem autenticação: `GET /health/live`, `GET /health/ready`, `GET /metrics`, `POST /webhooks/hotglue` (validado por HMAC).

## Workers: como escrever um handler

```go
w.Register(worker.Spec{
    Type: "meu.job",                       // = zeebe:taskDefinition type no BPMN
    Handler: func(ctx context.Context, j *worker.Job) (domain.Variables, error) {
        // sucesso                           -> return domain.Variables{"x": 1}, nil
        // erro de negócio (boundary)        -> return nil, worker.NewBusinessError("CODIGO", "msg", vars)
        // falha técnica (o Camunda retenta) -> return nil, err
        // falha definitiva (incidente)      -> return nil, worker.Permanent(err)
    },
})
```

Regras que valem para qualquer handler: ele precisa ser idempotente (o mesmo job pode rodar duas vezes); o `ctx` expira no
deadline do job; panic vira falha do job; o header `retryBackoff` (ex.: `30s`) define a espera entre tentativas.

## Monitoramento

- **Métricas** (`/metrics` na API, `:9091/metrics` no worker): requisições HTTP, instâncias iniciadas, jobs por resultado, duração de job, jobs em execução, incidentes ativos, saúde do motor.
- **Auditoria** (Postgres): cada comando e cada job vira um evento com ator, instância e duração. A retenção é configurável e o stream SSE junta eventos de todas as réplicas via `LISTEN/NOTIFY`.
- **Incidentes**: a API varre os incidentes ativos periodicamente e emite `incident.detected`. Com várias réplicas da API, deixe a varredura ligada em só uma.

## Notas de operação

- `/metrics` e `/health/*` são públicos: restrinja o acesso pela rede.
- Sem `API_KEYS`, a autenticação fica desligada (só fora de produção). Sem `DATABASE_URL`, os dados ficam em memória e API e workers **não** compartilham pedidos.
- O gateway de pagamento é simulado (`internal/domain/payment/simulated.go`); troque por um adapter real que implemente `payment.Gateway`.
