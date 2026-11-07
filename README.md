# NexusFlow

Plataforma para executar, monitorar e orquestrar processos de negócio distribuídos sobre o **Camunda 8**.

O Camunda (Zeebe) é o motor: guarda o estado dos processos, as filas de jobs e os timers.
O NexusFlow fica em volta dele e entrega:

- **API REST unificada** para deploy, execução, mensagens/sinais, incidentes e tarefas humanas, com autenticação, RBAC e idempotência
- **Workers distribuídos** que executam as service tasks com concorrência controlada, retentativa com backoff, erros BPMN e shutdown gracioso
- **Monitoramento**: painel consolidado, trilha de auditoria, stream de eventos em tempo real (SSE), detecção de incidentes e métricas Prometheus

> Fora do escopo deste repositório: o cluster Camunda 8 e a infraestrutura (deploy, banco, observabilidade).
> A aplicação só precisa das variáveis de ambiente descritas em [`.env.example`](.env.example).

## Arquitetura

```
              ┌──────────────────────── nexusflow-api (N réplicas) ───────────────────────┐
 clientes ──► │ Gin ─► auth/RBAC ─► idempotência ─► service ─► engine.Engine (porta)       │
 (ERP, UI)    │                                        │            │                      │
              │                               audit.Recorder   camunda.Client ───────────┼──► Camunda 8
              │  SSE Hub ◄── LISTEN/NOTIFY ──┐         │        (REST v2, OAuth/basic)    │    REST API v2
              └──────────────────────────────┼─────────┼──────────────────────────────────┘        ▲
                                             │         ▼                                           │
                                         PostgreSQL (eventos, idempotência, business keys)         │
                                             ▲                                                     │
              ┌──────────────────────────────┼─ nexusflow-worker (N réplicas) ────────────────────┐ │
              │  poller por job type ─► semáforo ─► Handler ─► complete / fail / throw error ─────┼─┘
              └────────────────────────────────────────────────────────────────────────────────────┘
```

| Pacote | Responsabilidade |
|---|---|
| `cmd/api`, `cmd/worker` | Binários; montagem de dependências e shutdown gracioso |
| `internal/domain` | Tipos de negócio e erros classificados (`Kind`), independentes do Camunda |
| `internal/engine` | Porta `Engine`; `camunda/` é o adapter REST v2; `enginetest/` é um fake para testes |
| `internal/service` | Casos de uso: processos, incidentes, tarefas, monitoramento |
| `internal/worker` | Framework de job workers; `handlers/` traz handlers genéricos |
| `internal/store` | Persistência própria: interface `Store` e implementação `memory` (dev/testes) |
| `internal/httpapi` | Rotas, handlers por recurso, middlewares (request-id, auth, RBAC, CORS, idempotência), SSE |
| `internal/httpapi/dto` | Contrato público da API: `request.go`, `response.go` e `mapper.go` (domínio → response) |
| `internal/metrics`, `internal/audit` | Métricas Prometheus e trilha de auditoria |

O adapter usa a **Orchestration Cluster REST API v2** (Camunda 8.8+). Ele aceita chaves como string
ou número e datas em vários formatos, então também funciona com 8.6/8.7, desde que os endpoints `/v2` estejam habilitados.

## Executando

```bash
cp .env.example .env      # ajuste CAMUNDA_* e DATABASE_URL
make run-api              # API em :8080
make run-worker           # workers (métricas em :9091)
make test                 # testes unitários (não exige Camunda nem banco)
make test-integration DB=postgres://user:pass@host:5432/db_descartavel
make build VERSION=1.0.0  # gera bin/nexusflow-api e bin/nexusflow-worker (estáticos)
```

As migrações do PostgreSQL são aplicadas automaticamente na subida, sob advisory lock (várias réplicas podem subir ao mesmo tempo).

## API (`/api/v1`)

Autenticação: `Authorization: Bearer <chave>` ou `X-API-Key: <chave>`. Erros seguem RFC 7807 (`application/problem+json`) e incluem `requestId`.

| Método | Rota | Papel | Descrição |
|---|---|---|---|
| POST | `/deployments` | admin | Deploy multipart (`resources`: .bpmn/.dmn/.form) |
| GET | `/process-definitions` | viewer | `?processDefinitionId&name&version&latest=true` |
| GET | `/process-definitions/:key[/xml]` | viewer | Definição / XML BPMN |
| POST | `/process-instances` | operator | Inicia instância (aceita `Idempotency-Key`) |
| GET | `/process-instances` | viewer | `?state&processDefinitionId&hasIncident&sort=startDate:desc` ou `?businessKey=` |
| GET | `/process-instances/:key` | viewer | Detalhe: instância + incidentes + tarefas + timeline |
| GET | `/process-instances/:key/variables` | viewer | Variáveis |
| GET | `/process-instances/:key/events` | viewer | Trilha de auditoria da instância |
| POST | `/process-instances/:key/cancellation` | operator | `{"reason": "..."}` |
| POST | `/messages` | operator | Publica (`correlate:false`, com TTL) ou correlaciona (`correlate:true`) |
| POST | `/signals` | operator | `{"signalName": "...", "variables": {}}` |
| GET | `/incidents[/:key]` | viewer | `?state=ACTIVE|RESOLVED|ALL&processInstanceKey&errorType` |
| POST | `/incidents/:key/resolution` | operator | `{"retries": 3}`: devolve retentativas ao job e resolve |
| GET | `/user-tasks[/:key]` | viewer | `?state=CREATED&assignee&candidateGroup` |
| POST | `/user-tasks/:key/assignment` | operator | `{"assignee": "ana", "allowOverride": false}` |
| DELETE | `/user-tasks/:key/assignee` | operator | Remove o responsável |
| POST | `/user-tasks/:key/completion` | operator | `{"variables": {}, "action": "aprovar"}` |
| GET | `/monitoring/overview` | viewer | Painel: instâncias ativas, incidentes, tarefas, jobs (`?window=24h`) |
| GET | `/monitoring/topology` | viewer | Topologia do cluster |
| GET | `/monitoring/jobs` | viewer | Throughput, falhas, média e p95 por job type (`?window=1h`) |
| GET | `/events` | viewer | Auditoria: `?type=a,b&processInstanceKey&jobType&since&until` |
| GET | `/events/stream` | viewer | **SSE** em tempo real, com os mesmos filtros; retoma via `Last-Event-ID` |

Públicos (para probes/scrape): `GET /health/live`, `GET /health/ready` (verifica Camunda e banco) e `GET /metrics`.

### Exemplos

```bash
# deploy
curl -X POST localhost:8080/api/v1/deployments -H "X-API-Key: $KEY" -F resources=@pedido.bpmn

# iniciar com business key, de forma segura para retentativa
curl -X POST localhost:8080/api/v1/process-instances -H "X-API-Key: $KEY" \
  -H "Idempotency-Key: pedido-123" -H "Content-Type: application/json" \
  -d '{"processDefinitionId":"pedido","businessKey":"PED-123","variables":{"pedido":{"valor":250}}}'

# correlacionar uma mensagem (ex.: pagamento confirmado)
curl -X POST localhost:8080/api/v1/messages -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"name":"pagamento-confirmado","correlationKey":"PED-123","correlate":true}'

# acompanhar em tempo real
curl -N "localhost:8080/api/v1/events/stream?type=job.failed,incident.detected" -H "X-API-Key: $KEY"
```

## Workers

Cada réplica de `nexusflow-worker` faz long-polling por job type, executa até `WORKER_CONCURRENCY`
jobs simultâneos por tipo e reporta o resultado ao Camunda. Para escalar, basta subir mais réplicas.
`WORKER_JOB_TYPES` dedica réplicas a tipos específicos (ex.: um pool só para integrações lentas).

### Escrevendo um handler

```go
// cmd/worker/business.go
w.Register(worker.Spec{
    Type:           "pagamento.cobrar",     // = zeebe:taskDefinition type no BPMN
    FetchVariables: []string{"pedido"},
    Handler: func(ctx context.Context, j *worker.Job) (domain.Variables, error) {
        // sucesso                    -> return domain.Variables{"cobrado": true}, nil
        // erro de negócio (boundary) -> return nil, worker.NewBusinessError("SEM_SALDO", "saldo insuficiente", nil)
        // falha técnica (retenta)    -> return nil, err            (retries-1, backoff exponencial)
        // falha definitiva           -> return nil, worker.Permanent(err)  (incidente imediato)
    },
})
```

O `ctx` do handler expira no deadline do job. Panics viram falha do job. O backoff pode ser fixado
por service task com o header `retryBackoff` (ex.: `30s`).

### Handlers incluídos

| Type | Função |
|---|---|
| `nexusflow.http` | Chamada HTTP configurada por custom headers: `url` (aceita `${variavel}`), `method`, `timeout`, `bodyVariable`, `resultVariable`, `headers` (JSON). 2xx completa; 4xx vira erro BPMN `HTTP_<status>`; 5xx/429/rede retentam. Envia `Idempotency-Key` = element instance key. Destinos restritos por `WORKER_HTTP_ALLOWED_HOSTS` (proteção contra SSRF). |
| `nexusflow.log` | Registra variáveis e headers no log (depuração de modelos) |
| `exemplo.validar-pedido` | Exemplo dos três desfechos (habilite com `WORKER_ENABLE_EXAMPLES=true`) |

## Monitoramento

- **Métricas** (`/metrics` na API; `:9091/metrics` no worker): `nexusflow_http_*`, `nexusflow_process_instances_started_total`,
  `nexusflow_jobs_handled_total{type,outcome}`, `nexusflow_job_duration_seconds`, `nexusflow_jobs_in_flight`,
  `nexusflow_job_activation_errors_total`, `nexusflow_incidents_active`, `nexusflow_engine_up`, `nexusflow_event_record_errors_total`
- **Eventos de auditoria**: `deployment.created`, `instance.started|cancelled`, `message.published`, `signal.broadcast`,
  `incident.detected|resolved`, `job.completed|failed|error_thrown|retries_updated`, `usertask.assigned|unassigned|completed`.
  Cada evento registra o ator (nome da chave de API ou do worker). Retenção configurável em `EVENT_RETENTION`.
- **Tempo real**: eventos de qualquer réplica (API ou worker) chegam a todas as conexões SSE via `LISTEN/NOTIFY` do PostgreSQL.
- **Incidentes**: a API varre os incidentes ativos a cada `MONITOR_INCIDENT_POLL_INTERVAL` e emite `incident.detected` para os novos.
  Com várias réplicas da API, deixe a varredura ligada em apenas uma (`0` nas demais) para não duplicar eventos.

## Notas de operação

- `/metrics` e `/health/*` não exigem autenticação; restrinja o acesso pela rede.
- Sem `API_KEYS`, a autenticação fica desligada (o principal é `dev`/admin). Em `APP_ENV=production` a subida falha sem chaves.
- Sem `DATABASE_URL`, o armazenamento fica em memória: serve só para desenvolvimento, porque API e worker não compartilham eventos.
- Falha ao gravar auditoria não derruba a operação (o comando já foi aceito pelo Camunda); ela é contabilizada em `nexusflow_event_record_errors_total`.
# NexusFlow
