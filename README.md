# dropin-queue

> **Drop-in replacement AWS SQS/SNS — backend de mensageria trocável (NATS JetStream ou Postgres)**

`dropin-queue` oferece uma API HTTP que fala os mesmos protocolos (`Query` e `JSON`) e devolve
as mesmas respostas que os serviços gerenciados da AWS. Você aponta qualquer cliente oficial
(`boto3`, `aws-sdk-go-v2`, `aws-sdk-js`, `aws-sdk-java`, `aws-cli`, SDK da sua linguagem favorita)
para o endpoint do shim e tudo funciona como antes — sem precisar de uma conta AWS.

A camada de mensageria por baixo é **trocável via config** (`GQ_BACKEND=nats|postgres`,
nunca os dois ao mesmo tempo) — a API HTTP, o wire format AWS e a lógica de negócio SQS/SNS
são idênticos nos dois casos; só o adapter de storage muda. Ver
[`docs/architecture.md`](docs/architecture.md#backend-postgres) para o mapeamento completo e
os trade-offs de cada backend.

### Backend NATS JetStream (default)

A camada de mensageria por baixo é o **NATS JetStream**, que entrega:

- Cluster Raft com 3 ou 5 nós (alta disponibilidade real)
- Persistência em disco + snapshots em object storage (S3/GCS/Azure Blob)
- Pull consumers com `expires` (long-polling nativo, igual ao `WaitTimeSeconds`)
- `AckWait` nativo (visibility timeout sem scheduler custom)
- Dedup nativo via `Nats-Msg-Id` (compatível com `MessageDeduplicationId` do SQS FIFO)
- Single binary, ~6 MB de RAM cold start

A camada de API é um **shim em Go** (`dropin-server`) que implementa:

- Parsers/serializers dos protocolos AWS SQS Query, SQS JSON, SNS Query, SNS JSON
- Verificação de **AWS Signature v4** (SigV4) com IAM real (policy JSON evaluation)
- Long-polling via `Fetch` do JetStream com `MaxWait`
- Visibility timeout scheduler
- DLQ via streams separados
- Fan-out SNS para subscribers SQS/HTTP/HTTPS com filter policy
- mTLS entre shim e broker
- Métricas Prometheus, logs JSON estruturados, tracing OpenTelemetry

### Backend Postgres (alternativo)

Para quem já opera Postgres em produção e quer evitar operar um segundo
sistema de mensageria, `GQ_BACKEND=postgres` troca o NATS JetStream por
tabelas relacionais + `LISTEN/NOTIFY` + `SELECT ... FOR UPDATE SKIP LOCKED`:

- `SKIP LOCKED` para claim de mensagens sem duplicidade sob concorrência
- Long-polling via `LISTEN/NOTIFY` (canal único, sem 1 conexão por
  long-poll) com poll de segurança contra notificação perdida
- FIFO (`MessageGroupId`, `MessageDeduplicationId`,
  `ContentBasedDeduplication`) com paridade funcional completa com o NATS
- Custo/operação menor para volume moderado; ver ressalva de escala em
  [`docs/architecture.md`](docs/architecture.md#backend-postgres)

```bash
make up-postgres         # sobe Postgres 16 + shim (porta 4567)
make test-int-postgres   # roda a MESMA suíte E2E (72 testes) contra ele
```

---

## Status atual

> Instruções para agentes IA (Claude Code, Aider, Cursor, etc) que venham a trabalhar
> neste repo: ver [`AGENTS.md`](AGENTS.md) na raiz e [`docs/gotchas.md`](docs/gotchas.md)
> antes de implementar qualquer coisa. Em especial: **atualize o README em toda mudança
> visível ao usuário** — esta seção é a fonte da verdade sobre o que está pronto.

**Fase 1 (SQS Standard — leitura/metadados) ✅ completa.**
**Fase 2 (SQS Standard — operações de mensagem) ✅ completa.**
**Fase 3 (Batch + FIFO completo) ✅ completa.**
**Fase 4 (SNS — tópicos + subscriptions + fan-out + filter policy) ✅ completa.**

### Operações SQS implementadas (13/13 do Standard)

| Operação                 | Protocolo Query | Protocolo JSON | Testes E2E |
|--------------------------|:--------------:|:--------------:|:----------:|
| `CreateQueue`            | ✅             | ✅             | ✅         |
| `GetQueueUrl`            | ✅             | ✅             | ✅         |
| `GetQueueAttributes`     | ✅             | ✅             | ✅         |
| `ListQueues`             | ✅             | ✅             | ✅         |
| `DeleteQueue`            | ✅             | ✅             | ✅         |
| `SetQueueAttributes`     | ✅             | ✅             | ✅         |
| `SendMessage`            | ✅             | ✅             | ✅         |
| `ReceiveMessage`         | ✅             | ✅             | ✅         |
| `DeleteMessage`          | ✅             | ✅             | ✅         |
| `ChangeMessageVisibility`| ✅             | ✅             | ✅         |
| `PurgeQueue`             | ✅             | ✅             | ✅         |
| `SendMessageBatch`       | ✅             | ✅             | ✅         |
| `DeleteMessageBatch`     | ✅             | ✅             | ✅         |

### Operações SNS implementadas (9/9 do MVP)

| Operação                   | Protocolo Query | Protocolo JSON | Testes E2E |
|----------------------------|:--------------:|:--------------:|:----------:|
| `CreateTopic`              | ✅             | ✅             | ✅         |
| `GetTopicAttributes`       | ✅             | ✅             | ✅         |
| `ListTopics`               | ✅             | ✅             | ✅         |
| `DeleteTopic`              | ✅             | ✅             | ✅         |
| `Subscribe` (sqs)          | ✅             | ✅             | ✅         |
| `Unsubscribe`              | ✅             | ✅             | ✅         |
| `Publish`                  | ✅             | ✅             | ✅         |
| `ListSubscriptions`        | ✅             | ✅             | ✅         |
| `ListSubscriptionsByTopic` | ✅             | ✅             | ✅         |

**Funcionalidades SNS:**
- **Fan-out síncrono** de mensagens para todas as subscriptions SQS inscritas
- **Filter policy** (`{"type":["alert"]}`) aplicado antes da entrega
- **Idempotência** de CreateTopic (mesmo nome → mesmo ARN) e DeleteTopic
  (não falha em ARN inexistente)
- **Cascade**: DeleteTopic remove subscriptions órfãs automaticamente
- **ConfirmSubscription** stub (UnsupportedOperation no MVP; subscriptions
  HTTP/HTTPS ficam pending)

**Cobertura de testes:** 72/72 passando (12 SQS smoke + 15 SQS messages +
18 SQS batch + 7 SQS limits + 20 SNS) em ~50s contra shim rodando em
docker-compose com NATS JetStream 2.14 + MinIO.

### Funcionalidades SQS implementadas

- **Dual protocol**: Query (form+XML) E JSON 1.0 simultaneamente, mesmo response
- **Dual format JSON 1.0**: `Attributes` array E map compacto (boto3 ≥1.40)
- **MessageAttribute round-trip**: DataType preservado (String, Number, Binary base64, String.List)
- **Long-polling nativo** via `FetchMaxWait` do JetStream
- **Visibility timeout** via `AckWait` + `NakWithDelay`
- **Receipt handles stateless** `rh2:<base64url(reply-subject)>` — o handle
  carrega o reply subject `$JS.ACK` do JetStream; DeleteMessage funciona em
  **qualquer réplica** do shim (sem sticky session, sem estado local)
- **Consumer durável por fila** criado no CreateQueue e cacheado
  (AckExplicitPolicy, `GQ_MAX_ACK_PENDING`, default 1000) — múltiplas
  réplicas compartilham o mesmo pull consumer
- **Retention WorkQueue**: mensagem consumida (acked) é apagada do disco na
  hora — custo de armazenamento proporcional ao backlog, não ao throughput
- **Backlog cheio rejeita publish** (`DiscardNew` → erro `OverLimit`);
  mensagens antigas nunca são descartadas silenciosamente
- **Validação de MessageAttributes**: máx 10 por mensagem; tamanho dos
  atributos conta no limite de 256 KiB (igual AWS)
- **ApproximateReceiveCount real** via `NumDelivered` do JetStream
- **FIFO queues completas**:
  - MessageGroupId particiona subject NATS → ordering dentro do grupo preservado
  - MessageDeduplicationId via Nats-Msg-Id (dedup explícito)
  - ContentBasedDeduplication via SHA-256(body) como Nats-Msg-Id (dedup implícito)
  - SequenceNumber retornado em cada send (MessageId = stream sequence)
- **Batch operations**:
  - SendMessageBatch até 10 entries com partial success (Failed[] por entry)
  - DeleteMessageBatch até 10 entries com partial success
  - Validação de Ids únicos (BatchEntryIdsNotDistinct)
  - Validação de limite (TooManyEntriesInBatch)
  - Validação de soma de bodies ≤ 256 KiB
- **44 commits granulares em português** (até final da Fase 4), cada um com mensagem detalhada

### O que vem a seguir

- **Fase 5 — Auth real:** Verificação SigV4 + IAM store (policy JSON evaluation) + CLI `shimctl`
- **Fase 6 — Provisioning:** Terraform módulos production-ready (network,
  compute, broker, api, observability, objectstore)
- **Fase 7 — Hardening produção:** HA multi-AZ, backup/restore, runbooks,
  SLOs, alerting rules

### Limitações conhecidas (MVP)

- **AUTH_MODE=off** — sem verificação SigV4 no dev; qualquer credencial é aceita
- **Sem IAM** — policy evaluation não implementada
- **SNS subscriptions HTTP/HTTPS ficam pending** — `ConfirmSubscription`
  é stub (retorna `UnsupportedOperation`); apenas protocol `sqs`
  está totalmente funcional para fan-out
- **Fan-out SNS é síncrono** (bound de 16 deliveries paralelas); latência do
  Publish cresce com o número de subscriptions. Fan-out assíncrono durável
  está no roadmap
- **VisibilityTimeout por request** diferente do default da fila atualiza o
  `AckWait` do consumer compartilhado (afeta a fila toda, não só o request)

### Migração de versões antigas

- **Retention mudou de LimitsPolicy → WorkQueuePolicy** e não pode ser
  alterada em stream existente: filas criadas por versões antigas precisam
  ser recriadas (`make down-v` em dev apaga tudo)
- **Receipt handles `rh1:` foram substituídos por `rh2:`** — handles em voo
  de versões antigas são rejeitados com `ReceiptHandleIsInvalid`
- **Produção agora exige `GQ_STREAM_REPLICAS=3`** quando `GQ_AUTH_MODE` é
  `verify`/`strict` — a config é rejeitada com 1 réplica (fail-loud em vez
  de rodar sem HA silenciosamente)

---

## Quickstart (desenvolvimento local)

### Pré-requisitos

- Go 1.26+
- Docker 24+ e Docker Compose v2+
- Python 3.10+ (para testes de integração com boto3)
- Make (opcional)

### Subindo o stack dev

```bash
# Sobe NATS JetStream + MinIO + dropin-server em background
make up

# Aguarda ~5s e roda o smoke test
make smoke

# Você deve ver:
# test/integration/test_sqs_smoke.py::test_create_queue PASSED
# test/integration/test_sqs_messages.py::test_send_and_receive_single PASSED
# test/integration/test_sqs_messages.py::test_delete_message PASSED
# ... 65 passed in ~60s

# Inspecionar logs do shim
make logs-shim

# Derrubar tudo
make down
```

### Usando o shim com boto3 (Python)

```python
import boto3

sqs = boto3.client(
    "sqs",
    endpoint_url="http://localhost:4566",  # porta do shim
    region_name="us-east-1",
    aws_access_key_id="AKIATEST",
    aws_secret_access_key="secret",
)

resp = sqs.create_queue(QueueName="my-queue")
print(resp["QueueUrl"])
# http://localhost:4566/000000000000/my-queue
```

### Usando o shim com aws-cli

```bash
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    sqs create-queue --queue-name my-queue
```

> **Importante:** no modo dev (`AUTH_MODE=off`), o shim aceita qualquer credencial
> e não valida assinatura SigV4. Use apenas para desenvolvimento e testes.

### Configuração (env vars / flags)

| Env var (`GQ_*`)      | Flag                 | Default   | Descrição |
|-----------------------|----------------------|-----------|-----------|
| `GQ_ADDR`             | `--addr`             | `:4566`   | Endereço HTTP |
| `GQ_BACKEND`          | `--backend`          | `nats`    | `nats\|postgres` — nunca os dois ao mesmo tempo |
| `GQ_AUTH_MODE`        | `--auth-mode`        | `off`     | `off\|verify\|strict` |
| `GQ_MAX_BODY_BYTES`   | `--max-body-bytes`   | `262144`  | Tamanho máx de request body |
| `GQ_SHUTDOWN_TIMEOUT` | `--shutdown-timeout` | `30s`     | Shutdown gracioso |

**Backend `nats`:**

| Env var (`GQ_*`)      | Flag                 | Default   | Descrição |
|-----------------------|----------------------|-----------|-----------|
| `GQ_NATS_URL`         | `--nats-url`         | `nats://localhost:4222` | Broker (suporta `tls://`) |
| `GQ_STREAM_REPLICAS`  | `--stream-replicas`  | `1`       | Réplicas Raft por stream (1/3/5). **Produção = 3**; `verify`/`strict` com 1 réplica é rejeitado |
| `GQ_MAX_ACK_PENDING`  | `--max-ack-pending`  | `1000`    | Máx mensagens in-flight por fila |
| `GQ_TOPIC_MAX_AGE`    | `--topic-max-age`    | `1h`      | Retenção do stream de arquivo dos tópicos SNS |

**Backend `postgres`:**

| Env var (`GQ_*`)              | Flag                        | Default | Descrição |
|--------------------------------|-----------------------------|---------|-----------|
| `GQ_POSTGRES_DSN`              | `--postgres-dsn`            | (obrigatório) | `postgres://user:pass@host:5432/db` |
| `GQ_POSTGRES_MAX_CONNS`        | `--postgres-max-conns`      | `20`    | Tamanho do pool de conexões |
| `GQ_POSTGRES_POLL_INTERVAL`    | `--postgres-poll-interval`  | `300ms` | Poll de segurança do long-poll (fallback do NOTIFY) |
| `GQ_POSTGRES_NOTIFY_COALESCE`  | `--postgres-notify-coalesce`| `20ms`  | Intervalo mínimo entre NOTIFYs da mesma fila |

Lista completa em `dropin-server --help` (`GQ_ACCOUNT_ID`, `GQ_REGION`,
`GQ_LOG_LEVEL`, `GQ_METRICS_ADDR`, `GQ_NATS_CREDS`, `GQ_NATS_CA_CERT`).

---

## Arquitetura

```
┌─────────────────────────────────────────────┐
│  AWS SDK / boto3 / aws-sdk-go-v2 / aws-cli  │
└──────────────────────┬──────────────────────┘
                       │ HTTPS · SigV4
                       ▼
┌─────────────────────────────────────────────┐
│  dropin-server (Go, stateless, N réplicas)  │
│  ┌─────────────┬──────────────┬──────────┐  │
│  │ awssig      │ protocol     │ iam      │  │
│  ├─────────────┴──────────────┴──────────┤  │
│  │ sqs · sns services                     │  │
│  ├───────────────────────────────────────┤  │
│  │ storage adapter interface            │  │
│  └─────────────┬─────────────────────────┘  │
│                │ mTLS                        │
└────────────────┼─────────────────────────────┘
                 ▼
┌─────────────────────────────────────────────┐
│  NATS JetStream cluster (3 nós, Raft)      │
└────────────────┬────────────────────────────┘
                 │ snapshots
                 ▼
┌─────────────────────────────────────────────┐
│  Object Storage (S3/GCS/Azure Blob)        │
└─────────────────────────────────────────────┘
```

Detalhes em [`docs/architecture.md`](docs/architecture.md).

---

## Estrutura do repositório

```
dropin-queue/
├── shim/                # API shim em Go (o coração do projeto)
│   ├── cmd/dropin-server/   # entrypoint
│   ├── internal/        # código privado (awssig, protocol, sqs, sns, iam, storage, ...)
│   ├── pkg/types/       # tipos públicos compartilhados
│   └── test/            # testes de integração (boto3 contra o shim)
├── terraform/           # IaC cloud-agnostic (em breve)
├── ops/                 # scripts, ansible, runbooks (em breve)
├── docs/                # documentação detalhada
└── docker-compose.yml   # stack dev local
```

---

## Desenvolvimento

### Comandos úteis

```bash
make help              # lista todos os targets
make up                # sobe docker-compose (backend NATS)
make up-postgres        # sobe docker-compose (backend Postgres, profile "postgres")
make down              # derruba docker-compose (todos os backends)
make build             # builda dropin-server
make test              # roda testes Go (adapter Postgres pula sem GQ_TEST_POSTGRES_DSN)
make test-int          # roda testes de integração contra o backend NATS
make test-int-postgres  # roda a MESMA suíte de integração contra o backend Postgres
make smoke             # roda o smoke test rápido
make lint              # roda golangci-lint
make fmt               # formata código Go
make logs-shim         # tail logs do shim
make shell-shim        # shell dentro do container do shim
```

### Estrutura de pastas do shim

Cada pacote em `shim/internal/` é isolado e testável:

- `awssig/` — verificação de AWS Signature v4
- `protocol/` — parsers/serializers dos protocolos AWS (Query + JSON)
- `sqs/` — implementação das operações SQS
- `sns/` — implementação das operações SNS
- `iam/` — avaliação de policy JSON
- `storage/` — interface + adapters (`storage/nats/` e `storage/postgres/`, trocáveis via `GQ_BACKEND`)
- `server/` — HTTP router + middleware
- `observability/` — métricas, tracing, logging
- `config/` — carregamento de configuração

### Testes

- **Unitários**: `go test ./shim/...` (não precisam de infra; os testes do
  adapter `storage/postgres` pulam automaticamente sem `GQ_TEST_POSTGRES_DSN`)
- **Integração — backend NATS**: `make test-int` (sobe docker-compose, roda pytest contra boto3)
- **Integração — backend Postgres**: `make test-int-postgres` (mesma suíte
  pytest, zero mudança nos arquivos de teste — eles falam HTTP/boto3, não
  sabem qual backend está por trás)
- **E2E contra ambiente real**: planejado para a Fase 6 (Terraform) —
  o target `make test-e2e` ainda não existe

---

## Compatibilidade AWS — Status atual

Esta seção reflete o que está **implementado e testado E2E** (65/65 testes passando).
Tabela detalhada com cobertura por protocolo também está no topo do README.
Acompanhe o progresso futuro em [`docs/api-compatibility.md`](docs/api-compatibility.md).

### SQS Standard (13/13)

- [x] `CreateQueue`
- [x] `GetQueueUrl`
- [x] `GetQueueAttributes`
- [x] `ListQueues`
- [x] `DeleteQueue`
- [x] `SetQueueAttributes`
- [x] `SendMessage`
- [x] `ReceiveMessage` (long-poll nativo via JetStream `FetchMaxWait`)
- [x] `DeleteMessage`
- [x] `ChangeMessageVisibility` (via `AckWait` + `NakWithDelay`)
- [x] `PurgeQueue`
- [x] DLQ (streams separados)

### SQS FIFO

- [x] `MessageGroupId` (particiona subject NATS → ordering dentro do grupo)
- [x] `MessageDeduplicationId` (via `Nats-Msg-Id` para dedup explícito)
- [x] `ContentBasedDeduplication` (SHA-256(body) → `Nats-Msg-Id` automático)

### SQS Batch

- [x] `SendMessageBatch` (≤10 entries, partial success via `Failed[]`)
- [x] `DeleteMessageBatch` (≤10 entries, partial success)
- [ ] `ChangeMessageVisibilityBatch` — **não implementado** (próxima iteração)

### SNS (9/9)

- [x] `CreateTopic`
- [x] `GetTopicAttributes`
- [x] `ListTopics`
- [x] `DeleteTopic` (com cascade — remove subscriptions órfãs)
- [x] `Subscribe`
  - [x] protocol `sqs` (auto-confirmed, fan-out funciona)
  - [ ] protocol `http` (pending — `ConfirmSubscription` stub)
  - [ ] protocol `https` (pending — `ConfirmSubscription` stub)
- [x] `Unsubscribe`
- [x] `Publish` (fan-out síncrono para subscribers SQS)
- [x] `ListSubscriptions`
- [x] `ListSubscriptionsByTopic`
- [x] `ConfirmSubscription` — **stub** (retorna `UnsupportedOperation` no MVP)
- [x] `FilterPolicy` MVP (match exato `{key: [allowed_values]}`)

### Auth & IAM (Fase 5 — próxima)

- [ ] Verificação SigV4
- [ ] IAM store + policy JSON evaluation
- [ ] CLI `shimctl`

### Infra (Fases 6-7)

- [ ] Terraform módulos production-ready
- [ ] HA multi-AZ + snapshots automáticos
- [x] Observability **no shim** (Prometheus metrics + logs JSON estruturados
      + OpenTelemetry tracing — tudo exposto em `/metrics` e OTLP endpoint)
- [ ] Observability **stack completo** (Grafana dashboards, Loki log aggregation,
      alerting rules)
- [ ] Runbooks (backup, restore, failover, incident response)

---

## Contribuindo

Em construção.

## Licença

A definir.
