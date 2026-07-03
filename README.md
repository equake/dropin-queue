# generic_queue

> Clone auto-hospedável, compatível com o protocolo AWS SNS/SQS, construído em Go sobre NATS JetStream.

`generic_queue` oferece uma API HTTP que fala os mesmos protocolos (`Query` e `JSON`) e devolve
as mesmas respostas que os serviços gerenciados da AWS. Você aponta qualquer cliente oficial
(`boto3`, `aws-sdk-go-v2`, `aws-sdk-js`, `aws-sdk-java`, `aws-cli`, SDK da sua linguagem favorita)
para o endpoint do shim e tudo funciona como antes — sem precisar de uma conta AWS.

A camada de mensageria por baixo é o **NATS JetStream**, que entrega:

- Cluster Raft com 3 ou 5 nós (alta disponibilidade real)
- Persistência em disco + snapshots em object storage (S3/GCS/Azure Blob)
- Pull consumers com `expires` (long-polling nativo, igual ao `WaitTimeSeconds`)
- `AckWait` nativo (visibility timeout sem scheduler custom)
- Dedup nativo via `Nats-Msg-Id` (compatível com `MessageDeduplicationId` do SQS FIFO)
- Single binary, ~6 MB de RAM cold start

A camada de API é um **shim em Go** (`shimd`) que implementa:

- Parsers/serializers dos protocolos AWS SQS Query, SQS JSON, SNS Query, SNS JSON
- Verificação de **AWS Signature v4** (SigV4) com IAM real (policy JSON evaluation)
- Long-polling via `Fetch` do JetStream com `MaxWait`
- Visibility timeout scheduler
- DLQ via streams separados
- Fan-out SNS para subscribers SQS/HTTP/HTTPS com filter policy
- mTLS entre shim e broker
- Métricas Prometheus, logs JSON estruturados, tracing OpenTelemetry

---

## Status atual

**Fase 1 (SQS Standard — leitura/metadados) ✅ completa.**
**Fase 2 (SQS Standard — operações de mensagem) ✅ completa.**
**Fase 3 (Batch + FIFO completo) ✅ completa.**

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

**Cobertura de testes:** 45/45 passando (12 smoke + 15 messages + 18 batch)
em ~18s contra shim rodando em docker-compose com NATS JetStream 2.10 + MinIO.

### Funcionalidades SQS implementadas

- **Dual protocol**: Query (form+XML) E JSON 1.0 simultaneamente, mesmo response
- **Dual format JSON 1.0**: `Attributes` array E map compacto (boto3 ≥1.40)
- **MessageAttribute round-trip**: DataType preservado (String, Number, Binary base64, String.List)
- **Long-polling nativo** via `FetchMaxWait` do JetStream
- **Visibility timeout** via `AckWait` + `NakWithDelay`
- **Receipt handles versionados** `rh1:<consumer>:<seq>` com cache de ack
- **Consumer durável único por fila** (AckExplicitPolicy, MaxAckPending=1000)
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
- **31 commits granulares em português**, cada um com mensagem detalhada

### O que vem a seguir

- **Fase 4 — SNS:** `CreateTopic`, `Subscribe` (sqs/http/https), `Publish`,
  `ListSubscriptions`, `Unsubscribe`, `DeleteTopic`, fan-out com filter policy
- **Fase 5 — Auth real:** Verificação SigV4 + IAM store (policy JSON evaluation) + CLI `shimctl`
- **Fase 6 — Provisioning:** Terraform módulos production-ready (network,
  compute, broker, api, observability, objectstore)
- **Fase 7 — Hardening produção:** HA multi-AZ, backup/restore, runbooks,
  SLOs, alerting rules

### Limitações conhecidas (MVP)

- **AUTH_MODE=off** — sem verificação SigV4 no dev; qualquer credencial é aceita
- **Sem IAM** — policy evaluation não implementada
- **Consumer único por fila** — não suporta múltiplos clients paralelos
  consumindo da mesma fila (SQS Standard permite). Solução em prod: sharding
  por partition key (FIFO) ou round-robin assignment (Standard)
- **AproximateNumberOfMessages** é contado a partir de `Stream.State.Msgs`
  que inclui mensagens já acked (lag de update do JetStream)
- **Sem SNS ainda** — apenas SQS; subscriptions HTTP/HTTPS precisam de job
  server assíncrono

---

## Quickstart (desenvolvimento local)

### Pré-requisitos

- Go 1.22+
- Docker 24+ e Docker Compose v2+
- Python 3.10+ (para testes de integração com boto3)
- Make (opcional)

### Subindo o stack dev

```bash
# Sobe NATS JetStream + MinIO + shimd em background
make up

# Aguarda ~5s e roda o smoke test
make smoke

# Você deve ver:
# test/integration/test_sqs_smoke.py::test_create_queue PASSED
# test/integration/test_sqs_messages.py::test_send_and_receive_single PASSED
# test/integration/test_sqs_messages.py::test_delete_message PASSED
# ... 27 passed in ~14s

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

---

## Arquitetura

```
┌─────────────────────────────────────────────┐
│  AWS SDK / boto3 / aws-sdk-go-v2 / aws-cli  │
└──────────────────────┬──────────────────────┘
                       │ HTTPS · SigV4
                       ▼
┌─────────────────────────────────────────────┐
│  shimd (Go, stateless, N réplicas)          │
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
generic_queue/
├── shim/                # API shim em Go (o coração do projeto)
│   ├── cmd/shimd/       # entrypoint
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
make up                # sobe docker-compose
make down              # derruba docker-compose
make build             # builda shimd
make test              # roda testes Go
make test-int          # roda testes de integração (boto3 + shim rodando)
make smoke             # roda o smoke test da semana 1
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
- `storage/` — interface + adapter NATS JetStream
- `server/` — HTTP router + middleware
- `observability/` — métricas, tracing, logging
- `config/` — carregamento de configuração

### Testes

- **Unitários**: `go test ./shim/...` (não precisam de infra)
- **Integração**: `make test-int` (sobe docker-compose, roda pytest contra boto3)
- **E2E**: `make test-e2e` (aponta para ambiente real provisionado por Terraform)

---

## Compatibilidade AWS — Roadmap

Acompanhe em [`docs/api-compatibility.md`](docs/api-compatibility.md) (em construção).

### SQS Standard

- [x] `CreateQueue` — Semana 1
- [ ] `SendMessage` — Semana 3
- [ ] `ReceiveMessage` (long-poll) — Semana 3
- [ ] `DeleteMessage` — Semana 3
- [ ] `ChangeMessageVisibility` — Semana 3
- [ ] `PurgeQueue` — Semana 3
- [ ] `DeleteQueue` — Semana 3
- [ ] `GetQueueAttributes` — Semana 3
- [ ] `GetQueueUrl` — Semana 3
- [ ] `ListQueues` — Semana 3
- [ ] `SetQueueAttributes` — Semana 3
- [ ] DLQ — Semana 3

### SQS FIFO

- [ ] `MessageGroupId` — Semana 3
- [ ] `MessageDeduplicationId` — Semana 3

### SQS Batch

- [ ] `SendMessageBatch` — Semana 3
- [ ] `DeleteMessageBatch` — Semana 3
- [ ] `ChangeMessageVisibilityBatch` — Semana 3

### SNS

- [ ] `CreateTopic` — Semana 4
- [ ] `Subscribe` (sqs/http/https) — Semana 4
- [ ] `Publish` — Semana 4
- [ ] `ListSubscriptions` — Semana 4
- [ ] `Unsubscribe` — Semana 4
- [ ] `DeleteTopic` — Semana 4
- [ ] `ConfirmSubscription` — Semana 4
- [ ] `FilterPolicy` — Semana 4

### Auth & IAM

- [ ] Verificação SigV4 — Semana 4
- [ ] IAM store + policy evaluation — Semana 4
- [ ] `shimctl` CLI — Semana 4

### Infra

- [ ] Terraform modules — Semanas 5-6
- [ ] HA + snapshots — Semana 5
- [ ] Observability (Prometheus, Grafana, Loki, OTel) — Semana 5
- [ ] Runbooks — Semana 6

---

## Contribuindo

Em construção.

## Licença

A definir.
