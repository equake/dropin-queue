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

**Fase 1 — Semana 1 (setup inicial).**

O que já funciona:

- Bootstrap do projeto (Go module, docker-compose dev, Makefile, CI)
- HTTP server com middleware (logging, recovery, métricas)
- Cliente NATS JetStream com reconexão automática
- Primeira operação SQS implementada: `CreateQueue` (protocolo Query)
- Smoke test ponta-a-ponta: `boto3` cria fila contra o shim com sucesso

O que vem a seguir:

- SQS Standard: `SendMessage`, `ReceiveMessage` (long-poll), `DeleteMessage`,
  `ChangeMessageVisibility`, `PurgeQueue`, `DeleteQueue`, `GetQueueAttributes`,
  `GetQueueUrl`, `ListQueues`, DLQ
- SNS: `CreateTopic`, `Subscribe` (sqs/http/https), `Publish`, `ListSubscriptions`,
  `Unsubscribe`, `DeleteTopic`, `ConfirmSubscription`
- SQS FIFO: `MessageGroupId`, `MessageDeduplicationId`, ordering
- Batch: `SendMessageBatch`, `DeleteMessageBatch`
- Verificação SigV4 + IAM store
- Terraform módulos (network, compute, broker, api, observability, objectstore)
- Hardening produção (HA, backup/restore, runbooks)

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
