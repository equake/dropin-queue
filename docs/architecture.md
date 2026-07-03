# Arquitetura do generic_queue

Este documento descreve a arquitetura do `generic_queue`, um clone
auto-hospedável, compatível com o protocolo AWS SNS/SQS, construído em Go
sobre NATS JetStream.

> Status: Fase 3 completa — 13 operações SQS Standard funcionais
> (CreateQueue/Get/List/Delete + SendMessage/ReceiveMessage/DeleteMessage/
> ChangeMessageVisibility/PurgeQueue + SetQueueAttributes + SendMessageBatch/
> DeleteMessageBatch), todas com dual protocol (Query+JSON), todas com
> testes E2E (45/45 passando) via boto3 oficial Python.
> Inclui FIFO queues completas: MessageGroupId, MessageDeduplicationId,
> ContentBasedDeduplication, SequenceNumber, ordering within group.

## Visão geral

```
┌───────────────────────────────────────────────────────────────┐
│                  AWS SDK / boto3 / aws-cli                    │
│  (Python, Go, Node.js, Java, Ruby, Rust, .NET, aws-cli)       │
└─────────────────────────┬─────────────────────────────────────┘
                          │ HTTPS · SigV4 (ou dummy em dev)
                          ▼
┌───────────────────────────────────────────────────────────────┐
│              shimd — API compatível AWS (Go)                  │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │  server/   chi router · middleware · handlers AWS      │  │
│  ├─────────────────────────────────────────────────────────┤  │
│  │  protocol/ SQS Query (form+XML) · SQS JSON 1.0         │  │
│  ├─────────────────────────────────────────────────────────┤  │
│  │  awssig/    [Semana 4] verificação de AWS Signature v4 │  │
│  ├─────────────────────────────────────────────────────────┤  │
│  │  iam/       [Semana 4] policy evaluation (JSON IAM)    │  │
│  ├─────────────────────────────────────────────────────────┤  │
│  │  sqs/       validação semântica + tradução erros       │  │
│  │  sns/       [Semana 4] fan-out · filter policy         │  │
│  ├─────────────────────────────────────────────────────────┤  │
│  │  storage/   interface pluggable (broker-agnostic)      │  │
│  ├─────────────────────────────────────────────────────────┤  │
│  │  observability/  slog JSON · Prometheus · OTel         │  │
│  └─────────────────────────────────────────────────────────┘  │
└─────────────────────────┬─────────────────────────────────────┘
                          │ mTLS (prod) · plain (dev)
                          ▼
┌───────────────────────────────────────────────────────────────┐
│              NATS JetStream cluster (3 nós, Raft)            │
│  ┌────────────────────┐    ┌────────────────────┐             │
│  │  Streams (filas)   │    │  KV (metadados)    │             │
│  │  "queue-<name>"    │    │  "queue_meta"      │             │
│  │  subjects: q.X.>   │    │  attrs por fila    │             │
│  └────────────────────┘    └────────────────────┘             │
└─────────────────────────┬─────────────────────────────────────┘
                          │ snapshots / file storage chunks
                          ▼
┌───────────────────────────────────────────────────────────────┐
│  Object Storage — S3 / GCS / Azure Blob / MinIO (dev)         │
│  Usado para File Storage do JetStream (snapshots + chunks)    │
└───────────────────────────────────────────────────────────────┘
```

## Camadas do shim

### 1. `cmd/shimd` — entrypoint

Responsabilidade: lifecycle do processo (config → logger → metrics →
tracing → storage → services → HTTP server → shutdown gracioso).

Não tem lógica de negócio. Toda orquestração fica aqui.

### 2. `server/` — HTTP router

- **Roteamento**: chi router
- **POST /**: entrypoint AWS (todas operações SQS/SNS)
- **GET /healthz**: liveness probe
- **GET /readyz**: readiness probe (chama ListQueues no broker)
- **GET /metrics**: Prometheus

Middleware (ordem importa):

1. **requestIDMiddleware**: extrai X-Request-ID do cliente ou gera novo
2. **recoveryMiddleware**: captura panics e devolve 500 com envelope AWS
3. **loggingMiddleware**: slog estruturado com method/path/status/latency
4. **metricsMiddleware**: IncInflight/ObserveHTTP

Detecção de protocolo:

```go
func isJSONProtocol(r *http.Request) bool {
    target := r.Header.Get("X-Amz-Target")
    ct := r.Header.Get("Content-Type")
    return target != "" && strings.HasPrefix(ct, "application/x-amz-json")
}
```

### 3. `protocol/` — AWS wire protocol

Dois protocolos suportados simultaneamente:

#### Query (form-encoded + XML)
- Body: `application/x-www-form-urlencoded`
- Action: `Action=CreateQueue` no body
- Response: `<?xml version="1.0"?><CreateQueueResponse xmlns="...">...`

#### JSON 1.0
- Body: `application/x-amz-json-1.0`
- Action: `X-Amz-Target: AmazonSQS.CreateQueue`
- Response: `{"QueueUrl": "..."}`

Operações conhecidas validadas em `IsValidAction(service, action)`.
Erros AWS formatados com códigos oficiais (`QueueDoesNotExist`, etc.)

### 4. `sqs/` — SQS service

Lógica de negócio SQS. Conhece o protocolo AWS mas não o broker.

Operações atuais (Fase 3 completa — 13/13 operações do SQS Standard):

- **Queue management**: CreateQueue, GetQueueUrl, GetQueueAttributes,
  ListQueues, DeleteQueue, SetQueueAttributes
- **Message operations**: SendMessage, ReceiveMessage (long-poll),
  DeleteMessage, ChangeMessageVisibility, PurgeQueue
- **Batch operations**: SendMessageBatch (≤10 entries, partial success),
  DeleteMessageBatch (≤10 entries, partial success)

Erros:

- `QueueDoesNotExist`, `QueueAlreadyExists`, `InvalidParameterValue`,
  `MissingParameter`, `OverLimit`, `MessageTooLarge`,
  `ReceiptHandleIsInvalid`, `BatchEntryIdsNotDistinct`,
  `TooManyEntriesInBatch`, `EmptyBatchRequest`, `UnsupportedOperation`,
  `InternalError`
- Tradução storage → AWS via `AsAWSError(err)` com `errors.As` para
  erros tipados (ErrInvalidReceiptHandleT, ErrMessageTooLargeT, etc.)

FIFO queues (Fase 3):

- MessageGroupId particiona subject NATS (`q.<queue>.<groupId>`) →
  ordering within group preservado nativamente
- MessageDeduplicationId via Nats-Msg-Id (dedup explícito em janela de 2min)
- ContentBasedDeduplication via SHA-256(body) como Nats-Msg-Id
  (dedup implícito em janela de 2min)
- SequenceNumber retornado em cada send (MessageId = stream sequence)

### 5. `sns/` — SNS service [Semana 4]

Stub atual. Implementará fan-out com filter policy em subscribers SQS
e HTTP/HTTPS.

### 6. `storage/` — broker abstraction

#### Interface

```go
type Storage interface {
    Queues() QueueStorage
    Messages() MessageStorage
    Topics() TopicStorage
    Close() error
}
```

#### Adapter NATS JetStream (`storage/nats/`)

Mapeamento:

| Conceito AWS          | Implementação JetStream                            |
|-----------------------|----------------------------------------------------|
| Fila SQS              | Stream `queue-<sanitized>` + subjects `q.<name>.>` |
| Mensagem              | Mensagem JetStream no subject                      |
| MessageGroupId (FIFO) | Subject particionado por hash do groupId           |
| MessageDeduplicationId| Header `Nats-Msg-Id` (dedup nativo JetStream)      |
| Visibility timeout    | AckWait (nativo JetStream)                         |
| Long-polling          | Fetch com MaxWait (nativo JetStream)               |
| Atributos da fila     | KV bucket `queue_meta` (separado dos streams)      |
| Tópico SNS            | Stream `topic-<sanitized>` + subject `t.<name>`    |
| Subscription SQS      | Consumer durável que republica no stream da queue  |
| Subscription HTTP/HTTPS | Consumer + worker entrega POST                    |

Por que KV separado para atributos:

- Mudanças em VisibilityTimeout não exigem recriar o stream
- Streams JetStream são para mensagens; metadata é orthogonal
- Permite evoluir schema sem migração de streams

### 7. `observability/` — telemetria

- **Logging**: slog JSON em prod, text colorido em dev
- **Métricas**: Prometheus (shim_http_*, shim_storage_*, shim_sqs_*,
  shim_sns_*)
- **Tracing**: OTel SDK com stdout exporter em dev (OTLP em prod [Semana 6])

### 8. `awssig/` — SigV4 [Semana 4]

Verificação de AWS Signature v4. Em modo dev (`AUTH_MODE=off`),
qualquer credencial é aceita.

### 9. `iam/` — IAM [Semana 4]

Avaliação de policy JSON (subset IAM). Suporta Action, Resource,
Effect, Condition básico.

## Fluxo end-to-end (CreateQueue)

```
[boto3]
   │ cliente calcula SigV4 (AWS4-HMAC-SHA256)
   │ POST / com body JSON: {"QueueName": "x", "Attributes": {...}}
   │ Headers: X-Amz-Target, Content-Type, Authorization
   ▼
[server.handleAWS]
   │ isJSONProtocol(r) → true
   │ → handleAWSJSON
   ▼
[protocol.ParseSQSJSONRequest]
   │ valida headers, parse JSON, valida Action
   ▼
[server.handleCreateQueueJSON]
   │ sqs.CreateQueueParamsFromJSON(params) → normaliza
   │ → s.handlers.SQS.CreateQueue(ctx, cqp)
   ▼
[sqs.Service.CreateQueue]
   │ valida QueueName (1-80 chars, [A-Za-z0-9._-])
   │ valida ranges atributos
   │ → s.storage.Queues().CreateQueue(ctx, q)
   ▼
[nats.Client.CreateQueue]
   │ CreateStream (JetStream) com Retention=Limits, Replicas=1 dev/3 prod
   │ saveQueueMetadata (KV bucket queue_meta)
   ▼
[NATS JetStream]
   │ Stream criado, persistido em disco
   │ KV entry criada com VisibilityTimeout, etc.
   ▼
[Resposta volta]
   │ Client.GetQueue carrega metadata do KV
   │ → URL + ARN preenchidos
   ▼
[protocol.EncodeSQSJSONResponse]
   │ {"QueueUrl": "http://localhost:4566/000000000000/x"}
   ▼
[boto3]
   │ Parse JSON → QueueUrl
```

## Decisões arquiteturais

### Por que NATS JetStream em vez de goaws/ElasticMQ/etc.

| Alternativa       | Decisão | Razão                                  |
|-------------------|---------|----------------------------------------|
| goaws             | Rejeitado | Fila interna, não battle-tested broker |
| ElasticMQ         | Rejeitado | SQS-only, JVM pesado                   |
| LocalStack        | Rejeitado | Projetado para dev, pesado em prod     |
| beyond/queue      | Rejeitado | Muito novo (maio/2026), pouca tração   |
| **NATS JetStream**| **Escolhido** | SQS semantics mapeiam 1:1              |

Mapeamentos críticos:

- **Long-polling**: NATS `Fetch(MaxWait)` ↔ SQS `WaitTimeSeconds`
- **Visibility timeout**: NATS `AckWait` ↔ SQS visibility timeout
- **FIFO + dedup**: NATS subject particionado + `Nats-Msg-Id`
- **HA**: Raft nativo, 3 nós (tolera perda de 1)
- **Persistência**: File storage + snapshots em object storage

### Por que Go

- AWS SDK oficial em Go (aws-sdk-go-v2) usado em produção internamente
- Single binary (deploy trivial)
- Excelente suporte para protocolos HTTP com strict parsing
- Concorrência nativa (goroutines para long-poll subscribers)

### Por que cloud-agnostic (Terraform)

- Mesma imagem Docker roda em AWS, GCP, Azure, Hetzner, on-prem
- Terraform permite state declarativo, IaC reproduzível
- Reduz lock-in e custo de migração

### Por que SigV4 + IAM real (não bypass)

- Clientes AWS SDK assinam requests automaticamente
- Bypass só funciona em dev/CI com AUTH_MODE=off
- Em prod, sem SigV4 qualquer um com a URL consegue consumir
- IAM policy evaluation permite least-privilege por access key

## Roadmap

### Fase 1 — SQS Standard leitura/metadados ✅ completa

- [x] Semana 1: CreateQueue/GetQueueUrl/GetQueueAttributes/ListQueues/DeleteQueue
      com persistência via JetStream KV `queue_meta`
- [x] Semana 2: SetQueueAttributes + Validação de ranges de atributos

### Fase 2 — SQS Standard operações de mensagem ✅ completa

- [x] SendMessage com headers MessageAttributes (X-Sqs-Atr-<name>)
- [x] ReceiveMessage com long-poll via FetchMaxWait
- [x] DeleteMessage com idempotência
- [x] ChangeMessageVisibility via NakWithDelay
- [x] PurgeQueue via stream purge
- [x] Receipt handles versionados (rh1:<consumer>:<seq>)
- [x] Consumer durável único por fila (AckExplicitPolicy, MaxAckPending=1000)
- [x] Cache de pending msgs em memória (sync.RWMutex)

### Fase 3 — Batch + FIFO completo ✅ completa

- [x] SendMessageBatch até 10 entries com partial success
- [x] DeleteMessageBatch até 10 entries com partial success
- [x] Validações: TooManyEntriesInBatch, BatchEntryIdsNotDistinct, EmptyBatchRequest
- [x] Validação de soma de bodies ≤ 256 KiB
- [x] FIFO MessageGroupId preserva ordem dentro do grupo
- [x] FIFO MessageDeduplicationId via Nats-Msg-Id
- [x] FIFO ContentBasedDeduplication via SHA-256(body) como Nats-Msg-Id
- [x] FIFO SequenceNumber retornado em cada send

### Fase 4 — SNS (próximo)

- [ ] CreateTopic
- [ ] Subscribe (sqs/http/https)
- [ ] Publish
- [ ] ListSubscriptions
- [ ] Unsubscribe / DeleteTopic
- [ ] Fan-out com filter policy
- [ ] Job server assíncrono para HTTP/HTTPS subscriptions

### Fase 5 — Auth + IAM real

- [ ] Verificação SigV4 com parsing de credential string
- [ ] IAM store (file + DynamoDB-like table)
- [ ] Policy JSON evaluation (resource, action, effect)
- [ ] CLI shimctl para gerenciar filas/topics/IAM

### Fase 6 — Provisioning

- [ ] Terraform modules (network, compute, broker, api, observability)
- [ ] HA cluster NATS 3 nós + snapshots S3
- [ ] mTLS entre shim e broker

### Fase 7 — Hardening produção

- [ ] Prometheus + Grafana + Loki + OTel exporters
- [ ] Runbooks + disaster recovery
- [ ] SLOs + alerting rules
- [ ] Multi-region replication

## Limitações atuais

### Conhecidas (aceitas para MVP)

- **Não SigV4 nem IAM** — qualquer credencial aceita (AUTH_MODE=off)
- **Não suporta SNS** — stub retorna UnsupportedOperation
- **Não tem cluster** — NATS single-node em dev
- **Não tem observability de produção** — só stdout/log
- **Não tem TLS** — HTTP plano, TLS termina no LB em prod
- **Consumer único por fila** — múltiplos clients paralelos na mesma fila
  não suportados (solução em prod: sharding por partition key)
- **AproximateNumberOfMessages** é contado de `Stream.State.Msgs` (inclui
  mensagens já acked — lag de update do JetStream)

### Performance

Não medida ainda. Estimativas para NATS JetStream em cluster 3 nós:

| Operação              | Latência p50 | Latência p99 | Throughput        |
|-----------------------|--------------|--------------|-------------------|
| CreateQueue           | ~10ms        | ~50ms        | 1k/s              |
| GetQueueUrl           | ~2ms         | ~10ms        | 50k/s             |
| GetQueueAttributes    | ~5ms (cache) | ~20ms        | 10k/s             |
| ListQueues            | ~20ms        | ~100ms       | 1k/s              |
| DeleteQueue           | ~10ms        | ~50ms        | 1k/s              |

Receive/Send (quando implementados):

- SendMessage: ~2ms p99, throughput 50k/s/nó
- ReceiveMessage long-poll: ~100ms p99 (limitado por AckWait mínimo)
- ReceiveMessage hot: ~5ms p99, throughput 30k/s/consumidor

## Anexos

- [Compatibilidade AWS](api-compatibility.md) — matriz de operações
- [Modelo de segurança](security-model.md) — SigV4, IAM, mTLS, network
- [Runbook operacional](operations-runbook.md) — procedures, troubleshooting
- [Uso de Terraform](terraform-usage.md) — como provisionar
