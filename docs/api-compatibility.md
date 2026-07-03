# Compatibilidade AWS — Roadmap detalhado

> Esta página acompanha o **progresso futuro** da compatibilidade dropin-queue.
> Para o status atual (o que já está pronto e testado), ver a seção
> [Compatibilidade AWS — Status atual](../README.md#compatibilidade-aws--status-atual)
> no README.

Status legend:
- ✅ implementado e testado E2E
- 🚧 em desenvolvimento
- [ ] não iniciado

---

## SQS — operações completas disponíveis

Todas as operações abaixo estão cobertas pelos dual protocols Query + JSON 1.0
e validadas pelos testes E2E em `shim/test/integration/test_sqs_*.py`.

### Standard

- [x] `CreateQueue`
- [x] `GetQueueUrl`
- [x] `GetQueueAttributes`
- [x] `ListQueues`
- [x] `DeleteQueue`
- [x] `SetQueueAttributes`
- [x] `SendMessage`
- [x] `ReceiveMessage` (long-poll via `FetchMaxWait`)
- [x] `DeleteMessage`
- [x] `ChangeMessageVisibility`
- [x] `PurgeQueue`

### FIFO

- [x] `MessageGroupId` (ordering dentro do grupo)
- [x] `MessageDeduplicationId` (dedup explícito via `Nats-Msg-Id`)
- [x] `ContentBasedDeduplication` (SHA-256(body) → `Nats-Msg-Id` automático)

### Batch

- [x] `SendMessageBatch` (≤10 entries, partial success via `Failed[]`)
- [x] `DeleteMessageBatch` (≤10 entries, partial success)
- [ ] `ChangeMessageVisibilityBatch` — **próxima iteração**
- [ ] `SendMessageBatch` com `MessageSystemAttributes`
- [ ] `SendMessageBatch` com `MessageDeduplicationId` por entry

### DLQ

- [x] Configuração via `RedrivePolicy` em `SetQueueAttributes`
- [x] Streams separados para DLQ
- [ ] Validação automática de tamanho máximo de `RedrivePolicy`
- [ ] Dead-letter metric counters expostos via Prometheus

---

## SNS — operações completas disponíveis

Cobertura por operação + protocolo. Validadas por
`shim/test/integration/test_sns.py`.

- [x] `CreateTopic`
- [x] `GetTopicAttributes`
- [x] `ListTopics`
- [x] `DeleteTopic` (com cascade de subscriptions órfãs)
- [x] `Subscribe`
  - [x] protocol `sqs` (auto-confirmed, fan-out funciona)
  - [ ] protocol `http` (pending — `ConfirmSubscription` stub)
  - [ ] protocol `https` (pending — `ConfirmSubscription` stub)
  - [ ] protocol `email` (não suportado)
  - [ ] protocol `sms` (não suportado)
  - [ ] protocol `lambda` (não suportado)
  - [ ] protocol `firehose` (não suportado)
- [x] `Unsubscribe`
- [x] `Publish` (fan-out síncrono para subscribers SQS)
- [x] `ListSubscriptions`
- [x] `ListSubscriptionsByTopic`
- [x] `ConfirmSubscription` — **stub** (retorna `UnsupportedOperation` no MVP)
- [x] `FilterPolicy` MVP (match exato `{key: [allowed_values]}`)
  - [ ] FilterPolicy com `$or` / `$and` (and/or logical)
  - [ ] FilterPolicy com match em `exists` / `numeric` / `prefix`
- [ ] `AddPermission` / `RemovePermission`
- [ ] `SetTopicAttributes`
- [ ] `TagResource` / `UntagResource` / `ListTagsForResource`

---

## Auth & IAM (Fase 5 — próxima fase)

- [ ] Verificação de AWS Signature v4 (SigV4)
- [ ] Suporte a SigV4 query string (presigned URLs)
- [ ] IAM store com persistência em JetStream KV
- [ ] Policy JSON evaluation (Effect, Action, Resource, Condition)
- [ ] Condition keys: `aws:SourceIp`, `aws:UserAgent`, `aws:CurrentTime`,
      `aws:RequestTag/*`, `aws:PrincipalTag/*`
- [ ] Resource-level permissions em SQS (`sqs:QueueName`)
- [ ] Resource-level permissions em SNS (`sns:topic`)
- [ ] CLI `shimctl` (criar usuário, anexar policy, rotacionar keys)
- [ ] Suporte a STS (assume role, federation)

---

## Mensageria avançada

- [ ] Message timers (`MessageRetentionPeriod` extendido, per-message TTL)
- [ ] Dead-letter queue metrics
- [ ] Cross-region replication
- [ ] Server-side encryption (SSE-SQS via KMS)
- [ ] Large payload support (>256 KiB via S3 extended client)

---

## Observability

- [x] Logs JSON estruturados (`slog` + campos padronizados)
- [x] Métricas Prometheus expostas em `/metrics`
- [x] OpenTelemetry tracing (OTLP exporter configurável)
- [ ] Dashboards Grafana pré-configurados (queue depth, fan-out latency,
      consumer lag, error rate por operação)
- [ ] Alertas Prometheus (queue depth crítico, consumer lag, auth failures)
- [ ] Log aggregation via Loki + estruturação por service/operation/queue
- [ ] Distributed tracing com spans por operação AWS (parent-child correlation)

---

## Infra (Fases 6-7)

- [ ] Terraform módulos production-ready
  - [ ] `network` (VPC, subnets, security groups)
  - [ ] `broker` (NATS JetStream cluster 3-nó)
  - [ ] `api` (dropin-server N réplicas + LB)
  - [ ] `objectstore` (snapshots JetStream → S3/GCS/Azure Blob)
  - [ ] `observability` (Prometheus + Grafana + Loki stack)
- [ ] HA multi-AZ (broker + api em AZs distintas)
- [ ] Snapshots automáticos JetStream (cron + objectstore upload)
- [ ] Restore drill (testar restore a partir de snapshot periodicamente)
- [ ] Runbooks (backup, restore, failover, incident response)
- [ ] SLOs definidos e documentados (availability, latency, durability)
- [ ] Capacity planning guide (msg/s por nó, storage por partition)

---

## Escalabilidade (limitações conhecidas)

- [ ] Sharding para multi-consumer Standard (atualmente: 1 consumer durável/fila)
- [ ] Round-robin assignment entre consumers
- [ ] Multi-region replication (active-active ou active-passive)
- [ ] Connection pooling para o NATS broker (atualmente: 1 conn por dropin-server)
- [ ] Sharded message processing para batch operations grandes

---

## Como contribuir

Cada item acima com [ ] pode virar uma issue ou um PR. Antes de começar:

1. Leia [`AGENTS.md`](../AGENTS.md) (princípios do projeto)
2. Leia [`gotchas.md`](gotchas.md) (bugs que já nos custaram tempo)
3. Leia a spec AWS correspondente em docs.aws.amazon.com
4. Use botocore + boto3 como referência do wire format
5. Adicione teste E2E em `shim/test/integration/` antes do PR
6. Atualize o README e este roadmap
