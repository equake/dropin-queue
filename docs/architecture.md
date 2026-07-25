# Arquitetura do dropin-queue

Este documento descreve a arquitetura do `dropin-queue`, um clone
auto-hospedável, compatível com o protocolo AWS SNS/SQS, construído em Go.
O backend de mensageria é trocável via config (`GQ_BACKEND=nats|postgres`,
nunca os dois ao mesmo tempo) — NATS JetStream é o default histórico do
projeto; Postgres (`LISTEN/NOTIFY` + `SKIP LOCKED`) é a alternativa,
detalhada em [Backend Postgres](#backend-postgres) mais abaixo.

> Status: Fase 4 completa — 13 SQS + 9 SNS operações funcionais (72/72 testes E2E passando)
> (CreateQueue/Get/List/Delete + SendMessage/ReceiveMessage/DeleteMessage/
> ChangeMessageVisibility/PurgeQueue + SetQueueAttributes + SendMessageBatch/
> DeleteMessageBatch), todas com dual protocol (Query+JSON), todas com
> testes E2E via boto3 oficial Python.
> Inclui FIFO queues completas: MessageGroupId, MessageDeduplicationId,
> ContentBasedDeduplication, SequenceNumber, ordering within group.
> Backend de mensageria trocável (NATS JetStream ou Postgres) via GQ_BACKEND.

## Visão geral

```
┌───────────────────────────────────────────────────────────────┐
│                  AWS SDK / boto3 / aws-cli                    │
│  (Python, Go, Node.js, Java, Ruby, Rust, .NET, aws-cli)       │
└─────────────────────────┬─────────────────────────────────────┘
                          │ HTTPS · SigV4 (ou dummy em dev)
                          ▼
┌───────────────────────────────────────────────────────────────┐
│              dropin-server — API compatível AWS (Go)           │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │  server/   chi router · middleware · handlers AWS      │  │
│  ├─────────────────────────────────────────────────────────┤  │
│  │  protocol/ SQS Query (form+XML) · SQS JSON 1.0         │  │
│  ├─────────────────────────────────────────────────────────┤  │
│  │  awssig/    (Fase 5) verificação de AWS Signature v4   │  │
│  ├─────────────────────────────────────────────────────────┤  │
│  │  iam/       (Fase 5) policy evaluation (JSON IAM)      │  │
│  ├─────────────────────────────────────────────────────────┤  │
│  │  sqs/       validação semântica + tradução erros       │  │
│  │  sns/       fan-out · filter policy                    │  │
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

> Diagrama acima mostra o backend `nats` (default). Com
> `GQ_BACKEND=postgres`, as duas últimas caixas (JetStream cluster +
> Object Storage) são substituídas por um único Postgres — sem cluster
> Raft nem snapshot em object storage; ver
> [Backend Postgres](#backend-postgres).

## Camadas do shim

### 1. `cmd/dropin-server` — entrypoint

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
- MessageDeduplicationId via Nats-Msg-Id (dedup explícito em janela de 5min)
- ContentBasedDeduplication via SHA-256(body) como Nats-Msg-Id
  (dedup implícito em janela de 5min)
- SequenceNumber retornado em cada send (MessageId = stream sequence)

### 5. `sns/` — SNS service

Fan-out com filter policy em subscribers SQS (completo). HTTP/HTTPS
ficam pending — `ConfirmSubscription` é stub (`UnsupportedOperation`).

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

Nenhum tipo do broker vaza por essa interface — `sqs/`, `sns/`, `protocol/`
e `server/` só conhecem `storage.Storage` e `pkg/types`. Isso é o que torna
o backend trocável via `config.Backend` (`GQ_BACKEND=nats|postgres`, nunca
os dois ao mesmo tempo): trocar de adapter é uma mudança contida em
`cmd/dropin-server/main.go` (qual `Connect()` chamar) — zero mudança nas
camadas acima.

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

#### Adapter Postgres (`storage/postgres/`) {#backend-postgres}

Opção de backend para quem já opera Postgres em produção e quer evitar
rodar um segundo sistema de mensageria. Habilitado via `GQ_BACKEND=postgres`
+ `GQ_POSTGRES_DSN`. Schema aplicado idempotente (`CREATE TABLE IF NOT
EXISTS`) no `Connect()` — sem ferramenta de migration externa no MVP.

Mapeamento:

| Conceito AWS              | Implementação Postgres                                    |
|----------------------------|------------------------------------------------------------|
| Fila SQS                  | Linha em `queues`                                          |
| Mensagem                  | Linha em `messages` (`queue_id`, `group_key`, `body`, `message_attributes` JSONB) |
| MessageGroupId (FIFO)      | Coluna `group_key` — preserva ordem relativa via `ORDER BY id` no claim |
| MessageDeduplicationId /   | Tabela `message_dedup` (`queue_id`, `dedup_id`) com `UNIQUE` + janela de 5min |
| ContentBasedDeduplication  | (SHA-256 do body quando não há ID explícito, igual ao adapter NATS)      |
| Visibility timeout         | Coluna `visible_at` + `claim_token` (UUID)                   |
| Claim de mensagens         | `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED)` |
| Long-polling               | `LISTEN/NOTIFY` (canal único `dropin_queue_msg`, payload = nome da fila) + poll de segurança |
| Tópico SNS                 | Linha em `topics` — **sem log de mensagens publicadas** (ver nota abaixo) |
| Subscription                | Linha em `subscriptions` (`topic_arn` persistido verbatim — storage não conhece account/region) |

**Divergência conhecida: histórico de publish do tópico.** O adapter NATS
mantém um stream JetStream por tópico (`topic-<nome>`, retenção
`GQ_TOPIC_MAX_AGE`, default 1h) onde toda mensagem publicada fica
registrada, independente do fan-out. O adapter Postgres **não tem
equivalente** — `Publish` só faz fan-out síncrono, nada é persistido a
mais no tópico em si. Decisão consciente, não lacuna a preencher por
padrão: hoje **nenhum dos dois backends tem qualquer consumidor desse
histórico** (o próprio comentário do `Publish` no adapter NATS já registra
isso como "disponível para... **futuro**"), então implementar o
equivalente no Postgres seria complexidade sem uso corrente. Fica
documentado aqui para não ser redescoberto como bug: se um dia alguém
implementar consumo do histórico de tópico (replay, auditoria, etc.), os
dois backends precisam ganhar essa capacidade juntos, não só um.

Receipt handle: `pg1:<message_id>:<claim_token>` — stateless, igual ao
`rh2:` do adapter NATS (ack/nak funciona de qualquer réplica do shim, sem
sticky session).

**Claim de mensagens (`SKIP LOCKED`).** `ReceiveMessage` usa uma única
query (Standard e FIFO, sem distinção — ver próximo parágrafo):

```sql
WITH claimed AS (
    UPDATE messages SET
        visible_at   = now() + make_interval(secs => $3),
        delivery_count = delivery_count + 1,
        claim_token = gen_random_uuid()
    WHERE id IN (
        SELECT id FROM messages
        WHERE queue_id = $1 AND visible_at <= now()
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        LIMIT $2
    )
    RETURNING id, body, message_attributes, enqueued_at, delivery_count, claim_token::text
)
SELECT * FROM claimed ORDER BY id;
```

O `ORDER BY id` externo (sobre a CTE, não sobre a subquery) é essencial:
`UPDATE ... RETURNING` não herda a ordem da subquery de seleção — o
resultado pode sair em ordem física do heap, que o MVCC embaralha a cada
UPDATE. Sem o `ORDER BY` externo, ordering-within-group do FIFO quebra de
forma intermitente sob concorrência (bug real, encontrado nos testes E2E
durante o desenvolvimento — ver `docs/gotchas.md`).

**FIFO não usa mutex de grupo.** Uma iteração inicial do design chegou a
usar uma tabela `queue_groups` com `FOR UPDATE SKIP LOCKED` para impor "no
máximo 1 mensagem in-flight por `MessageGroupId`" (o comportamento real do
SQS FIFO). Essa restrição foi removida porque quebrava paridade com o
adapter NATS: o adapter NATS **também não impõe** esse limite hoje (o
consumer durável compartilhado distribui livremente entre pulls
concorrentes) — só garante ordem relativa via log order. Manter o mutex no
Postgres teria deixado os dois backends com semânticas de FIFO
diferentes, quebrando o requisito de "mesma suíte E2E passa nos dois".
Ordering-within-group nos dois backends hoje = ordem relativa preservada,
**não** exclusividade de entrega. Fica documentado aqui como um item de
paridade a revisitar junto (nos dois backends) se o projeto precisar da
garantia real do SQS FIFO no futuro.

**Long-polling: NOTIFY com poll de segurança, não 1 conexão por request.**
Uma única conexão dedicada de `LISTEN` por réplica do shim recebe
notificações no canal fixo `dropin_queue_msg` (payload = nome da fila) e
multiplexa para os `ReceiveMessage` em espera via canais Go em memória.
`SendMessage` não dispara `NOTIFY` síncrono por mensagem — um debounce
leading-edge (`GQ_POSTGRES_NOTIFY_COALESCE`, default 20ms) evita o teto de
throughput que um `NOTIFY` por `INSERT` causaria (o artigo que motivou este
design, [dbos.dev](https://www.dbos.dev/blog/postgres-listen-notify-scalability),
mediu 2.9K writes/s com notificação por linha vs. 60K writes/s
bufferizando). Um poll de segurança (`GQ_POSTGRES_POLL_INTERVAL`, default
300ms) cobre o caso raro de notificação perdida (reconexão do listener,
etc.) — o mesmo padrão de rede de segurança que o artigo descreve.

**Trade-off de escala (avaliar antes de produção).** Postgres LISTEN/NOTIFY
+ `SKIP LOCKED` é uma opção legítima de baixo custo para volume
moderado/alto, mas a história de escala horizontal é estruturalmente
diferente da do cluster NATS JetStream (Raft, múltiplos nós ativos): em
Postgres, escrita é sempre single-writer (primary). O backend Postgres
provavelmente serve bem até um teto de throughput de escrita bem definido
por instância, mas não escala escrita horizontalmente como o cluster NATS
— é uma troca de "menor custo/operação mais simples" por "teto de
throughput mais baixo e HA dependente de failover do Postgres
(Patroni/RDS Multi-AZ/etc.), não de Raft nativo".

### 7. `observability/` — telemetria

- **Logging**: slog JSON em prod, text colorido em dev
- **Métricas**: Prometheus (shim_http_*, shim_storage_*, shim_sqs_*,
  shim_sns_*)
- **Tracing**: OTel SDK com stdout exporter em dev (OTLP em prod — roadmap)

### 8. `awssig/` — SigV4 (Fase 5 — planejado)

Verificação de AWS Signature v4. Em modo dev (`AUTH_MODE=off`),
qualquer credencial é aceita.

### 9. `iam/` — IAM (Fase 5 — planejado)

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

### Fase 4 — SNS ✅ completa

- [x] CreateTopic (idempotente — mesmo nome → mesmo ARN)
- [x] GetTopicAttributes / ListTopics
- [x] Subscribe SQS (subscriptions HTTP/HTTPS ficam pending — ConfirmSubscription stub)
- [x] Unsubscribe (idempotente — não falha em ARN inexistente)
- [x] DeleteTopic (idempotente + cascade remove subscriptions órfãs)
- [x] Publish com fan-out síncrono para subscriptions SQS
- [x] Filter policy (`{"type":["alert"]}`) aplicado antes da entrega
- [x] ListSubscriptions (global) e ListSubscriptionsByTopic
- [x] Validações: Message vazio, Message > 256 KiB, TopicArn inválido
- [x] Suporte a MessageAttributes (String/Number/Binary) + Subject
- [x] Dual protocol: Query (form+XML) e JSON 1.0 simultaneamente
- [x] 20 testes E2E com boto3 — 72/72 totais passando

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
- **SNS: subscriptions HTTP/HTTPS ficam pending** — `ConfirmSubscription`
  é stub (`UnsupportedOperation`); protocol `sqs` está completo
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
