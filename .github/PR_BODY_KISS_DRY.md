## Resumo

Integra duas análises KISS/DRY do projeto (minha + análise paralela do
Claude Fable). Resultado: **7 commits + branch dedicado `refactor/kiss-dry-pass-1`**
que resultam em **2 bug fixes reais** + **-858 LOC duplicadas removidas**
+ cobertura E2E expandida de **70/70 → 72/72**.

Contexto: o sistema ainda não está em produção; API pública = compatibility
com SDKs AWS oficiais. Validado: 72/72 testes E2E (boto3 + aws-cli)
passando após CADA commit; wire format byte-a-byte preservado.

## Bugs corrigidos (do path crítico — refatorar estética primeiro arrisca MAIS bugs)

### Bug 1 — métricas de storage infladas em 2× (Commit 1)

Padrão pré-fix em 3 métodos de `storage/nats` (SendMessage,
DeleteMessage, ChangeMessageVisibility) chamava `ObserveStorage(2×
no caminho de sucesso — uma em `defer`, uma explícita. Resultado:
storage_ops_total counter dobrado, taxa de erro mascarada
(50% de erro aparecia como ~33%), throughput aparente dobrado,
percentis do histograma distortidos.

Fix: helper `observability.StartObserve(op).Done(&err)` em
métodos com named return. Verificação: rodar 5 Sends + medir
/metrics antes/depois. Pré-fix delta=10, pós-fix delta=5.

### Bug 2 — data race em metadataKV (Commit 1)

3 cópias do helper lazy-init de KV (metadataKV, topicKV,
subscriptionKV). 2 delas usavam `c.mu.Lock/Unlock`; **1
esqueceu o mutex**. Duas requests concorrentes no startup
disparavam CreateOrUpdateKeyValue simultaneamente + escrita
de ponteiro `c.kvCache = kv`. Pego por `go test -race` (que
agora passa sem warnings).

Fix: helper único `lazyKVCache.loadKV` em `storage/nats/lazy_kv.go`
com sync.Mutex. Os 3 sites passam a chamá-lo. Próxima cópia já
nasce correta.

## Refactors DRY executados

| # | Commit | LOC impact | Diff |
|---|---|---|---|
| 1 | fix(storage) — métricas + race | 324+/259- | helper StartObserve + lazyKV |
| 2 | refactor(http) — parse-once | 224+/63- | removed 1MB sniff + gotcha #5 |
| 3 | refactor — internal/awserr | 348+/135- | Error struct + sender-fault registry |
| 4 | refactor(protocol) — common.go | 514+/420- | parse/encode parametrizados |
| 5 | refactor — helpers ARN/stream | 211+/103- | ResourceNameFromARN + queueStreamName |
| 6 | refactor(server) — 1 error writer | 405+/129- | writeFatalError consolida 4 cópias |
| 7 | docs (AGENTS + README) | 20+/3- | convenções + baseline 70→72 |
| **Total** | **7+1 commits** | **2046+/1112-** | **+934 líquido (com testes)** |

## Elimina dívida

- **Gotcha #5 do docs/gotchas.md** — sniff 1 MB substituído por
  parse direto via `cfg.MaxRequestBodyBytes` (5 MB default).
- **.gitkeep em dirs não-vazios** — removidos em path de documentação.
- **10<<20 hardcoded em 3 lugares** → `protocol.MaxWireBodyBytes`
  (1 lugar).
- **AWSError divergia entre SQS/SNS** — sender-fault registry único
  em awserr (Fase 5 adicionará new codes em 1 lugar, não 2).
- **list ALL:** pre-fix tinha ~30 sites para os 8 helpers distintos
  pré-fix; pós-fix tem 30 sites todos chamando helpers únicos.

## Compatibilidade wire

**100% preservada.** 72/72 testes E2E passam após cada commit.
boto3 (Python), aws-cli, aws-sdk-js/go/java — todos continuam
funcionando idênticos.

## Plano de commits (granular — preserva git bisect)

```
1c73a70 fix(storage): corrigir double-count de métricas + data race em metadataKV
837d149 refactor(http): roteamento parse-once sem sniff 1MB + limite único de body
ceb45fe refactor: extrair internal/awserr + protocol/message_attribute
a3d98fc refactor(protocol): consolidar parse/encode comuns entre SQS e SNS
3faf315 refactor: centralizar helpers de ARN/URL + stream names
4e70a5e refactor(server): consolidar 4 error writers em 1 função
27113e4 docs: refletir refactor/kiss-dry-pass-1 no AGENTS.md e README
```

(8 commits no total — 7 planejados + 1 docs. Original plano tinha
5 sub-commits em Commit 6; consolidei em 1 porque o valor marginal
de fragmentar mais era baixo vs. ruído de rebase.)

## Verificação por commit

Cada commit foi validado em CI local antes do push:

```bash
gofmt -s -l .              # clean
go vet ./...               # clean
go test -race -short ./... # passa em ~1s
docker compose up -d --build
make test-int              # 72/72 em ~50s
```

Verificação manual remota dos bugs:
- `curl /metrics | grep storage_ops_total{op="send_message"}` — delta exato
  após N mensagens (não 2×N).
- `go test -race ./...` no storage/nats — sem DATA RACE warnings
  na metadataKV lazy init.

## Não-objetivos (preservados)

- **Mantidos como no original**: `params` parse functions (mecânico,
  grep-friendly, AWS wire format tem exceções demais pra codegen).
  `isValidQueueName/isValidTopicName` (regras divergentes).
  Switches de dispatch SQS/SNS (proporcionais após refactor de
  handlers).
- **NÃO adicionados**: SigV4/IAM (Fase 5), DLQ automático (Fase 7),
  Terraform skeletons (Fase 6).

## Arquivos novos

- `shim/internal/awserr/awserr.go` + `awserr_test.go` — Error struct
  + sender-fault registry + FromStorage helper.
- `shim/internal/storage/nats/lazy_kv.go` + `lazy_kv_test.go` — helper
  thread-safe lazy-init KV (corrige race).
- `shim/internal/protocol/common.go` + `common_test.go` — parse/encode
  compartilhados SQS+SNS.
- `shim/internal/protocol/message_attribute.go` + `_test.go` —
  MessageAttributeToTypes + MessageAttributesSize centralizados.
- `shim/internal/protocol/arn_test.go` — ARN/URL parsing tests.
- `shim/internal/server/response.go` + `response_test.go` —
  writeFatalError consolidado + detecção de transport.
- `shim/test/integration/test_sns_large_publish.py` — 2 testes
  validando o dispatch sem sniff.

## Próximos passos (PASS-2)

Itens legítimos para `refactor/kiss-dry-pass-2` em sprint futura:

1. Split de `server/http.go` em `sqs_handlers.go` + `sns_handlers.go`
   (ainda ~1700 linhas, navegação ruim).
2. `respondXML(w, action, result, reqID)` helper para eliminar ~22
   cópias do XML envelope building em cada handler.
3. `requestFromContext(r)` para garantir X-Request-ID header ==
   RequestId no body (hoje gera novo, diverge).
4. `params` em SNS — adicionar `MessageSystemAttributes` e
   MessageDeduplicationId por entry em batch.

Não bloqueadores. Roadmap normal.

## Checklist AGENTS.md

- [x] `make test` passa
- [x] Teste E2E adicionado (2 novos em test_sns_large_publish.py + atualização em test_sqs_batch.py)
- [x] `make test-int` passa (70/70 → **72/72**)
- [x] README + AGENTS.md atualizados
- [x] Commit em pt-BR detalhado (why, não what)
- [x] Sem mudanças de wire format (compat AWS preservada)
