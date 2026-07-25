# Gotchas — Memória institucional do dropin-queue

> Antes de implementar ou corrigir algo, leia esta lista. Cada item aqui é um bug
> que já nos custou pelo menos 1 hora de debug.

## AWS SDK / Protocol

1. **boto3 envia Attributes em DOIS formatos.** No SQS JSON ≥ 1.40 usa map compacto
   `Attributes: {"key": "value"}`; em SNS Subscribe usa `Attributes.entry.N.key/value`.
   O parser precisa aceitar ambos ou boto3 falha.

2. **SNS Query usa wrapper `entry` para MessageAttributes.**
   `MessageAttributes.entry.N.Name.Value.StringValue` é o caminho real. Não é
   `MessageAttributes.N.Name/Value` como em SQS. Já causou truncate silencioso.

3. **ConfirmSubscription retorna `UnsupportedOperation` no MVP.**
   Subscriptions HTTP/HTTPS ficam pending para sempre. Não prometa auto-confirmação
   a não ser que esteja implementado de fato.

4. **ListSubscriptions vs ListSubscriptionsByTopic têm XML wrappers diferentes.**
   `<ListSubscriptionsResult>` vs `<ListSubscriptionsByTopicResult>`. Hardcoded
   causa boto3 retry infinito.

5. **(removido no refactor/kiss-dry-pass-1 — Commit 2).**
   O sniff 1 MB foi substituído por parse direto do body em
   `handleAWSQueryDispatch`. Body > cfg.MaxRequestBodyBytes (5 MB
   default) gera erro coerente antes do dispatch, sem fallback
   silencioso para SQS. Ver `test_sns_large_publish.py:36`.

## NATS JetStream

6. **`jetstream.ErrNoKeysFound` ≠ `nats.ErrNoKeysFound`.**
   Ambos têm a string `"nats: no keys found"` mas são sentinels diferentes (pacotes
   diferentes). `errors.Is(err, nats.ErrNoKeysFound)` retorna `false` mesmo quando
   o erro é exatamente "no keys found". Use sempre o do `jetstream`.

7. **Chaves de KV rejeitam `:` e `.`.**
   ARNs (`arn:aws:sqs:us-east-1:000000000000:my-queue`) viram chave com sanitização:
   `arn_aws_sqs_us-east-1_000000000000_my-queue`. Helper: `subscriptionKey()`.

8. **Stream não expõe `PublishMsgAsync` — use o client JetStream.**
   `js.PublishMsgAsync(nmsg)` é o caminho. `stream.PublishMsgAsync` não existe.

9. **NUNCA faça `Fetch` sem `FetchMaxWait`.** Sem MaxWait, o pull request usa o
   default do servidor (30s) e fica pendente DEPOIS que você retorna — a próxima
   mensagem da fila é entregue a um fetch que ninguém está lendo (invisível até
   AckWait expirar). Short-poll usa `FetchMaxWait(100ms)`; `FetchMaxWait(0)` é
   rejeitado pela lib. Esse bug ficou mascarado por meses porque
   `CreateOrUpdateConsumer` a cada receive cancelava os pulls fantasma.

10. **Consumer EFÊMERO por chamada ReceiveMessage não funciona.**
    Solução atual: 1 consumer durável por fila (criado no CreateQueue, cacheado)
    + receipt handle carregando o reply subject `$JS.ACK` — ack/nak é um publish
    no reply subject, de qualquer réplica, sem estado local.

10b. **`FlushWithContext` exige ctx COM deadline.** Ctx sem deadline retorna
    `nats: context requires a deadline` — e o ctx de request HTTP não tem.
    Embrulhe com `context.WithTimeout` antes.

10c. **`Retention` de stream é IMUTÁVEL.** `UpdateStream` mudando Retention
    falha. Migração LimitsPolicy → WorkQueuePolicy exige recriar o stream
    (`make down-v` em dev; em produção, migração de dados).

10d. **`-NAK {"delay": 0}` ≠ `-NAK` puro.** Com o payload JSON de delay 0 a
    redelivery pode demorar mais que a janela de um short-poll (100ms). Para
    visibility timeout 0 (redelivery imediato), envie `-NAK` sem payload.

## AWS SDK / boto3 specifics

11. **`Sequence` em JetStream `MsgMetadata` é struct (`SequencePair`), não uint64.**
    Use `meta.Sequence.Stream`.

12. **`Msg.Data`, `Msg.Metadata`, `Msg.Headers` são MÉTODOS, não campos.** Esquecer
    os parênteses vira panic em nil.

13. **boto3 não expõe `MD5OfMessage` no response de SendMessage** mesmo quando o
    servidor envia. Aceite essa divergência — não é bug nosso.

14. **DataType de MessageAttribute precisa header NATS separado (`-dt`).**
    Sem isso o decoder assume "String" e corrompe Binary.

## Storage / Sharding

15. **ApproximateNumberOfMessages usa `Stream.State.Msgs`.** Com WorkQueuePolicy
    (retention atual), mensagens acked são apagadas — o número reflete só
    não-consumidas (visíveis + in-flight), próximo do SQS. Pode haver pequeno
    lag de update do JetStream.

16. **Múltiplos clients na mesma fila funcionam via pull consumer compartilhado.**
    N réplicas do shim (e N clients) fazem `Fetch` no MESMO consumer durável;
    JetStream distribui as mensagens. Receipt handles são stateless (reply
    subject), então o delete pode chegar em qualquer réplica.

## Postgres backend (storage/postgres)

18. **`UPDATE ... RETURNING` NÃO herda o `ORDER BY` da subquery usada no
    `WHERE id IN (SELECT ... ORDER BY id FOR UPDATE SKIP LOCKED LIMIT N)`.**
    O resultado do `RETURNING` pode sair em ordem física do heap (MVCC
    reordena fisicamente as tuplas a cada UPDATE), não na ordem de chegada.
    Sintoma: ordering-within-group do FIFO quebra de forma intermitente sob
    concorrência — só apareceu rodando a suíte E2E inteira, nunca em teste
    isolado. Fix: envolver em CTE e aplicar `ORDER BY id` numa `SELECT`
    externa (`WITH claimed AS (UPDATE ... RETURNING ...) SELECT * FROM
    claimed ORDER BY id`). Ver `messages.go:claim`.

19. **`gen_random_uuid()` é nativo desde o Postgres 13** — não precisa da
    extensão `pgcrypto`. Evita `CREATE EXTENSION` no schema (requisito a
    menos em Postgres gerenciado com extensões restritas).

20. **Dedup (`MessageDeduplicationId`/`ContentBasedDeduplication`) precisa
    devolver o `MessageId` da mensagem ORIGINAL em duplicatas, não um ID
    sintético.** Mesmo comportamento do JetStream: `PubAck.Duplicate=true`
    reaproveita o `Sequence` original em vez de gerar um novo — clientes
    que enviam a mesma mensagem 3x esperam o MESMO `MessageId` nas 3
    respostas. Implementação: insere a mensagem sempre, e se
    `message_dedup` já tinha esse `(queue_id, dedup_id)`, desfaz o insert
    (`DELETE`) e devolve o `message_id` gravado na primeira vez.

21. **FIFO (`storage/postgres`) não restringe a "1 mensagem in-flight por
    grupo".** Só preserva ordem relativa (via `ORDER BY id`), igual ao
    adapter NATS. Uma versão anterior do design impunha essa restrição via
    mutex de grupo (`FOR UPDATE SKIP LOCKED` numa tabela `queue_groups`) —
    foi removida por quebrar paridade de comportamento com o NATS (que
    também não impõe o limite) e por quebrar o teste E2E
    `test_fifo_ordering_within_message_group`, que espera receber TODAS as
    mensagens do grupo numa única chamada, não uma por vez.

22. **Teste E2E que usa `subprocess`/`curl` direto (em vez do client boto3
    da fixture) precisa importar `SHIM_ENDPOINT` de `conftest.py`, não
    hardcodear `http://localhost:4566`.** `test_sns_large_publish.py` e uma
    versão anterior de `test_sns.py` (fixtures locais duplicadas com
    `endpoint_url` fixo) tinham esse hardcode — invisível enquanto só
    existia 1 backend/porta, mas silenciosamente testava o backend ERRADO
    assim que um segundo backend passou a rodar numa porta diferente
    (`shim-postgres`, porta 4567). Só apareceu ao rodar a MESMA suíte
    contra os dois backends lado a lado.

23. **`ON CONFLICT ... DO NOTHING` em tabela de dedup bloqueia o ID PARA
    SEMPRE, não só durante a janela.** `message_dedup` tem
    `PRIMARY KEY (queue_id, dedup_id)`; um `DO NOTHING` simples nunca deixa
    o mesmo `dedup_id` ser reescrito, mesmo depois de `expires_at` passar —
    diferente da spec SQS FIFO, que permite reuso do
    `MessageDeduplicationId` após a janela de 5min. Fix: `ON CONFLICT ...
    DO UPDATE SET ... WHERE message_dedup.expires_at <= now()` — upsert
    condicional que "reaproveita" a linha expirada na hora, sem depender
    do reaper (`reapOnce` em `client.go`) já ter rodado. O reaper existe só
    para reclamar disco, não para garantir corretude.

24. **`MessageRetentionPeriod` não é enforced automaticamente em nenhuma
    tabela.** Diferente do NATS (que usa `MaxAge` nativo do stream), o
    adapter Postgres precisa de um reaper explícito (`reapOnce` em
    `client.go`, chamado a cada `reapInterval` por `reapLoop`) fazendo
    `DELETE FROM messages WHERE enqueued_at < now() - retention`. Sem ele,
    mensagens nunca-recebidas ficam na fila indefinidamente — divergência
    real da spec, não só custo de disco.

25. **Métrica de storage (`ObserveStorage`) chamada explicitamente com
    `nil` hardcoded é uma call-site fácil de esquecer ao copiar padrão
    entre adapters.** `deliverToSubscription` em AMBOS os backends
    (`storage/nats/topics.go` e `storage/postgres/topics.go`) tinha
    `ObserveStorage("publish_deliver", nil, ...)` — falhas de fan-out SNS
    nunca apareciam nas métricas, só em log `Warn` que ninguém monitora
    ativamente. Viola a convenção do §6 do AGENTS.md (usar sempre
    `StartObserve(op).Done(&err)` com named return / var local
    endereçável, nunca `ObserveStorage` explícito). O `Publish` em si
    continua retornando sucesso mesmo com falhas de fan-out — isso é
    intencional (replica a semântica real do SNS: `Publish` só garante que
    a AWS aceitou a mensagem, entrega a subscribers é assíncrona/
    desacoplada) — o bug era só a métrica ficar sempre "ok".

26. **Divergência documentada, não corrigida:** o adapter Postgres não
    mantém um log histórico de mensagens publicadas em tópicos SNS (o NATS
    mantém via stream `topic-<nome>`, retenção `GQ_TOPIC_MAX_AGE`). Ver
    `docs/architecture.md#backend-postgres`. Decisão consciente: hoje nada
    consome esse histórico em nenhum dos dois backends.

27. **`jetstream.StreamConfig.Duplicates` não setado usa o default do
    nats-server (2min), não o valor da spec SQS FIFO (5min).** O adapter
    NATS deduplicava mensagens FIFO numa janela mais curta que o SQS real
    e que o adapter Postgres (`dedupWindow = 5*time.Minute` desde sempre)
    — uma msg reenviada entre 2min e 5min do original não era deduplicada
    no NATS mas seria no Postgres/SQS real. Fix: `cfg.Duplicates =
    dedupWindow` em `streamCfg()` (mesmo nome de const nos dois adapters).
    **Cuidado ao mexer nisso de novo**: `Duplicates` não pode exceder
    `MaxAge` — o nats-server rejeita `CreateStream` nesse caso. Como
    `MessageRetentionPeriod` aceita valores a partir de 60s (spec SQS),
    uma fila de retenção curta com `Duplicates` fixo em 5min quebraria
    `CreateQueue`. O fix capa: `if cfg.Duplicates > cfg.MaxAge { cfg.
    Duplicates = cfg.MaxAge }`. Verificado contra nats-server real (não só
    unitário) antes de considerar resolvido.

28. **Sweep de código morto (whole-program, via `golang.org/x/tools/cmd/
    deadcode ./cmd/dropin-server`) achou um subsistema legado inteiro de
    encoding Query/JSON em `protocol/common.go`, abandonado mas nunca
    deletado.** `server/response.go` tem hoje a versão consolidada
    (`respondQueryXML`/`respondSQSQueryXML`/`respondSNSQueryXML`,
    `writeFatalError`) que todos os handlers em `server/sqs_handlers.go` e
    `server/sns_handlers.go` realmente chamam. As funções antigas em
    `protocol/common.go` (`writeQueryEnvelope`, `EncodeSQSQueryResponse`,
    `EncodeSNSQueryResponse`, `writeQueryError`, `EncodeSQSQueryError`,
    `EncodeSNSQueryError`, `EncodeSNSJSONResponse`, `EncodeSQSJSONError`,
    `EncodeSNSJSONError`) mais `EncodeBase64`
    (`protocol/message_attribute.go`), `ExtractQuerySystemAttributes`
    (`protocol/sqs_query.go`) e o tipo `RequestKind` + método `Validate()`
    (`protocol/types.go`) — tudo isso não tem NENHUM caller de produção,
    só testes dedicados em `protocol/common_test.go`,
    `protocol/sqs_query_test.go` e `protocol/sqs_json_test.go` que
    exercitam a implementação morta isoladamente. Os 4 construtores
    ergonômicos `InvalidParameter`/`MissingParameter`/`Internal`/
    `UnsupportedOperation` em `awserr/awserr.go` também nunca foram
    adotados — call sites em todo o repo constroem `&AWSError{Code: ...,
    Message: ...}` literal diretamente.

    **Decisão consciente (2026-07-25): catalogado, não removido nesta
    branch.** É um cleanup real mas grande (~11 símbolos em `protocol/`,
    exige editar os 3 arquivos de teste cirurgicamente — cada um tem
    dezenas de outros testes válidos misturados) e sem relação nenhuma com
    o trabalho de backend Postgres desta branch. Candidato a PR dedicada
    de limpeza. Itens MENORES do mesmo sweep (só 1 teste dedicado, zero
    caller de produção, remoção isolada sem tocar teste de outra coisa) —
    `PurgeQueueStorage` (storage/nats), `Service.SetNow` +
    `SQSActionResult`/`actionResultTag` (sqs/service.go), `StartSpan`/
    `ObserveAuth`/`ObserveSNSPublish`/`StatusFromHTTP` (observability/),
    `respondJSON`/`detectTransport` (server/) — esses SIM foram removidos
    nesta branch, junto com seus testes dedicados.

## Convenções de código

17. **Commits em português, mensagens detalhadas** descrevendo o porquê da mudança.
    Um commit por mudança lógica. Squash só quando a IA fizer mudanças múltiplas
    numa única rodada.
