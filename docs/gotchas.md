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

## Convenções de código

17. **Commits em português, mensagens detalhadas** descrevendo o porquê da mudança.
    Um commit por mudança lógica. Squash só quando a IA fizer mudanças múltiplas
    numa única rodada.
