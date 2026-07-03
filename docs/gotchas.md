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

5. **`isSNSQueryRequest` sniff precisa de 1 MB, não 64 KB.**
   Publish com body > 64 KB era truncado e reescrito como JSON 1.0 silenciosamente.

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

9. **`FetchMaxWait(0)` é rejeitado.** Skip o opt quando `waitSeconds=0` e use deadline
   interno curto (100ms é suficiente).

10. **Consumer EFÊMERO por chamada ReceiveMessage não funciona.**
    Solução atual: 1 consumer durável por fila + cache em memória de mensagens
    pendentes indexado por `(consumerName, sequence)`.

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

15. **ApproximateNumberOfMessages usa `Stream.State.Msgs`** que inclui mensagens já
    acked (lag de update do JetStream). Para count preciso, mantenha counter atômico
    em memória ou use Consumer `num_ack_pending`.

16. **Consumer único por fila (atual).**
    Não suporta múltiplos clients paralelos consumindo da mesma fila. SQS Standard
    permite. Roadmap: sharding por partition key.

## Convenções de código

17. **Commits em português, mensagens detalhadas** descrevendo o porquê da mudança.
    Um commit por mudança lógica. Squash só quando a IA fizer mudanças múltiplas
    numa única rodada.
