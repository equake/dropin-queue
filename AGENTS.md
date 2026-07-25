# AGENTS.md — Diretrizes para Agentes IA no dropin-queue

> Este arquivo é lido por qualquer agente IA (Claude Code, Aider, Cursor, Continue, etc)
> ao iniciar uma tarefa neste repositório. Siga-o antes de tomar decisões.

---

## 1. Princípios (não negociáveis)

1. **Atualize o README em toda mudança visível ao usuário.**
   Adição de operação AWS, novo flag, novo atributo, mudança de protocolo,
   nova dependência — qualquer coisa que afete como o projeto é usado entra no README.
   A tabela "Compatibilidade AWS — Status atual" é a fonte da verdade sobre o que está pronto.

2. **Spec-driven. AWS API é a verdade.**
   Antes de implementar uma operação SQS/SNS:
   - Leia a spec oficial em `https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/`
     ou `https://docs.aws.amazon.com/sns/latest/api/API_Operations.html`
   - Use o **botocore** (`/home/seiwa/venv/lib/python3.12/site-packages/botocore/data/sqs/*`)
     como referência do wire format exato (request + response)
   - Use o **boto3** para validar comportamento do cliente (especialmente flags opcionais)
   Nunca adivinhe nomes de campos ou formatos. Quando a spec diz `MessageAttributes.entry.N.Name.Value.StringValue`,
   é exatamente isso que o cliente manda.

3. **E2E é o critério de "pronto".**
   Nada é considerado feito sem teste E2E passando contra o shim rodando em docker-compose.
   - Teste unitário Go: implementa a lógica
   - Teste E2E Python (boto3): valida que clientes AWS reais funcionam
   - Se você só consegue passar unit mas E2E falha, você não terminou

4. **Pense em produção desde o dia 1.**
   - Escalabilidade: o design atual assume 1 consumer durável por fila. Para multi-consumer
     (Standard), use sharding por partition key ou round-robin assignment.
   - Manutenibilidade: erros tipados (não strings), tipos explícitos em APIs internas,
     logs estruturados JSON via `observability.L()`, nunca `fmt.Println` em código de produção.
   - Segurança: nenhuma credencial em logs, nenhum segredo em testes, paths de credenciais
     sob `/etc/dropin-server/` (não hardcoded).

---

## 2. Comandos essenciais

```bash
make up              # sobe NATS + MinIO + dropin-server
make build           # compila shim/bin/dropin-server
make test            # roda testes Go (com -race)
make test-int        # roda pytest contra shim rodando
make down            # derruba stack
make logs-shim       # tail logs do shim (JSON estruturado)
```

Stack:
- Go 1.25 (em `/home/seiwa/go/bin/go`)
- Python 3.12 + boto3 1.43 + pytest 9 (em `/home/seiwa/venv`)
- Docker Compose v2
- NATS JetStream 2.10, MinIO latest

---

## 3. Onde encontrar o quê

- `README.md` — overview, status, quickstart, arquitetura
- `docs/architecture.md` — arquitetura detalhada
- `docs/gotchas.md` — **LEIA ANTES DE IMPLEMENTAR** — bugs que já nos custaram tempo
- `shim/internal/protocol/` — parsers/serializers AWS (referência para dual protocol)
- `shim/internal/storage/nats/` — adapter NATS JetStream (referência para KV + streams)
- `shim/internal/storage/postgres/` — adapter Postgres (LISTEN/NOTIFY + SKIP LOCKED),
  trocável via `GQ_BACKEND`; ver `docs/architecture.md#backend-postgres`
- `shim/internal/awserr/` — type AWSError compartilhado entre SQS e SNS; sender-fault registry
- `shim/test/integration/` — testes E2E boto3 (referência para uso correto da API).
  Rodam contra os dois backends sem mudança (`make test-int` / `make test-int-postgres`) —
  **nunca hardcode `http://localhost:4566` em teste novo**, use a fixture
  `sqs_client`/`sns_client` de `conftest.py` ou importe `SHIM_ENDPOINT` de lá
  (ver gotcha #22 em `docs/gotchas.md`)

---

## 4. Antes de declarar "pronto"

- [ ] `make test` passa (testes Go com race detector)
- [ ] Teste E2E adicionado em `shim/test/integration/` para a nova funcionalidade
- [ ] `make test-int` passa (70/70 mínimo; deve crescer a cada feature) — atualmente **72/72** pós-refactor/kiss-dry-pass-1
- [ ] README atualizado: tabela de compatibilidade + seção "Funcionalidades" + comando quickstart se aplicável
- [ ] Commit em português, mensagem detalhada descrevendo o **porquê** (não só o quê)
- [ ] `git push origin main` e atualize descrição do repo se a feature mudar o escopo

## 5. Atribuição de commits

**Regra:** cada committer é responsável pela **sua própria** atribuição.
Não usar `Co-Authored-By:` creditando outra pessoa/agente que não
participou da escrita daquele diff específico.

Caso histórico (não repetir): o assistente opencode/minimax-m3
copiou footers `Co-Authored-By: Claude Fable <noreply@anthropic.com>`
vistos em commits antigos do repo. Resultou em 9 commits com atribuição
falsa (audit claim errada em git history). O humano autorizou deixar
esses 9 commits sem reescrever histórico; futuros commits são limpos.

## 6. Convenções de instrumentação (refactor/kiss-dry-pass-1)

- **Métricas de storage**: usar SEMPRE `observability.StartObserve(op).Done(&err)`
  em método com named return + `defer`. Chamada explícita de
  `ObserveStorage` é proibida (causa double-count: pré-fix gerava
  2× `Inc()` + 2× `Observe()` em sucesso).
- **Error writers HTTP**: usar `server.writeFatalError(w, transport, code,
  msg, requestID)`. As 4 funções `write<X><Y>FatalError` foram consolidadas
  em 1 (Commit 6).
- **ARN/URL parsing**: usar `protocol.ResourceNameFromARN(arn)` e
  `protocol.QueueNameFromURL(endpoint)`. Não escrever `strings.Split`
  inline — formato ARN é estruturalmente idêntico entre SQS/SNS.
- **Stream names**: usar `queueStreamName(name)` e `topicStreamName(name)`
  em `storage/nats/client.go`. Não escrever `"queue-"+sanitize...` inline
  — formato é o índice primário de dados persistidos; rename = data loss.
