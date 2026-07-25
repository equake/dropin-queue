package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/storage"
	"github.com/equake/dropin-queue/shim/pkg/types"
)

// defaultMaxMsgsPerQueue é o backlog máximo de mensagens NÃO-consumidas
// por fila. Com WorkQueuePolicy, mensagens acked são removidas na hora,
// então este limite só é atingido se consumers pararem de consumir.
const defaultMaxMsgsPerQueue = 10_000_000

// dedupWindow é a janela de deduplicação FIFO (Nats-Msg-Id) — mesmo valor
// e mesmo nome de const que storage/postgres/messages.go, para os dois
// backends convergirem no valor real da spec SQS FIFO (5min). Sem setar
// isso, o campo Duplicates do StreamConfig fica zero e o JetStream usa
// seu próprio default de 2min — divergência real encontrada em revisão:
// uma msg reenviada entre 2 e 5min do original não seria deduplicada no
// NATS mas seria no Postgres (e no SQS real).
const dedupWindow = 5 * time.Minute

// streamCfg converte uma Queue em configuração JetStream.
//
// Mapeamentos:
//   - Stream name: "queue-<sanitized-name>" (JetStream não aceita "." em nomes)
//   - Subjects: "q.<name>.>" (wildcard)
//   - Storage: File (persistência em disco)
//   - Replicas: c.replicas (config GQ_STREAM_REPLICAS — nunca inferido)
//   - Retention: WorkQueue — mensagem é APAGADA no ack (DeleteMessage).
//     Igual a SQS: consumida = removida. Sem isso (LimitsPolicy), toda
//     mensagem já consumida ficaria em disco até MaxAge — a milhões de
//     msgs/dia isso multiplica o custo de armazenamento por ordens de
//     magnitude sem nenhum benefício.
//   - Discard: DiscardNew — fila cheia REJEITA o publish com erro
//     (mapeado para throttling na camada service). DiscardOld descartaria
//     silenciosamente mensagens antigas não-consumidas: perda de dados,
//     inaceitável (SQS nunca perde mensagem dentro da retenção).
//   - MaxAge: MessageRetentionPeriod (mensagens não-consumidas expiram)
//   - MaxMsgs: limite duro de backlog para evitar OOM/disco cheio
//
// NOTA: Retention não pode ser alterada em stream existente (JetStream).
// Streams criados por versões antigas (LimitsPolicy) precisam ser
// recriados para adotar WorkQueuePolicy.
func (c *Client) streamCfg(q types.Queue) jetstream.StreamConfig {
	cfg := jetstream.StreamConfig{
		Name:      queueStreamName(q.Name),
		Subjects:  []string{c.queueSubject(q.Name) + ".>"},
		Storage:   jetstream.FileStorage,
		Discard:   jetstream.DiscardNew,
		Retention: jetstream.WorkQueuePolicy,
		Replicas:  c.replicas,
		MaxMsgs:   defaultMaxMsgsPerQueue,
	}

	if q.Attributes.MessageRetentionPeriod > 0 {
		cfg.MaxAge = time.Duration(q.Attributes.MessageRetentionPeriod) * time.Second
	} else {
		cfg.MaxAge = 4 * 24 * time.Hour // SQS default 4 dias
	}

	// Duplicates não pode exceder MaxAge (rejeitado pelo nats-server) —
	// SQS aceita retenção a partir de 60s, bem abaixo dos 5min de
	// dedupWindow, então o cap é necessário para filas de retenção curta.
	cfg.Duplicates = dedupWindow
	if cfg.Duplicates > cfg.MaxAge {
		cfg.Duplicates = cfg.MaxAge
	}

	return cfg
}

// sanitizeStreamName remove caracteres não permitidos em stream names JetStream.
//
// JetStream aceita [A-Za-z0-9-_], max 255 chars. AWS SQS aceita [A-Za-z0-9-_].
// Substituímos tudo que não é alfanumérico/dash/underscore por underscore.
func sanitizeStreamName(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z',
			c >= 'a' && c <= 'z',
			c >= '0' && c <= '9',
			c == '-', c == '_':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	if len(out) > 200 {
		out = out[:200]
	}
	return string(out)
}

// CreateQueue cria um stream JetStream que representa a fila SQS.
//
// Comportamento idempotente: se já existe stream com mesmo nome (após
// sanitização), devolve a fila existente em vez de erro (igual SQS).
//
// Além do stream, persiste metadados em KV bucket (queue_meta) para que
// atributos como VisibilityTimeout sobrevivam ao restart e sejam
// recuperáveis via GetQueue.
func (c *Client) CreateQueue(ctx context.Context, q types.Queue) (result *types.Queue, err error) {
	defer observability.StartObserve("create_queue").Done(&err)

	cfg := c.streamCfg(q)

	// Tenta criar; se já existe, busca e devolve (idempotência SQS).
	s, cerr := c.js.CreateStream(ctx, cfg)
	if cerr != nil {
		if errors.Is(cerr, jetstream.ErrStreamNameAlreadyInUse) {
			// SQS é idempotente em CreateQueue — devolve a existente.
			existing, gerr := c.js.Stream(ctx, cfg.Name)
			if gerr != nil {
				return nil, fmt.Errorf("queue existe mas falhou em buscar: %w", gerr)
			}
			q2 := q
			q2.CreatedAt = existing.CachedInfo().Created
			c.cacheStream(cfg.Name, existing)
			// Tenta carregar metadata existente (pode não ter sido gravada em versão antiga).
			if md, merr := c.loadQueueMetadata(ctx, q.Name); merr == nil {
				q2.Attributes.VisibilityTimeout = md.Attributes["VisibilityTimeout"]
				q2.Attributes.MessageRetentionPeriod = md.Attributes["MessageRetentionPeriod"]
				q2.Attributes.MaximumMessageSize = md.Attributes["MaximumMessageSize"]
				q2.Attributes.DelaySeconds = md.Attributes["DelaySeconds"]
				q2.Attributes.ReceiveMessageWaitTimeSeconds = md.Attributes["ReceiveMessageWaitTimeSeconds"]
				q2.Attributes.ContentBasedDeduplication = md.Attributes["ContentBasedDeduplication"] == 1
				q2.FIFO = md.FIFO
				q2.Tags = md.Tags
			}
			return &q2, nil
		}
		return nil, fmt.Errorf("create stream: %w", cerr)
	}

	c.cacheStream(cfg.Name, s)
	q.CreatedAt = s.CachedInfo().Created

	// Persiste metadados no KV bucket (best-effort — não falhamos o
	// CreateQueue se KV falhar; stream já foi criado).
	if err := c.saveQueueMetadata(ctx, q); err != nil {
		observability.L().Warn("falha ao persistir metadata KV",
			"queue", q.Name, "err", err.Error())
	}

	// Cria o consumer durável da fila já na criação (best-effort).
	// Evita a latência de CreateOrUpdateConsumer no primeiro
	// ReceiveMessage e registra o filter subject no stream WorkQueue.
	visibility := q.Attributes.VisibilityTimeout
	if visibility <= 0 {
		visibility = 30
	}
	if _, cerr := c.ensureQueueConsumer(ctx, s, q.Name, visibility); cerr != nil {
		observability.L().Warn("falha ao pré-criar consumer da fila",
			"queue", q.Name, "err", cerr.Error())
	}

	observability.L().Info("fila criada",
		"name", q.Name,
		"stream", cfg.Name,
		"fifo", q.FIFO,
	)
	return &q, nil
}

// ListQueues lista todas as filas, opcionalmente filtradas por prefixo.
func (c *Client) ListQueues(ctx context.Context, prefix string) (result []types.Queue, err error) {
	defer observability.StartObserve("list_queues").Done(&err)

	// Lista streams cujo nome começa com "queue-".
	lister := c.js.StreamNames(ctx)
	var out []types.Queue
	for name := range lister.Name() {
		if !hasPrefix(name, "queue-") {
			continue
		}
		queueName := name[len("queue-"):]
		if prefix != "" && !hasPrefix(queueName, prefix) {
			continue
		}
		q := types.Queue{
			Name:       queueName,
			URL:        "", // preenchido pelo caller (precisa de endpoint base)
			Attributes: types.DefaultQueueAttributes(),
		}
		out = append(out, q)
	}
	if lerr := lister.Err(); lerr != nil {
		return out, fmt.Errorf("list streams: %w", lerr)
	}
	return out, nil
}

// DeleteQueue remove o stream. Idempotente — não retorna erro se não existe.
func (c *Client) DeleteQueue(ctx context.Context, name string) (err error) {
	defer observability.StartObserve("delete_queue").Done(&err)

	streamName := queueStreamName(name)
	derr := c.js.DeleteStream(ctx, streamName)
	c.invalidateStream(streamName)
	c.invalidateConsumer(queueConsumerName(name))
	if derr != nil && !errors.Is(derr, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("delete stream: %w", derr)
	}
	observability.L().Info("fila removida", "name", name, "stream", streamName)
	return nil
}

// PurgeQueue esvazia o stream sem removê-lo.
func (c *Client) PurgeQueue(ctx context.Context, name string) (err error) {
	defer observability.StartObserve("purge_queue").Done(&err)

	streamName := queueStreamName(name)
	s, serr := c.js.Stream(ctx, streamName)
	if serr != nil {
		if errors.Is(serr, jetstream.ErrStreamNotFound) {
			return storage.ErrQueueNotFound
		}
		return fmt.Errorf("get stream: %w", serr)
	}
	if perr := s.Purge(ctx); perr != nil {
		return fmt.Errorf("purge: %w", perr)
	}
	return nil
}

func hasPrefix(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}
