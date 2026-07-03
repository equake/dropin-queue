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

// streamCfg converte uma Queue em configuração JetStream.
//
// Mapeamentos:
//   - Stream name: "queue-<sanitized-name>" (JetStream não aceita "." em nomes)
//   - Subjects: "q.<name>.>" (wildcard)
//   - Storage: File (persistência em disco)
//   - Replicas: 3 em produção, 1 em dev
//   - Retention: Limits (apaga mensagens após MaxAge ou MaxMsgs)
//   - MaxAge: MessageRetentionPeriod
//   - MaxMsgs: limite duro para evitar OOM
func (c *Client) streamCfg(q types.Queue, devMode bool) jetstream.StreamConfig {
	cfg := jetstream.StreamConfig{
		Name:     "queue-" + sanitizeStreamName(q.Name),
		Subjects: []string{c.queueSubject(q.Name) + ".>"},
		Storage:  jetstream.FileStorage,
		Discard:  jetstream.DiscardOld,
	}

	if devMode {
		cfg.Replicas = 1
	} else {
		cfg.Replicas = 3
	}

	// Mensagens: limite default 10M por fila. Ajustável via attr.
	cfg.MaxMsgs = 10_000_000

	// Retention
	cfg.Retention = jetstream.LimitsPolicy

	if q.Attributes.MessageRetentionPeriod > 0 {
		cfg.MaxAge = time.Duration(q.Attributes.MessageRetentionPeriod) * time.Second
	} else {
		cfg.MaxAge = 4 * 24 * time.Hour // SQS default 4 dias
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
func (c *Client) CreateQueue(ctx context.Context, q types.Queue) (*types.Queue, error) {
	start := time.Now()
	defer func() { observability.ObserveStorage("create_queue", nil, time.Since(start)) }()

	cfg := c.streamCfg(q, c.isDevMode())

	// Tenta criar; se já existe, busca e devolve (idempotência SQS).
	s, err := c.js.CreateStream(ctx, cfg)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
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
		observability.ObserveStorage("create_queue", err, time.Since(start))
		return nil, fmt.Errorf("create stream: %w", err)
	}

	c.cacheStream(cfg.Name, s)
	q.CreatedAt = s.CachedInfo().Created

	// Persiste metadados no KV bucket (best-effort — não falhamos o
	// CreateQueue se KV falhar; stream já foi criado).
	if err := c.saveQueueMetadata(ctx, q); err != nil {
		observability.L().Warn("falha ao persistir metadata KV",
			"queue", q.Name, "err", err.Error())
	}

	observability.L().Info("fila criada",
		"name", q.Name,
		"stream", cfg.Name,
		"fifo", q.FIFO,
	)
	return &q, nil
}

// ListQueues lista todas as filas, opcionalmente filtradas por prefixo.
func (c *Client) ListQueues(ctx context.Context, prefix string) ([]types.Queue, error) {
	start := time.Now()
	defer func() { observability.ObserveStorage("list_queues", nil, time.Since(start)) }()

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
	if err := lister.Err(); err != nil {
		return out, fmt.Errorf("list streams: %w", err)
	}
	return out, nil
}

// DeleteQueue remove o stream. Idempotente — não retorna erro se não existe.
func (c *Client) DeleteQueue(ctx context.Context, name string) error {
	start := time.Now()
	defer func() { observability.ObserveStorage("delete_queue", nil, time.Since(start)) }()

	streamName := "queue-" + sanitizeStreamName(name)
	err := c.js.DeleteStream(ctx, streamName)
	c.invalidateStream(streamName)
	if err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("delete stream: %w", err)
	}
	observability.L().Info("fila removida", "name", name, "stream", streamName)
	return nil
}

// PurgeQueue esvazia o stream sem removê-lo.
func (c *Client) PurgeQueue(ctx context.Context, name string) error {
	start := time.Now()
	defer func() { observability.ObserveStorage("purge_queue", nil, time.Since(start)) }()

	streamName := "queue-" + sanitizeStreamName(name)
	s, err := c.js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return storage.ErrQueueNotFound
		}
		return fmt.Errorf("get stream: %w", err)
	}
	if err := s.Purge(ctx); err != nil {
		return fmt.Errorf("purge: %w", err)
	}
	return nil
}

// isDevMode detecta se estamos em modo dev (1 réplica).
//
// Critério atual: sem prefixo definido E nome "dev" em env. Será refinado
// quando tivermos config injetada.
func (c *Client) isDevMode() bool {
	return c.prefix == ""
}

func hasPrefix(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}
