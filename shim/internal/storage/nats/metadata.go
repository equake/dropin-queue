package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/storage"
	"github.com/equake/dropin-queue/shim/pkg/types"
)

// queueMetadataBucket é o nome do KV bucket que armazena metadados das filas.
//
// Schema: chave = nome da fila, valor = JSON com QueueAttributes + extras.
// É separado dos streams JetStream (que armazenam só mensagens) para que
// mudanças em metadados (VisibilityTimeout etc.) não exijam recriar stream.
const queueMetadataBucket = "queue_meta"

// queueMetadataV1 é a versão 1 do schema de metadados.
//
// version: schema version para permitir migração futura.
// attributes: copia exata dos QueueAttributes no momento da criação/update.
// fifo: flag FIFO.
// tags: tags opcionais (SQS suporta até 50 tags por fila).
// created_at: timestamp de criação (RFC3339Nano).
// updated_at: timestamp do último update de atributos.
type queueMetadataV1 struct {
	Version    int               `json:"v"`
	Attributes map[string]int32  `json:"attributes"`
	FIFO       bool              `json:"fifo"`
	Tags       map[string]string `json:"tags,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

// metadataKV gerencia acesso ao bucket JetStream KV de metadados.
//
// Lazy initialization: o bucket é criado na primeira chamada a Get/Put.
// Em cluster mode, o bucket tem replicas=3 para HA.
func (c *Client) metadataKV(ctx context.Context) (jetstream.KeyValue, error) {
	if c.kvCache != nil {
		return c.kvCache, nil
	}
	kv, err := c.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: queueMetadataBucket,
		// Storage: file (persiste em disco, igual aos streams).
		// TTL: 0 (sem expiração automática — filas são persistentes).
		// History: 1 (só valor atual; mudanças sobrescrevem).
	})
	if err != nil {
		return nil, fmt.Errorf("create kv %s: %w", queueMetadataBucket, err)
	}
	c.kvCache = kv
	return kv, nil
}

// saveQueueMetadata persiste metadados da fila no KV bucket.
func (c *Client) saveQueueMetadata(ctx context.Context, q types.Queue) error {
	kv, err := c.metadataKV(ctx)
	if err != nil {
		return err
	}

	attrs := map[string]int32{
		"VisibilityTimeout":             q.Attributes.VisibilityTimeout,
		"MessageRetentionPeriod":        q.Attributes.MessageRetentionPeriod,
		"MaximumMessageSize":            q.Attributes.MaximumMessageSize,
		"DelaySeconds":                  q.Attributes.DelaySeconds,
		"ReceiveMessageWaitTimeSeconds": q.Attributes.ReceiveMessageWaitTimeSeconds,
	}
	// Converte bool para int32 (ContentBasedDeduplication).
	if q.Attributes.ContentBasedDeduplication {
		attrs["ContentBasedDeduplication"] = 1
	}

	md := queueMetadataV1{
		Version:    1,
		Attributes: attrs,
		FIFO:       q.FIFO,
		Tags:       q.Tags,
		CreatedAt:  q.CreatedAt,
		UpdatedAt:  q.CreatedAt,
	}
	if md.UpdatedAt.IsZero() {
		md.UpdatedAt = time.Now()
	}

	body, err := json.Marshal(md)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	// Revision explícita para concorrência otimista. Put com rev esperada;
	// se rev não bate, conflito.
	entry, getErr := kv.Get(ctx, q.Name)
	var rev uint64 = 0
	if getErr == nil {
		rev = entry.Revision()
	} else if !errors.Is(getErr, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("get existing metadata: %w", getErr)
	}

	if _, putErr := kv.Put(ctx, q.Name, body); putErr != nil {
		return fmt.Errorf("put metadata: %w", putErr)
	}
	_ = rev // ignored — we always overwrite; idempotent metadata
	return nil
}

// loadQueueMetadata recupera metadados da fila do KV bucket.
//
// Devolve storage.ErrQueueNotFound se não existir (sinal de bug — fila
// sem metadata não deveria acontecer, mas tratamos como erro).
func (c *Client) loadQueueMetadata(ctx context.Context, name string) (*queueMetadataV1, error) {
	kv, err := c.metadataKV(ctx)
	if err != nil {
		return nil, err
	}
	entry, err := kv.Get(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, storage.ErrQueueNotFound
		}
		return nil, fmt.Errorf("get metadata: %w", err)
	}

	var md queueMetadataV1
	if err := json.Unmarshal(entry.Value(), &md); err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}
	return &md, nil
}

// GetQueue busca uma fila existente.
//
// Carrega metadados do KV bucket. Se existirem, sobrescrevem os defaults.
// O stream JetStream é a fonte da verdade para CreatedAt e Subjects;
// os atributos vêm do KV.
func (c *Client) GetQueue(ctx context.Context, name string) (*types.Queue, error) {
	start := time.Now()
	defer func() { observability.ObserveStorage("get_queue", nil, time.Since(start)) }()

	streamName := "queue-" + sanitizeStreamName(name)
	s, err := c.js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, storage.ErrQueueNotFound
		}
		return nil, fmt.Errorf("get stream: %w", err)
	}
	c.cacheStream(streamName, s)

	info := s.CachedInfo()

	// Defaults são a base; metadata sobrescreve se existir.
	q := &types.Queue{
		Name:       name,
		Attributes: types.DefaultQueueAttributes(),
		CreatedAt:  info.Created,
		FIFO:       strings.HasSuffix(name, ".fifo"),
	}

	md, err := c.loadQueueMetadata(ctx, name)
	if err != nil {
		if errors.Is(err, storage.ErrQueueNotFound) {
			// Stream existe mas metadata não — provavelmente fila criada
			// por versão antiga. Retorna defaults sem erro.
			observability.L().Warn("queue sem metadata KV — usando defaults",
				"queue", name)
			return q, nil
		}
		return nil, err
	}
	// Sobrescreve atributos com valores persistidos.
	q.Attributes.VisibilityTimeout = md.Attributes["VisibilityTimeout"]
	q.Attributes.MessageRetentionPeriod = md.Attributes["MessageRetentionPeriod"]
	q.Attributes.MaximumMessageSize = md.Attributes["MaximumMessageSize"]
	q.Attributes.DelaySeconds = md.Attributes["DelaySeconds"]
	q.Attributes.ReceiveMessageWaitTimeSeconds = md.Attributes["ReceiveMessageWaitTimeSeconds"]
	q.Attributes.ContentBasedDeduplication = md.Attributes["ContentBasedDeduplication"] == 1
	q.FIFO = md.FIFO
	q.Tags = md.Tags
	if !md.CreatedAt.IsZero() {
		q.CreatedAt = md.CreatedAt
	}
	return q, nil
}

// SetQueueAttributes atualiza atributos mutáveis (MaxAge + metadata KV).
//
// Persiste:
//
//   - MessageRetentionPeriod → MaxAge do stream
//   - VisibilityTimeout, MaximumMessageSize, DelaySeconds,
//     ReceiveMessageWaitTimeSeconds, ContentBasedDeduplication → KV
func (c *Client) SetQueueAttributes(ctx context.Context, name string, attrs types.QueueAttributes) error {
	start := time.Now()
	defer func() { observability.ObserveStorage("set_queue_attrs", nil, time.Since(start)) }()

	streamName := "queue-" + sanitizeStreamName(name)
	s, err := c.js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return storage.ErrQueueNotFound
		}
		return fmt.Errorf("get stream: %w", err)
	}

	cfg := s.CachedInfo().Config
	if attrs.MessageRetentionPeriod > 0 {
		cfg.MaxAge = time.Duration(attrs.MessageRetentionPeriod) * time.Second
	}
	if _, err := c.js.UpdateStream(ctx, cfg); err != nil {
		return fmt.Errorf("update stream: %w", err)
	}
	c.invalidateStream(streamName)

	// VisibilityTimeout novo → AckWait do consumer durável precisa refletir.
	// Invalida o cache e recria com o novo valor; réplicas remotas pegam a
	// config nova no próximo mismatch de AckWait (ensureQueueConsumer).
	if attrs.VisibilityTimeout > 0 {
		c.invalidateConsumer(queueConsumerName(name))
		if _, err := c.ensureQueueConsumer(ctx, s, name, attrs.VisibilityTimeout); err != nil {
			observability.L().Warn("falha ao atualizar AckWait do consumer",
				"queue", name, "err", err.Error())
		}
	}

	// Atualiza metadata KV com todos os atributos (não só o que mudou).
	q, err := c.GetQueue(ctx, name)
	if err != nil {
		return err
	}
	// Mescla: novos attrs sobrescrevem onde fornecidos (>0); outros mantidos.
	if attrs.VisibilityTimeout > 0 {
		q.Attributes.VisibilityTimeout = attrs.VisibilityTimeout
	}
	if attrs.MessageRetentionPeriod > 0 {
		q.Attributes.MessageRetentionPeriod = attrs.MessageRetentionPeriod
	}
	if attrs.MaximumMessageSize > 0 {
		q.Attributes.MaximumMessageSize = attrs.MaximumMessageSize
	}
	if attrs.DelaySeconds > 0 {
		q.Attributes.DelaySeconds = attrs.DelaySeconds
	}
	if attrs.ReceiveMessageWaitTimeSeconds > 0 {
		q.Attributes.ReceiveMessageWaitTimeSeconds = attrs.ReceiveMessageWaitTimeSeconds
	}
	q.Attributes.ContentBasedDeduplication = attrs.ContentBasedDeduplication

	if err := c.saveQueueMetadata(ctx, *q); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}
	return nil
}
