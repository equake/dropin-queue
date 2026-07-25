package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/storage"
	"github.com/equake/dropin-queue/shim/pkg/types"
)

// CreateQueue insere a fila. Idempotente: se já existe fila com mesmo
// nome, devolve a existente em vez de erro (igual SQS) — mesma semântica
// do adapter NATS (CreateStream + ErrStreamNameAlreadyInUse).
func (c *Client) CreateQueue(ctx context.Context, q types.Queue) (result *types.Queue, err error) {
	defer observability.StartObserve("create_queue").Done(&err)

	attrs := q.Attributes
	if attrs.VisibilityTimeout == 0 {
		attrs.VisibilityTimeout = 30
	}
	if attrs.MessageRetentionPeriod == 0 {
		attrs.MessageRetentionPeriod = 345600
	}
	if attrs.MaximumMessageSize == 0 {
		attrs.MaximumMessageSize = 262144
	}

	tagsJSON, terr := json.Marshal(q.Tags)
	if terr != nil {
		return nil, fmt.Errorf("marshal tags: %w", terr)
	}

	var id int64
	var createdAt = q.CreatedAt
	err = c.pool.QueryRow(ctx, `
		INSERT INTO queues (name, fifo, visibility_timeout, message_retention_period,
			maximum_message_size, delay_seconds, receive_message_wait_time_seconds,
			content_based_dedup, tags)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
		RETURNING id, created_at, fifo, visibility_timeout, message_retention_period,
			maximum_message_size, delay_seconds, receive_message_wait_time_seconds,
			content_based_dedup, tags
	`, q.Name, q.FIFO, attrs.VisibilityTimeout, attrs.MessageRetentionPeriod,
		attrs.MaximumMessageSize, attrs.DelaySeconds, attrs.ReceiveMessageWaitTimeSeconds,
		attrs.ContentBasedDeduplication, tagsJSON,
	).Scan(&id, &createdAt, &q.FIFO, &attrs.VisibilityTimeout, &attrs.MessageRetentionPeriod,
		&attrs.MaximumMessageSize, &attrs.DelaySeconds, &attrs.ReceiveMessageWaitTimeSeconds,
		&attrs.ContentBasedDeduplication, &tagsJSON)
	if err != nil {
		return nil, fmt.Errorf("create queue: %w", err)
	}

	var tags map[string]string
	if uerr := json.Unmarshal(tagsJSON, &tags); uerr == nil {
		q.Tags = tags
	}

	c.cacheQueue(q.Name, queueIdent{id: id, fifo: q.FIFO})

	q.Attributes = attrs
	q.CreatedAt = createdAt

	observability.L().Info("fila criada", "name", q.Name, "fifo", q.FIFO)
	return &q, nil
}

// GetQueue busca uma fila por nome.
func (c *Client) GetQueue(ctx context.Context, name string) (result *types.Queue, err error) {
	defer observability.StartObserve("get_queue").Done(&err)

	q := &types.Queue{Name: name}
	var id int64
	var tagsJSON []byte
	row := c.pool.QueryRow(ctx, `
		SELECT id, fifo, visibility_timeout, message_retention_period,
			maximum_message_size, delay_seconds, receive_message_wait_time_seconds,
			content_based_dedup, tags, created_at
		FROM queues WHERE name = $1
	`, name)
	if serr := row.Scan(&id, &q.FIFO, &q.Attributes.VisibilityTimeout, &q.Attributes.MessageRetentionPeriod,
		&q.Attributes.MaximumMessageSize, &q.Attributes.DelaySeconds, &q.Attributes.ReceiveMessageWaitTimeSeconds,
		&q.Attributes.ContentBasedDeduplication, &tagsJSON, &q.CreatedAt); serr != nil {
		if errors.Is(serr, pgx.ErrNoRows) {
			return nil, storage.ErrQueueNotFound
		}
		return nil, fmt.Errorf("get queue: %w", serr)
	}
	var tags map[string]string
	if uerr := json.Unmarshal(tagsJSON, &tags); uerr == nil && len(tags) > 0 {
		q.Tags = tags
	}
	c.cacheQueue(name, queueIdent{id: id, fifo: q.FIFO})
	return q, nil
}

// ListQueues lista filas, opcionalmente filtradas por prefixo.
func (c *Client) ListQueues(ctx context.Context, prefix string) (result []types.Queue, err error) {
	defer observability.StartObserve("list_queues").Done(&err)

	rows, qerr := c.pool.Query(ctx, `
		SELECT name, fifo, visibility_timeout, message_retention_period,
			maximum_message_size, delay_seconds, receive_message_wait_time_seconds,
			content_based_dedup, created_at
		FROM queues
		WHERE $1 = '' OR name LIKE $1 || '%'
		ORDER BY name
	`, prefix)
	if qerr != nil {
		return nil, fmt.Errorf("list queues: %w", qerr)
	}
	defer rows.Close()

	var out []types.Queue
	for rows.Next() {
		q := types.Queue{Attributes: types.DefaultQueueAttributes()}
		if serr := rows.Scan(&q.Name, &q.FIFO, &q.Attributes.VisibilityTimeout, &q.Attributes.MessageRetentionPeriod,
			&q.Attributes.MaximumMessageSize, &q.Attributes.DelaySeconds, &q.Attributes.ReceiveMessageWaitTimeSeconds,
			&q.Attributes.ContentBasedDeduplication, &q.CreatedAt); serr != nil {
			return nil, fmt.Errorf("scan queue: %w", serr)
		}
		out = append(out, q)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("list queues rows: %w", rows.Err())
	}
	return out, nil
}

// DeleteQueue remove a fila (e mensagens/dedup/grupos via ON DELETE
// CASCADE). Idempotente — não retorna erro se não existe.
func (c *Client) DeleteQueue(ctx context.Context, name string) (err error) {
	defer observability.StartObserve("delete_queue").Done(&err)

	if _, derr := c.pool.Exec(ctx, `DELETE FROM queues WHERE name = $1`, name); derr != nil {
		return fmt.Errorf("delete queue: %w", derr)
	}
	c.invalidateQueueCache(name)
	observability.L().Info("fila removida", "name", name)
	return nil
}

// PurgeQueue esvazia a fila sem removê-la.
func (c *Client) PurgeQueue(ctx context.Context, name string) (err error) {
	defer observability.StartObserve("purge_queue").Done(&err)

	q, rerr := c.resolveQueue(ctx, name)
	if rerr != nil {
		return rerr
	}
	if _, derr := c.pool.Exec(ctx, `DELETE FROM messages WHERE queue_id = $1`, q.id); derr != nil {
		return fmt.Errorf("purge queue: %w", derr)
	}
	return nil
}

// SetQueueAttributes atualiza atributos mutáveis. Mesma semântica de merge
// do adapter NATS: só sobrescreve campos numéricos com valor > 0 (0 =
// "não fornecido, manter atual"); ContentBasedDeduplication é sempre
// sobrescrito (bool não tem "não fornecido" natural aqui).
func (c *Client) SetQueueAttributes(ctx context.Context, name string, attrs types.QueueAttributes) (err error) {
	defer observability.StartObserve("set_queue_attrs").Done(&err)

	if _, rerr := c.resolveQueue(ctx, name); rerr != nil {
		return rerr
	}

	tag, uerr := c.pool.Exec(ctx, `
		UPDATE queues SET
			visibility_timeout = CASE WHEN $2 > 0 THEN $2 ELSE visibility_timeout END,
			message_retention_period = CASE WHEN $3 > 0 THEN $3 ELSE message_retention_period END,
			maximum_message_size = CASE WHEN $4 > 0 THEN $4 ELSE maximum_message_size END,
			delay_seconds = CASE WHEN $5 > 0 THEN $5 ELSE delay_seconds END,
			receive_message_wait_time_seconds = CASE WHEN $6 > 0 THEN $6 ELSE receive_message_wait_time_seconds END,
			content_based_dedup = $7
		WHERE name = $1
	`, name, attrs.VisibilityTimeout, attrs.MessageRetentionPeriod, attrs.MaximumMessageSize,
		attrs.DelaySeconds, attrs.ReceiveMessageWaitTimeSeconds, attrs.ContentBasedDeduplication)
	if uerr != nil {
		return fmt.Errorf("set queue attributes: %w", uerr)
	}
	if tag.RowsAffected() == 0 {
		return storage.ErrQueueNotFound
	}
	return nil
}
