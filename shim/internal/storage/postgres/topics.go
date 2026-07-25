package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/protocol"
	"github.com/equake/dropin-queue/shim/internal/storage"
	"github.com/equake/dropin-queue/shim/pkg/types"
)

// CreateTopic insere o tópico. Idempotente — mesmo padrão de CreateQueue.
func (c *Client) CreateTopic(ctx context.Context, t types.Topic) (result *types.Topic, err error) {
	defer observability.StartObserve("create_topic").Done(&err)

	var id int64
	var createdAt time.Time
	qerr := c.pool.QueryRow(ctx, `
		INSERT INTO topics (name) VALUES ($1)
		ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
		RETURNING id, created_at
	`, t.Name).Scan(&id, &createdAt)
	if qerr != nil {
		return nil, fmt.Errorf("create topic: %w", qerr)
	}

	c.cacheTopic(t.Name, id)
	t.CreatedAt = createdAt
	observability.L().Info("tópico criado", "name", t.Name)
	return &t, nil
}

// GetTopic busca um tópico existente.
func (c *Client) GetTopic(ctx context.Context, name string) (result *types.Topic, err error) {
	defer observability.StartObserve("get_topic").Done(&err)

	t := &types.Topic{Name: name}
	var id int64
	qerr := c.pool.QueryRow(ctx, `SELECT id, created_at FROM topics WHERE name = $1`, name).Scan(&id, &t.CreatedAt)
	if qerr != nil {
		if errors.Is(qerr, pgx.ErrNoRows) {
			return nil, storage.ErrTopicNotFound
		}
		return nil, fmt.Errorf("get topic: %w", qerr)
	}
	c.cacheTopic(name, id)
	return t, nil
}

// ListTopics lista tópicos, opcionalmente filtrados por prefixo.
func (c *Client) ListTopics(ctx context.Context, prefix string) (result []types.Topic, err error) {
	defer observability.StartObserve("list_topics").Done(&err)

	rows, qerr := c.pool.Query(ctx, `
		SELECT name, created_at FROM topics
		WHERE $1 = '' OR name LIKE $1 || '%'
		ORDER BY name
	`, prefix)
	if qerr != nil {
		return nil, fmt.Errorf("list topics: %w", qerr)
	}
	defer rows.Close()

	var out []types.Topic
	for rows.Next() {
		var t types.Topic
		if serr := rows.Scan(&t.Name, &t.CreatedAt); serr != nil {
			return nil, fmt.Errorf("scan topic: %w", serr)
		}
		out = append(out, t)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("list topics rows: %w", rows.Err())
	}
	return out, nil
}

// DeleteTopic remove o tópico. Subscriptions são removidas em cascata
// (ON DELETE CASCADE via topic_id).
func (c *Client) DeleteTopic(ctx context.Context, name string) (err error) {
	defer observability.StartObserve("delete_topic").Done(&err)

	if _, derr := c.pool.Exec(ctx, `DELETE FROM topics WHERE name = $1`, name); derr != nil {
		return fmt.Errorf("delete topic: %w", derr)
	}
	c.invalidateTopicCache(name)
	observability.L().Info("tópico removido", "name", name)
	return nil
}

// Subscribe adiciona uma inscrição a um tópico.
//
// sub.TopicARN já vem completo (a camada sns/ monta account/region antes
// de chamar o storage — o storage não conhece esses campos, só persiste).
func (c *Client) Subscribe(ctx context.Context, sub types.Subscription) (result *types.Subscription, err error) {
	defer observability.StartObserve("subscribe").Done(&err)

	topicName := protocol.ResourceNameFromARN(sub.TopicARN)
	topicID, terr := c.resolveTopic(ctx, topicName)
	if terr != nil {
		return nil, terr
	}

	if sub.ARN == "" {
		sub.ARN = generateSubscriptionARN(sub.TopicARN, sub.Protocol, sub.Endpoint)
	}
	if sub.Protocol == "http" || sub.Protocol == "https" {
		sub.Pending = true
	} else if sub.Protocol == "sqs" {
		sub.Pending = false
	}

	var createdAt time.Time
	qerr := c.pool.QueryRow(ctx, `
		INSERT INTO subscriptions (arn, topic_id, topic_arn, protocol, endpoint, filter_policy, raw_delivery, pending)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (arn) DO UPDATE SET arn = EXCLUDED.arn
		RETURNING created_at
	`, sub.ARN, topicID, sub.TopicARN, sub.Protocol, sub.Endpoint, sub.FilterPolicy, sub.RawDelivery, sub.Pending).Scan(&createdAt)
	if qerr != nil {
		return nil, fmt.Errorf("subscribe: %w", qerr)
	}
	sub.CreatedAt = createdAt

	observability.L().Info("inscrição criada",
		"arn", sub.ARN, "topic", sub.TopicARN, "protocol", sub.Protocol, "endpoint", sub.Endpoint)
	return &sub, nil
}

// ListSubscriptions lista inscrições, opcionalmente filtradas por tópico.
func (c *Client) ListSubscriptions(ctx context.Context, topicName string) (result []types.Subscription, err error) {
	defer observability.StartObserve("list_subscriptions").Done(&err)

	rows, qerr := c.pool.Query(ctx, `
		SELECT s.arn, s.topic_arn, s.protocol, s.endpoint, s.filter_policy, s.raw_delivery, s.pending, s.created_at
		FROM subscriptions s
		JOIN topics t ON t.id = s.topic_id
		WHERE $1 = '' OR t.name = $1
		ORDER BY s.arn
	`, topicName)
	if qerr != nil {
		return nil, fmt.Errorf("list subscriptions: %w", qerr)
	}
	defer rows.Close()

	var out []types.Subscription
	for rows.Next() {
		var s types.Subscription
		if serr := rows.Scan(&s.ARN, &s.TopicARN, &s.Protocol, &s.Endpoint, &s.FilterPolicy, &s.RawDelivery, &s.Pending, &s.CreatedAt); serr != nil {
			return nil, fmt.Errorf("scan subscription: %w", serr)
		}
		out = append(out, s)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("list subscriptions rows: %w", rows.Err())
	}
	return out, nil
}

// Unsubscribe remove uma inscrição pelo ARN. Idempotente — não retorna
// erro se o ARN não existir, mesma semântica do adapter NATS (kv.Delete()
// do JetStream não retorna ErrKeyNotFound para chave inexistente, só
// Get/Purge — então o Delete lá é idempotente por natureza da API; aqui
// replicamos explicitamente, já que DELETE de SQL também é naturalmente
// idempotente: 0 linhas afetadas não é erro).
func (c *Client) Unsubscribe(ctx context.Context, subscriptionARN string) (err error) {
	defer observability.StartObserve("unsubscribe").Done(&err)

	if _, derr := c.pool.Exec(ctx, `DELETE FROM subscriptions WHERE arn = $1`, subscriptionARN); derr != nil {
		return fmt.Errorf("unsubscribe: %w", derr)
	}
	observability.L().Info("inscrição removida", "arn", subscriptionARN)
	return nil
}

// Publish publica uma mensagem em um tópico + faz fan-out síncrono para
// subscribers — mesmo modelo síncrono do adapter NATS (roadmap: async
// com DLQ, ainda não implementado em nenhum dos dois backends).
func (c *Client) Publish(ctx context.Context, topicName string, msg *types.Message) (result *types.Message, err error) {
	defer observability.StartObserve("publish").Done(&err)

	if msg == nil {
		return nil, storage.ErrInvalidArgument("msg é nil")
	}
	topicID, terr := c.resolveTopic(ctx, topicName)
	if terr != nil {
		return nil, terr
	}

	msg.EnqueuedAt = time.Now().UTC()
	msg.ID = strconv.FormatInt(topicID, 10) + "-" + strconv.FormatInt(msg.EnqueuedAt.UnixNano(), 10)

	subs, lerr := c.ListSubscriptions(ctx, topicName)
	if lerr != nil {
		return msg, nil // msg "publicada" (não hã stream de tópico para persistir aqui), fan-out falhou
	}

	// Fan-out com bound de concorrência — mesmo raciocínio do adapter
	// NATS: sem o bound, um tópico com milhares de subscriptions dispara
	// milhares de goroutines por Publish, todas competindo pelo pool.
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxFanoutConcurrency)
	for _, sub := range subs {
		if sub.Pending {
			observability.L().Debug("skip pending subscription", "arn", sub.ARN)
			continue
		}
		if !matchesFilterPolicy(sub.FilterPolicy, msg) {
			observability.L().Debug("skip non-matching filter policy", "arn", sub.ARN)
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			observability.L().Warn("fan-out interrompido por ctx", "topic", topicName)
			wg.Wait()
			return msg, nil
		}
		wg.Add(1)
		subCopy := sub
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			c.deliverToSubscription(ctx, &subCopy, msg)
		}()
	}
	wg.Wait()

	return msg, nil
}

// maxFanoutConcurrency limita deliveries de fan-out em paralelo por
// Publish — mesmo valor do adapter NATS.
const maxFanoutConcurrency = 16

// deliverToSubscription entrega a mensagem a uma inscrição. SQS: SendMessage
// síncrono na fila destino. HTTP/HTTPS/outros: não implementado no MVP,
// mesma limitação documentada no adapter NATS.
func (c *Client) deliverToSubscription(ctx context.Context, sub *types.Subscription, msg *types.Message) {
	start := time.Now()
	defer func() { observability.ObserveStorage("publish_deliver", nil, time.Since(start)) }()

	switch sub.Protocol {
	case "sqs":
		queueName := protocol.QueueNameFromURL(sub.Endpoint)
		if queueName == "" {
			observability.L().Warn("SQS subscription com endpoint inválido", "endpoint", sub.Endpoint)
			return
		}
		delivery := &types.Message{
			Body:              msg.Body,
			MessageAttributes: msg.MessageAttributes,
		}
		if _, err := c.SendMessage(ctx, queueName, delivery); err != nil {
			observability.L().Warn("fan-out SQS failed",
				"sub_arn", sub.ARN, "queue", queueName, "err", err.Error())
			return
		}
		observability.L().Debug("fan-out SQS ok", "sub_arn", sub.ARN, "queue", queueName)
	default:
		observability.L().Debug("skip unsupported protocol", "protocol", sub.Protocol, "arn", sub.ARN)
	}
}

// matchesFilterPolicy verifica se a mensagem passa pelo filter policy —
// mesma lógica do adapter NATS (JSON: attrName -> lista de valores aceitos).
func matchesFilterPolicy(policyJSON string, msg *types.Message) bool {
	if policyJSON == "" {
		return true
	}
	var policy map[string][]string
	if err := json.Unmarshal([]byte(policyJSON), &policy); err != nil {
		return true
	}
	for attrName, allowed := range policy {
		var actual string
		if ma, ok := msg.MessageAttributes[attrName]; ok {
			actual = ma.StringValue
		} else if sa, ok := msg.Attributes[attrName]; ok {
			actual = sa
		} else {
			return false
		}
		matched := false
		for _, v := range allowed {
			if v == actual {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// generateSubscriptionARN gera um ARN único para uma inscrição — mesma
// lógica do adapter NATS.
func generateSubscriptionARN(topicARN, protocolName, endpoint string) string {
	h := sha256.Sum256([]byte(topicARN + ":" + protocolName + ":" + endpoint + ":" + strconv.FormatInt(time.Now().UnixNano(), 10)))
	return topicARN + ":" + hex.EncodeToString(h[:8])
}

// resolveTopic busca o id do tópico, usando o cache quando possível.
func (c *Client) resolveTopic(ctx context.Context, name string) (int64, error) {
	if id, ok := c.cachedTopic(name); ok {
		return id, nil
	}
	var id int64
	err := c.pool.QueryRow(ctx, `SELECT id FROM topics WHERE name = $1`, name).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, storage.ErrTopicNotFound
		}
		return 0, fmt.Errorf("resolve topic: %w", err)
	}
	c.cacheTopic(name, id)
	return id, nil
}
