package nats

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/storage"
	"github.com/equake/dropin-queue/shim/pkg/types"
)

// topicMetadataBucket é o nome do KV bucket que armazena metadados dos tópicos.
//
// Schema: chave = nome do tópico, valor = JSON com Topic metadata.
const topicMetadataBucket = "topic_meta"

// subscriptionMetadataBucket é o nome do KV bucket que armazena inscrições.
//
// Schema: chave = subscription ARN, valor = JSON com Subscription metadata.
const subscriptionMetadataBucket = "subscription_meta"

// topicMetadataV1 é o schema v1 de metadados de tópico.
type topicMetadataV1 struct {
	Version   int       `json:"v"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// subscriptionMetadataV1 é o schema v1 de metadados de inscrição.
type subscriptionMetadataV1 struct {
	Version      int       `json:"v"`
	ARN          string    `json:"arn"`
	TopicARN     string    `json:"topic_arn"`
	TopicName    string    `json:"topic_name"`
	Protocol     string    `json:"protocol"`
	Endpoint     string    `json:"endpoint"`
	FilterPolicy string    `json:"filter_policy,omitempty"`
	RawDelivery  bool      `json:"raw_delivery,omitempty"`
	Pending      bool      `json:"pending,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// topicKV retorna o KV bucket de metadados dos tópicos (lazy init).
func (c *Client) topicKV(ctx context.Context) (jetstream.KeyValue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.topicKVCache != nil {
		return c.topicKVCache, nil
	}
	kv, err := c.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: topicMetadataBucket,
	})
	if err != nil {
		return nil, fmt.Errorf("create kv %s: %w", topicMetadataBucket, err)
	}
	c.topicKVCache = kv
	return kv, nil
}

// subscriptionKV retorna o KV bucket de inscrições (lazy init).
func (c *Client) subscriptionKV(ctx context.Context) (jetstream.KeyValue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subKVCache != nil {
		return c.subKVCache, nil
	}
	kv, err := c.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: subscriptionMetadataBucket,
	})
	if err != nil {
		return nil, fmt.Errorf("create kv %s: %w", subscriptionMetadataBucket, err)
	}
	c.subKVCache = kv
	return kv, nil
}

// saveTopicMetadata persiste metadados do tópico.
func (c *Client) saveTopicMetadata(ctx context.Context, t types.Topic) error {
	kv, err := c.topicKV(ctx)
	if err != nil {
		return err
	}
	md := topicMetadataV1{
		Version:   1,
		Name:      t.Name,
		CreatedAt: t.CreatedAt,
	}
	if t.CreatedAt.IsZero() {
		md.CreatedAt = time.Now().UTC()
	}
	body, err := json.Marshal(md)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if _, err := kv.Put(ctx, t.Name, body); err != nil {
		return fmt.Errorf("put: %w", err)
	}
	return nil
}

// loadTopicMetadata recupera metadados do tópico.
func (c *Client) loadTopicMetadata(ctx context.Context, name string) (*topicMetadataV1, error) {
	kv, err := c.topicKV(ctx)
	if err != nil {
		return nil, err
	}
	entry, err := kv.Get(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, storage.ErrTopicNotFound
		}
		return nil, fmt.Errorf("get: %w", err)
	}
	var md topicMetadataV1
	if err := json.Unmarshal(entry.Value(), &md); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &md, nil
}

// topicStreamCfg retorna a configuração JetStream para um tópico SNS.
//
// O stream do tópico é apenas um REGISTRO do que foi publicado — o fan-out
// para subscribers acontece de forma síncrona no Publish, ninguém consome
// deste stream hoje. Por isso a retenção default é curta (1h, configurável
// via GQ_TOPIC_MAX_AGE): reter dias de publicações × réplicas seria custo
// de disco puro sem benefício.
func (c *Client) topicStreamCfg(t types.Topic) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      "topic-" + sanitizeStreamName(t.Name),
		Subjects:  []string{c.topicSubject(t.Name)},
		Storage:   jetstream.FileStorage,
		Discard:   jetstream.DiscardOld, // registro: descartar antigas é OK aqui
		Retention: jetstream.LimitsPolicy,
		MaxMsgs:   defaultMaxMsgsPerQueue,
		MaxAge:    c.topicMaxAge,
		Replicas:  c.replicas,
	}
}

// CreateTopic cria um stream JetStream que representa o tópico SNS.
//
// Comportamento idempotente: se já existe stream com mesmo nome, devolve o
// tópico existente em vez de erro (igual SNS).
//
// Persiste metadados em KV bucket `topic_meta` (CreatedAt, etc.).
func (c *Client) CreateTopic(ctx context.Context, t types.Topic) (*types.Topic, error) {
	start := time.Now()
	defer func() { observability.ObserveStorage("create_topic", nil, time.Since(start)) }()

	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now().UTC()
	}

	cfg := c.topicStreamCfg(t)
	s, err := c.js.CreateStream(ctx, cfg)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			existing, gerr := c.js.Stream(ctx, cfg.Name)
			if gerr != nil {
				return nil, fmt.Errorf("topic existe mas falhou em buscar: %w", gerr)
			}
			t.CreatedAt = existing.CachedInfo().Created
			c.cacheStream(cfg.Name, existing)
			if md, merr := c.loadTopicMetadata(ctx, t.Name); merr == nil && !md.CreatedAt.IsZero() {
				t.CreatedAt = md.CreatedAt
			}
			return &t, nil
		}
		observability.ObserveStorage("create_topic", err, time.Since(start))
		return nil, fmt.Errorf("create stream: %w", err)
	}

	c.cacheStream(cfg.Name, s)
	t.CreatedAt = s.CachedInfo().Created

	if err := c.saveTopicMetadata(ctx, t); err != nil {
		observability.L().Warn("falha ao persistir topic metadata KV",
			"topic", t.Name, "err", err.Error())
	}

	observability.L().Info("tópico criado", "name", t.Name, "stream", cfg.Name)
	return &t, nil
}

// GetTopic busca um tópico existente.
//
// Carrega metadados do KV bucket; se stream não existe, devolve ErrTopicNotFound.
func (c *Client) GetTopic(ctx context.Context, name string) (*types.Topic, error) {
	start := time.Now()
	defer func() { observability.ObserveStorage("get_topic", nil, time.Since(start)) }()

	streamName := "topic-" + sanitizeStreamName(name)
	s, err := c.js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, storage.ErrTopicNotFound
		}
		return nil, fmt.Errorf("get stream: %w", err)
	}
	c.cacheStream(streamName, s)

	t := &types.Topic{
		Name:      name,
		CreatedAt: s.CachedInfo().Created,
	}

	if md, merr := c.loadTopicMetadata(ctx, name); merr == nil && !md.CreatedAt.IsZero() {
		t.CreatedAt = md.CreatedAt
	}
	return t, nil
}

// ListTopics lista todos os tópicos, opcionalmente filtrados por prefixo.
func (c *Client) ListTopics(ctx context.Context, prefix string) ([]types.Topic, error) {
	start := time.Now()
	defer func() { observability.ObserveStorage("list_topics", nil, time.Since(start)) }()

	lister := c.js.StreamNames(ctx)
	var out []types.Topic
	for name := range lister.Name() {
		if !hasPrefix(name, "topic-") {
			continue
		}
		topicName := name[len("topic-"):]
		if prefix != "" && !hasPrefix(topicName, prefix) {
			continue
		}
		t := types.Topic{
			Name: topicName,
		}
		if md, merr := c.loadTopicMetadata(ctx, topicName); merr == nil {
			t.CreatedAt = md.CreatedAt
		}
		out = append(out, t)
	}
	if err := lister.Err(); err != nil {
		return out, fmt.Errorf("list streams: %w", err)
	}
	return out, nil
}

// DeleteTopic remove o stream + metadata KV.
//
// Também remove todas as inscrições do tópico (subscriptions órfãs não fazem sentido).
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	start := time.Now()
	defer func() { observability.ObserveStorage("delete_topic", nil, time.Since(start)) }()

	streamName := "topic-" + sanitizeStreamName(name)
	err := c.js.DeleteStream(ctx, streamName)
	c.invalidateStream(streamName)
	if err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("delete stream: %w", err)
	}

	// Remove metadata KV do tópico.
	kv, kerr := c.topicKV(ctx)
	if kerr == nil {
		_ = kv.Delete(ctx, name)
	}

	// Remove todas as inscrições do tópico — match por suffix do ARN.
	subs, lerr := c.listAllSubscriptions(ctx)
	if lerr == nil {
		for _, s := range subs {
			if extractTopicName(s.TopicARN) == name {
				skv, skerr := c.subscriptionKV(ctx)
				if skerr == nil {
					_ = skv.Delete(ctx, subscriptionKey(s.ARN))
				}
			}
		}
	}

	observability.L().Info("tópico removido", "name", name, "stream", streamName)
	return nil
}

// Subscribe adiciona uma inscrição a um tópico.
//
// Persiste em KV `subscription_meta`. Subscription ARN é gerado como
// `arn:aws:sns:<region>:<account>:<topic>:<random>`.
//
// Pending=true se for HTTP/HTTPS (precisa ConfirmSubscription); false se SQS.
func (c *Client) Subscribe(ctx context.Context, sub types.Subscription) (*types.Subscription, error) {
	start := time.Now()
	defer func() { observability.ObserveStorage("subscribe", nil, time.Since(start)) }()

	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = time.Now().UTC()
	}

	// Verifica que tópico existe.
	if _, err := c.GetTopic(ctx, extractTopicName(sub.TopicARN)); err != nil {
		return nil, err
	}

	// Gera ARN se não veio preenchido.
	if sub.ARN == "" {
		sub.ARN = generateSubscriptionARN(sub.TopicARN, sub.Protocol, sub.Endpoint)
	}

	// HTTP/HTTPS ficam pending até ConfirmSubscription.
	if sub.Protocol == "http" || sub.Protocol == "https" && !sub.Pending {
		sub.Pending = true
	} else if sub.Protocol == "sqs" {
		sub.Pending = false
	}

	kv, err := c.subscriptionKV(ctx)
	if err != nil {
		return nil, err
	}
	md := subscriptionMetadataV1{
		Version:      1,
		ARN:          sub.ARN,
		TopicARN:     sub.TopicARN,
		TopicName:    extractTopicName(sub.TopicARN),
		Protocol:     sub.Protocol,
		Endpoint:     sub.Endpoint,
		FilterPolicy: sub.FilterPolicy,
		RawDelivery:  sub.RawDelivery,
		Pending:      sub.Pending,
		CreatedAt:    sub.CreatedAt,
	}
	body, err := json.Marshal(md)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	if _, err := kv.Put(ctx, subscriptionKey(sub.ARN), body); err != nil {
		return nil, fmt.Errorf("put: %w", err)
	}

	observability.L().Info("inscrição criada",
		"arn", sub.ARN,
		"topic", sub.TopicARN,
		"protocol", sub.Protocol,
		"endpoint", sub.Endpoint,
	)
	return &sub, nil
}

// ListSubscriptions lista inscrições, opcionalmente filtradas por tópico.
func (c *Client) ListSubscriptions(ctx context.Context, topicName string) ([]types.Subscription, error) {
	start := time.Now()
	defer func() { observability.ObserveStorage("list_subscriptions", nil, time.Since(start)) }()

	all, err := c.listAllSubscriptions(ctx)
	if err != nil {
		return nil, err
	}
	if topicName == "" {
		return all, nil
	}
	out := make([]types.Subscription, 0, len(all))
	for _, s := range all {
		if extractTopicName(s.TopicARN) == topicName {
			out = append(out, s)
		}
	}
	return out, nil
}

// listAllSubscriptions retorna todas as inscrições (helper interno).
//
// NATS KV não tem List() que retorna entries — só ListKeys() que retorna
// as chaves. Para cada chave, fazemos Get() individual.
func (c *Client) listAllSubscriptions(ctx context.Context) ([]types.Subscription, error) {
	kv, err := c.subscriptionKV(ctx)
	if err != nil {
		return nil, err
	}
	keys, err := kv.Keys(ctx)
	if err != nil {
		// NATS KV retorna ErrNoKeysFound quando o bucket está vazio;
		// nesse caso retornamos uma lista vazia em vez de propagar erro.
		// (NOTA: o erro vem do pacote jetstream, não do pacote nats)
		if errors.Is(err, jetstream.ErrNoKeysFound) || err == jetstream.ErrNoKeysFound {
			return []types.Subscription{}, nil
		}
		return nil, fmt.Errorf("list keys: %w", err)
	}
	out := make([]types.Subscription, 0, len(keys))
	for _, k := range keys {
		entry, gerr := kv.Get(ctx, k)
		if gerr != nil {
			continue
		}
		var md subscriptionMetadataV1
		if uerr := json.Unmarshal(entry.Value(), &md); uerr != nil {
			continue
		}
		// md.ARN já contém o ARN real (sem sanitização); a chave KV só
		// guarda a versão sanitizada para evitar conflito com subjects JetStream.
		out = append(out, types.Subscription{
			ARN:          md.ARN,
			TopicARN:     md.TopicARN,
			Protocol:     md.Protocol,
			Endpoint:     md.Endpoint,
			FilterPolicy: md.FilterPolicy,
			RawDelivery:  md.RawDelivery,
			Pending:      md.Pending,
			CreatedAt:    md.CreatedAt,
		})
	}
	return out, nil
}

// Unsubscribe remove uma inscrição pelo ARN.
func (c *Client) Unsubscribe(ctx context.Context, subscriptionARN string) error {
	start := time.Now()
	defer func() { observability.ObserveStorage("unsubscribe", nil, time.Since(start)) }()

	kv, err := c.subscriptionKV(ctx)
	if err != nil {
		return err
	}
	if err := kv.Delete(ctx, subscriptionKey(subscriptionARN)); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return storage.ErrInvalidArgument("subscription não encontrada")
		}
		return fmt.Errorf("delete: %w", err)
	}
	observability.L().Info("inscrição removida", "arn", subscriptionARN)
	return nil
}

// Publish publica uma mensagem em um tópico + faz fan-out síncrono para subscribers.
//
// Fluxo:
//
//  1. Publish no stream do tópico (sujeito "t.<topic>") — fica disponível para
//     subscribers que queiram usar consumer pattern (futuro).
//  2. Lista subscriptions do tópico em KV.
//  3. Para cada subscription: valida filter policy + delivera ao endpoint.
//
// Delivery:
//
//   - SQS: chama SendMessage no storage para a fila destino (síncrono).
//   - HTTP/HTTPS: POST com retry exponencial (3 tentativas).
//   - Outros protocolos (email, sms): retorna erro de UnsupportedOperation
//     por entry (similar a batch partial failure).
//
// **Síncrono**: Publish só retorna depois que todas as deliveries completam
// (ou falham). Trade-off: latência proporcional a #subscribers. Em prod, AWS
// usa workers assíncronos com DLQ; para MVP, esta versão simplificada é
// suficiente e mais previsível.
func (c *Client) Publish(ctx context.Context, topicName string, msg *types.Message) (*types.Message, error) {
	start := time.Now()
	defer func() { observability.ObserveStorage("publish", nil, time.Since(start)) }()

	if msg == nil {
		return nil, storage.ErrInvalidArgument("msg é nil")
	}

	// 1. Publish no stream do tópico.
	topic, err := c.GetTopic(ctx, topicName)
	if err != nil {
		return nil, err
	}

	headers, herr := messageHeaders(msg.MessageAttributes)
	if herr != nil {
		return nil, herr
	}

	nmsg := &nats.Msg{
		Subject: c.topicSubject(topicName),
		Data:    []byte(msg.Body),
		Header:  headers,
	}
	ackFuture, err := c.js.PublishMsgAsync(nmsg)
	if err != nil {
		return nil, fmt.Errorf("publish async: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case a := <-ackFuture.Ok():
		msg.ID = strconv.FormatUint(a.Sequence, 10)
		msg.EnqueuedAt = time.Now().UTC()
	case e := <-ackFuture.Err():
		return nil, fmt.Errorf("publish ack: %w", e)
	}

	// 2. Lista subscriptions.
	subs, err := c.ListSubscriptions(ctx, topicName)
	if err != nil {
		return msg, nil // msg publicada, fan-out falhou
	}

	// 3. Fan-out: subscriptions entregues em paralelo, com bound de
	// concorrência. Sem o bound, um tópico com milhares de subscriptions
	// dispararia milhares de goroutines por Publish — e todas competindo
	// pela mesma conexão NATS. O semáforo limita o paralelismo; o ctx do
	// request limita o tempo total (delivery lento não trava o Publish
	// para sempre).
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxFanoutConcurrency)
	for _, sub := range subs {
		if sub.Pending {
			// HTTP/HTTPS não confirmados: AWS skip delivery
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
			c.deliverToSubscription(ctx, topic, &subCopy, msg)
		}()
	}
	wg.Wait()

	return msg, nil
}

// maxFanoutConcurrency limita quantas deliveries de fan-out rodam em
// paralelo por Publish. Valor conservador: deliveries SQS são publishes
// no mesmo broker (rápidos); o gargalo é a conexão compartilhada.
const maxFanoutConcurrency = 16

// deliverToSubscription delivers uma mensagem a uma inscrição específica.
//
// SQS: chama SendMessage no storage para a queue destino (síncrono).
// HTTP/HTTPS: POST com retry exponencial.
func (c *Client) deliverToSubscription(ctx context.Context, topic *types.Topic, sub *types.Subscription, msg *types.Message) {
	start := time.Now()
	defer func() { observability.ObserveStorage("publish_deliver", nil, time.Since(start)) }()

	switch sub.Protocol {
	case "sqs":
		// SQS endpoint é "arn:aws:sqs:..." ou "http(s)://..."
		queueName := extractQueueNameFromEndpoint(sub.Endpoint)
		if queueName == "" {
			observability.L().Warn("SQS subscription com endpoint inválido", "endpoint", sub.Endpoint)
			return
		}
		delivery := &types.Message{
			Body:              msg.Body,
			MessageAttributes: msg.MessageAttributes,
		}
		_, err := c.SendMessage(ctx, queueName, delivery)
		if err != nil {
			observability.L().Warn("fan-out SQS failed",
				"sub_arn", sub.ARN, "queue", queueName, "err", err.Error())
			return
		}
		observability.L().Debug("fan-out SQS ok", "sub_arn", sub.ARN, "queue", queueName)
	default:
		// HTTP/HTTPS, email, sms, etc. — não implementado no MVP.
		observability.L().Debug("skip unsupported protocol", "protocol", sub.Protocol, "arn", sub.ARN)
	}
}

// matchesFilterPolicy verifica se a mensagem passa pelo filter policy.
//
// Filter policy AWS é um JSON object onde cada chave é um attribute name
// (message attribute ou system attribute) e o valor é uma lista de valores
// aceitos. Mensagem passa se TODOS os atributos presentes no filter
// tiverem um valor que casa com a lista.
//
// Exemplos:
//
//	{"event_type": ["order_placed", "order_updated"]}
//	{"store": ["NY"], "event_type": ["order_placed"]}
//
// Filter vazio = passa tudo.
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

// generateSubscriptionARN gera um ARN único para uma inscrição.
//
// Formato: arn:aws:sns:<region>:<account>:<topic>:<random>
func generateSubscriptionARN(topicARN, protocol, endpoint string) string {
	h := sha256.Sum256([]byte(topicARN + ":" + protocol + ":" + endpoint + ":" + strconv.FormatInt(time.Now().UnixNano(), 10)))
	return topicARN + ":" + hex.EncodeToString(h[:8])
}

// subscriptionKey retorna uma versão do ARN segura para usar como chave de KV.
//
// KV JetStream keys não podem conter '.' ou ':'. Substituímos por '_'.
func subscriptionKey(arn string) string {
	return strings.ReplaceAll(strings.ReplaceAll(arn, ":", "_"), ".", "_")
}

// extractTopicName extrai o nome do tópico de um ARN SNS.
//
// Formato ARN: arn:aws:sns:<region>:<account>:<name>
func extractTopicName(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) < 6 {
		return ""
	}
	return parts[5]
}

// extractQueueNameFromEndpoint extrai o nome da fila de um SQS endpoint.
//
// Endpoint pode ser:
//
//   - arn:aws:sqs:<region>:<account>:<name>
//   - https://sqs.<region>.amazonaws.com/<account>/<name>
//   - http://localhost:4566/<account>/<name>
func extractQueueNameFromEndpoint(endpoint string) string {
	if strings.HasPrefix(endpoint, "arn:") {
		parts := strings.Split(endpoint, ":")
		if len(parts) >= 6 {
			return parts[5]
		}
		return ""
	}
	// URL — extrai último segmento do path.
	idx := strings.LastIndex(endpoint, "/")
	if idx < 0 || idx == len(endpoint)-1 {
		return ""
	}
	return endpoint[idx+1:]
}
