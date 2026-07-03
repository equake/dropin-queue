// Package nats implementa o adapter Storage usando NATS JetStream.
//
// Mapeamento:
//
//	Fila SQS Standard  →  JetStream Stream com subjects "q.<name>.>"
//	Fila SQS FIFO      →  JetStream Stream com subjects particionados por group
//	Tópico SNS         →  JetStream Stream com subject "t.<name>"
//	Subscriber SQS     →  Consumer durável que republica no stream da queue
//	Subscriber HTTP    →  Consumer + worker entrega POST
//
// Esta é a única camada que conhece NATS. Acima dela, tudo fala AWS protocol.
package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/storage"
)

const (
	// queueSubjectPrefix é o prefixo de subject usado para filas SQS.
	// Subjects JetStream: "q.<queueName>.<messageGroupId>".
	queueSubjectPrefix = "q."

	// topicSubjectPrefix é o prefixo de subject usado para tópicos SNS.
	// "t.<topicName>".
	topicSubjectPrefix = "t."

	// queueStreamPrefix é o prefixo para nomes de stream JetStream de filas.
	// ATENÇÃO: mudar este formato invalida TODAS as filas existentes
	// em disco — o nome do stream é o índice primário no broker. O comentário
	// em queueConsumerName (messages.go) já avisa sobre o mesmo risco
	// para nomes de consumer; aqui estende ao nome do stream.
	queueStreamPrefix = "queue-"

	// topicStreamPrefix é o prefixo para nomes de stream JetStream de
	// tópicos SNS. Mesmo cuidado de migration: rename = data loss.
	topicStreamPrefix = "topic-"
)

// queueStreamName retorna o nome do stream JetStream de uma fila.
//
// Refactor/kiss-dry-pass-1 — antes era "queue-"+sanitizeStreamName(name)
// espalhado em 8 sites (queues.go, metadata.go, messages.go). Centralizado
// aqui para que futuras mudanças de formato sejam atômicas.
func queueStreamName(queueName string) string {
	return queueStreamPrefix + sanitizeStreamName(queueName)
}

// topicStreamName retorna o nome do stream JetStream de um tópico SNS.
//
// Mesmo padrão de queueStreamName; antes era "topic-"+sanitizeStreamName(name)
// em 3 sites (topics.go).
func topicStreamName(topicName string) string {
	return topicStreamPrefix + sanitizeStreamName(topicName)
}

// Client encapsula a conexão com NATS JetStream e expõe a interface Storage.
//
// Uma instância é segura para uso concorrente — JetStream context é goroutine-safe.
//
// O Client NÃO guarda estado por mensagem: receipt handles carregam o reply
// subject do JetStream (ver messages.go), então qualquer réplica do shim
// consegue ack/nak qualquer mensagem. Isso permite escalar horizontalmente
// (N réplicas atrás de um LB) sem sticky sessions.
type Client struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	mu      sync.RWMutex
	streams map[string]jetstream.Stream // cache de streams para evitar lookups repetidos
	prefix  string                      // prefixo adicional para multi-tenant (futuro)
	kvCache *lazyKVCache                // cache do bucket de metadados (lazy init thread-safe)

	// consumers é o cache de consumers duráveis por fila (chave: consumer
	// name). Evita CreateOrUpdateConsumer (1 RTT ao broker) a cada
	// ReceiveMessage no caminho quente. ackWait registra o AckWait com que
	// o consumer foi criado/atualizado — se um ReceiveMessage pedir
	// VisibilityTimeout diferente, o consumer é atualizado (caminho raro).
	consumers map[string]cachedConsumer

	// replicas é o número de réplicas Raft por stream (1 dev, 3 prod).
	// Vem de config — nunca inferido.
	replicas int

	// maxAckPending limita mensagens in-flight por fila.
	maxAckPending int

	// topicMaxAge é a retenção do stream de arquivo dos tópicos SNS.
	topicMaxAge time.Duration

	// topicKVCache é o KV bucket de metadados dos tópicos (lazy init thread-safe).
	topicKVCache *lazyKVCache
	// subscriptionKVCache é o KV bucket de inscrições (lazy init thread-safe).
	subscriptionKVCache *lazyKVCache
}

// cachedConsumer é uma entrada do cache de consumers duráveis.
type cachedConsumer struct {
	consumer jetstream.Consumer
	ackWait  time.Duration
}

// Options configura a conexão NATS.
type Options struct {
	// URL: "nats://host:4222" ou "tls://host:4222".
	URL string

	// CredentialsFile: caminho do arquivo .creds para autenticação JWT.
	CredentialsFile string

	// CACert: certificado CA para validar TLS do broker.
	CACert string

	// Name: nome do cliente (aparece em /connectionsz).
	Name string

	// Prefix: prefixo extra para subjects (multi-tenant).
	Prefix string

	// StreamReplicas: réplicas Raft por stream (1 dev, 3 prod).
	// Se 0, assume 1.
	StreamReplicas int

	// MaxAckPending: máximo de mensagens in-flight por fila.
	// Se 0, assume 1000.
	MaxAckPending int

	// TopicMaxAge: retenção do stream de arquivo dos tópicos SNS.
	// Se 0, assume 1 hora.
	TopicMaxAge time.Duration
}

// Connect estabelece conexão com NATS JetStream.
//
// Retorna erro se não conseguir conectar dentro de 10 segundos.
// O caller deve chamar Close() no shutdown.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	if opts.URL == "" {
		return nil, errors.New("nats URL vazio")
	}

	natsOpts := []nats.Option{
		nats.Name(opts.Name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.ReconnectJitter(500*time.Millisecond, 2*time.Second),
		nats.Timeout(5 * time.Second),
		nats.PingInterval(20 * time.Second),
		nats.MaxPingsOutstanding(3),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			observability.L().Warn("nats async error",
				"subject", sub.Subject,
				"err", err.Error(),
			)
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				observability.L().Warn("nats disconnected", "err", err.Error())
			}
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			observability.L().Info("nats reconnected")
		}),
	}

	if opts.CredentialsFile != "" {
		natsOpts = append(natsOpts, nats.UserCredentials(opts.CredentialsFile))
	}
	if opts.CACert != "" {
		natsOpts = append(natsOpts, nats.RootCAs(opts.CACert))
	}

	nc, err := nats.Connect(opts.URL, natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	// Espera JetStream estar disponível.
	if _, err := nc.JetStream(); err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream context: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream new: %w", err)
	}

	replicas := opts.StreamReplicas
	if replicas == 0 {
		replicas = 1
	}
	maxAckPending := opts.MaxAckPending
	if maxAckPending == 0 {
		maxAckPending = 1000
	}
	topicMaxAge := opts.TopicMaxAge
	if topicMaxAge == 0 {
		topicMaxAge = time.Hour
	}

	c := &Client{
		nc:                  nc,
		js:                  js,
		streams:             make(map[string]jetstream.Stream),
		prefix:              opts.Prefix,
		kvCache:             &lazyKVCache{},
		topicKVCache:        &lazyKVCache{},
		subscriptionKVCache: &lazyKVCache{},
		consumers:           make(map[string]cachedConsumer),
		replicas:            replicas,
		maxAckPending:       maxAckPending,
		topicMaxAge:         topicMaxAge,
	}

	// Valida que o account tem JetStream habilitado.
	if _, err := js.AccountInfo(ctx); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("jetstream account info: %w", err)
	}

	observability.L().Info("conectado ao NATS JetStream",
		"url", opts.URL,
		"client_name", opts.Name,
	)
	return c, nil
}

// Close fecha a conexão NATS.
func (c *Client) Close() error {
	if c.nc == nil {
		return nil
	}
	c.nc.Close()
	observability.L().Info("conexão NATS fechada")
	return nil
}

// rawSubject retorna o subject com prefixo (se houver) aplicado.
func (c *Client) rawSubject(s string) string {
	if c.prefix == "" {
		return s
	}
	return c.prefix + "." + s
}

// queueSubject retorna o subject JetStream para uma fila.
func (c *Client) queueSubject(queueName string) string {
	return c.rawSubject(queueSubjectPrefix + queueName)
}

// topicSubject retorna o subject JetStream para um tópico.
func (c *Client) topicSubject(topicName string) string {
	return c.rawSubject(topicSubjectPrefix + topicName)
}

// cacheStream adiciona stream ao cache.
func (c *Client) cacheStream(name string, s jetstream.Stream) {
	c.mu.Lock()
	c.streams[name] = s
	c.mu.Unlock()
}

// invalidateStream remove do cache (chamado após DeleteStream etc.).
func (c *Client) invalidateStream(name string) {
	c.mu.Lock()
	delete(c.streams, name)
	c.mu.Unlock()
}

// cachedQueueConsumer devolve o consumer cacheado se o AckWait bater.
func (c *Client) cachedQueueConsumer(name string, ackWait time.Duration) (jetstream.Consumer, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.consumers[name]
	if !ok || entry.ackWait != ackWait {
		return nil, false
	}
	return entry.consumer, true
}

// cacheQueueConsumer registra o consumer no cache.
func (c *Client) cacheQueueConsumer(name string, consumer jetstream.Consumer, ackWait time.Duration) {
	c.mu.Lock()
	c.consumers[name] = cachedConsumer{consumer: consumer, ackWait: ackWait}
	c.mu.Unlock()
}

// invalidateConsumer remove o consumer do cache (DeleteQueue,
// SetQueueAttributes com novo VisibilityTimeout, etc.).
func (c *Client) invalidateConsumer(name string) {
	c.mu.Lock()
	delete(c.consumers, name)
	c.mu.Unlock()
}

// Ping verifica que o broker está acessível e com JetStream habilitado.
//
// Custo O(1) — usado pelo readiness probe. NÃO usar ListQueues aqui:
// listagem de streams é O(n) e o probe roda a cada poucos segundos em
// cada réplica.
func (c *Client) Ping(ctx context.Context) error {
	if c.nc == nil || !c.nc.IsConnected() {
		return storage.ErrBrokerUnavailable
	}
	if _, err := c.js.AccountInfo(ctx); err != nil {
		return fmt.Errorf("jetstream account info: %w", err)
	}
	return nil
}
