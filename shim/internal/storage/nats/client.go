// Package nats implementa o adapter Storage usando NATS JetStream.
//
// Mapeamento:
//
//   Fila SQS Standard  →  JetStream Stream com subjects "q.<name>.>"
//   Fila SQS FIFO      →  JetStream Stream com subjects particionados por group
//   Tópico SNS         →  JetStream Stream com subject "t.<name>"
//   Subscriber SQS     →  Consumer durável que republica no stream da queue
//   Subscriber HTTP    →  Consumer + worker entrega POST
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

	"github.com/anomalyco/generic_queue/shim/internal/observability"
)

const (
	// queueSubjectPrefix é o prefixo de subject usado para filas SQS.
	// Subjects JetStream: "q.<queueName>.<messageGroupId>".
	queueSubjectPrefix = "q."

	// topicSubjectPrefix é o prefixo de subject usado para tópicos SNS.
	// "t.<topicName>".
	topicSubjectPrefix = "t."

	// consumerPrefix é o nome usado para consumers duráveis.
	// "shim-consumer-<queueName>" para queues, "shim-sub-<topic>-<endpoint>" para subscribers.
)

// Client encapsula a conexão com NATS JetStream e expõe a interface Storage.
//
// Uma instância é segura para uso concorrente — JetStream context é goroutine-safe.
type Client struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	mu       sync.RWMutex
	streams  map[string]jetstream.Stream // cache de streams para evitar lookups repetidos
	prefix   string                     // prefixo adicional para multi-tenant (futuro)
	kvCache  jetstream.KeyValue         // cache do bucket de metadados (lazy init)

	// pendingMsgs é um cache de mensagens recebidas mas não confirmadas.
	// Chave externa: consumerName (efêmero, criado por ReceiveMessage).
	// Chave interna: stream sequence.
	// Valor: jetstream.Msg com métodos Ack/Nak.
	//
	// Necessário porque Fetch no mesmo consumer após o primeiro não retorna
	// necessariamente pending messages (depende de estado interno do servidor).
	// Cache local elimina essa dependência.
	//
	// Cleanup: entry é removida após ack/nak. Consumer efêmero é removido
	// pelo servidor após InactiveThreshold=60s; nesse ponto a entry no map
	// também deixa de ser referenciada.
	pendingMsgs map[string]map[uint64]jetstream.Msg
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

	c := &Client{
		nc:          nc,
		js:          js,
		streams:     make(map[string]jetstream.Stream),
		prefix:      opts.Prefix,
		pendingMsgs: make(map[string]map[uint64]jetstream.Msg),
	}

	// Valida que o account tem JetStream habilitado.
	if _, err := js.AccountInfo(ctx); err != nil {
		c.Close()
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

// getStreamCached retorna um stream do cache ou busca no broker.
// Usado para evitar StreamInfo() repetido em hot paths.
func (c *Client) getStreamCached(ctx context.Context, name string) (jetstream.Stream, bool) {
	c.mu.RLock()
	s, ok := c.streams[name]
	c.mu.RUnlock()
	if ok {
		return s, true
	}
	return nil, false
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
