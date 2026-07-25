// Package postgres implementa o adapter Storage usando Postgres como
// backend: tabelas relacionais para filas/tópicos/mensagens, claim via
// `SELECT ... FOR UPDATE SKIP LOCKED` e long-polling via LISTEN/NOTIFY
// com fallback de poll.
//
// Mapeamento (paralelo ao storage/nats):
//
//	Fila SQS Standard  →  linhas na tabela messages, claim sem restrição de grupo
//	Fila SQS FIFO      →  messages.group_key preserva ordem relativa (ORDER BY id)
//	Tópico SNS         →  linha na tabela topics; fan-out síncrono no Publish
//	Visibility timeout →  coluna visible_at (equivalente ao AckWait)
//	Long-polling        →  LISTEN/NOTIFY (canal fixo) + poll de segurança
//
// Trocável via config.Backend (GQ_BACKEND=postgres) — nunca simultâneo
// com o adapter NATS. Esta é a única camada que conhece SQL; acima dela,
// tudo fala storage.Storage.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/storage"
)

// notifyChannel é o canal fixo do LISTEN/NOTIFY usado para acordar
// long-polls. Payload = nome da fila. Canal único (não um por fila) para
// que uma réplica do shim precise de exatamente 1 conexão de LISTEN,
// nunca 1 por long-poll HTTP em voo — é esse padrão que evita o teto de
// poucas conexões concorrentes que motivou o design (ver docs/architecture.md).
const notifyChannel = "dropin_queue_msg"

// Options configura a conexão Postgres.
type Options struct {
	// DSN: connection string (postgres://user:pass@host:5432/db).
	DSN string

	// MaxConns: tamanho máximo do pool (sem contar a conexão dedicada de
	// LISTEN). Se 0, assume 20.
	MaxConns int32

	// PollInterval: poll de segurança do long-poll — rede de segurança
	// contra NOTIFY perdido (crash durante coalescing, reconexão do
	// listener, etc.). Se 0, assume 300ms.
	PollInterval time.Duration

	// NotifyCoalesce: intervalo mínimo entre NOTIFYs emitidos para a
	// mesma fila. Evita 1 pg_notify() por SendMessage sob alto throughput
	// (ver comentário em client.go:notifyCoalescer).
	NotifyCoalesce time.Duration
}

// queueIdent é o identificador interno cacheado de uma fila (id + fifo).
// Imutáveis após CreateQueue — cache nunca precisa invalidar por update de
// atributos, só por DeleteQueue.
type queueIdent struct {
	id   int64
	fifo bool
}

// Client encapsula o pool de conexões Postgres e expõe a interface Storage.
//
// Uma instância é segura para uso concorrente. Como os receipt handles
// carregam id+claim_token (sem estado local do shim), qualquer réplica
// processa ack/nak de qualquer mensagem — mesma garantia stateless do
// adapter NATS.
type Client struct {
	pool *pgxpool.Pool

	listenConn *pgx.Conn
	cancelBg   context.CancelFunc
	bgWG       sync.WaitGroup

	hub       *notifyHub
	coalescer *notifyCoalescer

	pollInterval time.Duration

	mu         sync.RWMutex
	queueCache map[string]queueIdent
	topicCache map[string]int64
}

// Connect estabelece o pool de conexões, aplica o schema (idempotente) e
// sobe o listener de NOTIFY.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	if opts.DSN == "" {
		return nil, errors.New("postgres DSN vazio")
	}

	maxConns := opts.MaxConns
	if maxConns == 0 {
		maxConns = 20
	}
	pollInterval := opts.PollInterval
	if pollInterval == 0 {
		pollInterval = 300 * time.Millisecond
	}
	notifyCoalesce := opts.NotifyCoalesce
	if notifyCoalesce == 0 {
		notifyCoalesce = 20 * time.Millisecond
	}

	poolCfg, err := pgxpool.ParseConfig(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	poolCfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}

	if _, err := pool.Exec(ctx, schemaDDL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	// Conexão dedicada para LISTEN — fora do pool, para nunca ser
	// reciclada/reutilizada com uma sessão em estado de LISTEN ativo.
	listenConn, err := pgx.Connect(ctx, opts.DSN)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres listen connect: %w", err)
	}
	if _, err := listenConn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		_ = listenConn.Close(ctx)
		pool.Close()
		return nil, fmt.Errorf("listen: %w", err)
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	c := &Client{
		pool:         pool,
		listenConn:   listenConn,
		cancelBg:     cancel,
		hub:          newNotifyHub(),
		pollInterval: pollInterval,
		queueCache:   make(map[string]queueIdent),
		topicCache:   make(map[string]int64),
	}
	c.coalescer = newNotifyCoalescer(notifyCoalesce, func(queue string) {
		// Fire-and-forget: melhor esforço. Se falhar, o poll de segurança
		// do ReceiveMessage cobre a perda (ver messages.go).
		if _, err := pool.Exec(context.Background(), "SELECT pg_notify($1, $2)", notifyChannel, queue); err != nil {
			observability.L().Warn("postgres notify falhou (poll de segurança cobre)", "queue", queue, "err", err.Error())
		}
	})

	c.bgWG.Add(2)
	go func() { defer c.bgWG.Done(); c.listenLoop(bgCtx) }()
	go func() { defer c.bgWG.Done(); c.reapLoop(bgCtx) }()

	observability.L().Info("conectado ao Postgres", "max_conns", maxConns)
	return c, nil
}

// listenLoop bloqueia recebendo notificações e repassa para o hub em
// memória, que acorda os ReceiveMessage em long-poll esperando por
// aquela fila. Reconecta automaticamente se a conexão cair.
func (c *Client) listenLoop(ctx context.Context) {
	for {
		notif, err := c.listenConn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			observability.L().Warn("postgres listen: notificação perdida (poll de segurança cobre)", "err", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		c.hub.notify(notif.Payload)
	}
}

// reapInterval é o intervalo do reaper de limpeza (dedup expirado +
// mensagens além da retenção). Não é exposto via config: é GC puramente
// operacional, não afeta corretude — o upsert condicional em
// message_dedup (ver messages.go:SendMessage) já garante que um dedup_id
// expirado pode ser reaproveitado na hora, mesmo que o reaper não tenha
// rodado ainda. O reaper só existe para não deixar as tabelas crescerem
// sem limite.
const reapInterval = time.Minute

// reapLoop roda a limpeza periódica em background. Cada réplica do shim
// roda seu próprio reaper — sem eleição de líder por simplicidade; DELETE
// concorrente de múltiplas réplicas é seguro (só redundante ocasionalmente).
func (c *Client) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// context.Background() de propósito, não ctx: um reap em voo
			// no momento do shutdown (Close → cancelBg) termina os 2
			// DELETEs rápidos em vez de ser abortado no meio — mais
			// seguro que interromper uma limpeza pela metade, e o custo
			// é irrelevante (bgWG.Wait() em Close já espera essa
			// goroutine terminar de qualquer forma).
			c.reapOnce(context.Background())
		}
	}
}

// reapOnce remove entradas de dedup expiradas e mensagens além da
// MessageRetentionPeriod da fila — equivalente ao MaxAge nativo do
// JetStream no adapter NATS, que aqui precisa ser explícito.
func (c *Client) reapOnce(ctx context.Context) {
	if tag, err := c.pool.Exec(ctx, `DELETE FROM message_dedup WHERE expires_at < now()`); err != nil {
		observability.L().Warn("reaper: limpeza de message_dedup falhou", "err", err.Error())
	} else if n := tag.RowsAffected(); n > 0 {
		observability.L().Debug("reaper: dedup expirado removido", "rows", n)
	}

	if tag, err := c.pool.Exec(ctx, `
		DELETE FROM messages m
		USING queues q
		WHERE m.queue_id = q.id
		  AND m.enqueued_at < now() - make_interval(secs => q.message_retention_period)
	`); err != nil {
		observability.L().Warn("reaper: limpeza de mensagens expiradas por retenção falhou", "err", err.Error())
	} else if n := tag.RowsAffected(); n > 0 {
		observability.L().Info("reaper: mensagens expiradas por retenção removidas", "rows", n)
	}
}

// Ping verifica que o pool está acessível. Custo O(1) — usado pelo
// readiness probe.
func (c *Client) Ping(ctx context.Context) error {
	if err := c.pool.Ping(ctx); err != nil {
		return storage.ErrBrokerUnavailable
	}
	return nil
}

// Close encerra o listener, o reaper e o pool de conexões.
func (c *Client) Close() error {
	c.cancelBg()
	c.bgWG.Wait()
	err := c.listenConn.Close(context.Background())
	c.pool.Close()
	observability.L().Info("conexão Postgres fechada")
	return err
}

// cachedQueue devolve o queueIdent cacheado, se houver.
func (c *Client) cachedQueue(name string) (queueIdent, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	q, ok := c.queueCache[name]
	return q, ok
}

func (c *Client) cacheQueue(name string, q queueIdent) {
	c.mu.Lock()
	c.queueCache[name] = q
	c.mu.Unlock()
}

func (c *Client) invalidateQueueCache(name string) {
	c.mu.Lock()
	delete(c.queueCache, name)
	c.mu.Unlock()
}

func (c *Client) cachedTopic(name string) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	id, ok := c.topicCache[name]
	return id, ok
}

func (c *Client) cacheTopic(name string, id int64) {
	c.mu.Lock()
	c.topicCache[name] = id
	c.mu.Unlock()
}

func (c *Client) invalidateTopicCache(name string) {
	c.mu.Lock()
	delete(c.topicCache, name)
	c.mu.Unlock()
}

// resolveQueue busca id+fifo da fila, usando o cache quando possível.
func (c *Client) resolveQueue(ctx context.Context, name string) (queueIdent, error) {
	if q, ok := c.cachedQueue(name); ok {
		return q, nil
	}
	var q queueIdent
	err := c.pool.QueryRow(ctx, `SELECT id, fifo FROM queues WHERE name = $1`, name).Scan(&q.id, &q.fifo)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return queueIdent{}, storage.ErrQueueNotFound
		}
		return queueIdent{}, fmt.Errorf("resolve queue: %w", err)
	}
	c.cacheQueue(name, q)
	return q, nil
}

// notifyHub multiplexa notificações de 1 conexão LISTEN para N waiters em
// memória (1 por ReceiveMessage em long-poll). Padrão "broadcast único":
// cada subscribe devolve um canal que fecha exatamente 1 vez.
type notifyHub struct {
	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}
}

func newNotifyHub() *notifyHub {
	return &notifyHub{subs: make(map[string]map[chan struct{}]struct{})}
}

// subscribe registra interesse em notificações da fila. O caller DEVE
// chamar a função de cancelamento devolvida quando parar de esperar
// (contexto cancelado, mensagem já encontrada via poll, etc.) para não
// vazar o canal do mapa.
func (h *notifyHub) subscribe(queue string) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	h.mu.Lock()
	if h.subs[queue] == nil {
		h.subs[queue] = make(map[chan struct{}]struct{})
	}
	h.subs[queue][ch] = struct{}{}
	h.mu.Unlock()

	cancel := func() {
		h.mu.Lock()
		if set, ok := h.subs[queue]; ok {
			delete(set, ch)
			if len(set) == 0 {
				delete(h.subs, queue)
			}
		}
		h.mu.Unlock()
	}
	return ch, cancel
}

// notify acorda todos os waiters da fila (broadcast único — fecha os
// canais e remove do mapa).
func (h *notifyHub) notify(queue string) {
	h.mu.Lock()
	set := h.subs[queue]
	delete(h.subs, queue)
	h.mu.Unlock()
	for ch := range set {
		close(ch)
	}
}

// notifyCoalescer emite NOTIFY com debounce leading-edge: a primeira
// chamada para uma fila dispara na hora (baixa latência no caso comum de
// escrita esparsa); chamadas seguintes dentro da janela `interval` são
// coalescidas em nenhum NOTIFY extra — o waiter já foi acordado e vai
// reclamar a nova mensagem no próximo SKIP LOCKED.
//
// Isso evita o teto de throughput que um NOTIFY por INSERT causa: o
// artigo que motivou este design (dbos.dev) mediu 2.9K writes/s com
// trigger por linha vs 60K writes/s bufferizando/batcheando notificações.
type notifyCoalescer struct {
	mu       sync.Mutex
	interval time.Duration
	pending  map[string]struct{}
	fire     func(queue string)
}

func newNotifyCoalescer(interval time.Duration, fire func(queue string)) *notifyCoalescer {
	return &notifyCoalescer{
		interval: interval,
		pending:  make(map[string]struct{}),
		fire:     fire,
	}
}

func (n *notifyCoalescer) trigger(queue string) {
	n.mu.Lock()
	if _, scheduled := n.pending[queue]; scheduled {
		n.mu.Unlock()
		return
	}
	n.pending[queue] = struct{}{}
	n.mu.Unlock()

	n.fire(queue)

	time.AfterFunc(n.interval, func() {
		n.mu.Lock()
		delete(n.pending, queue)
		n.mu.Unlock()
	})
}
