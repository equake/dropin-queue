package postgres

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/storage"
	"github.com/equake/dropin-queue/shim/pkg/types"
)

// dedupWindow é a janela de deduplicação FIFO — mesma janela documentada
// para o header Nats-Msg-Id no adapter NATS.
const dedupWindow = 5 * time.Minute

// encodeReceiptHandle codifica um receipt handle opaco para o cliente.
//
// Formato: "pg1:<id>:<claim_token>" — versionado para permitir evolução
// futura, mesmo padrão do "rh2:" do adapter NATS. Como o claim_token vive
// na tabela messages (não em memória do shim), qualquer réplica processa
// ack/nak — é isso que torna o shim stateless também neste backend.
func encodeReceiptHandle(id int64, claimToken string) string {
	return fmt.Sprintf("pg1:%d:%s", id, claimToken)
}

// decodeReceiptHandle extrai id + claim_token do receipt handle.
func decodeReceiptHandle(rh string) (id int64, claimToken string, err error) {
	parts := strings.SplitN(rh, ":", 3)
	if len(parts) != 3 || parts[0] != "pg1" {
		return 0, "", fmt.Errorf("receipt handle inválido: %q", rh)
	}
	id, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("receipt handle inválido: %q", rh)
	}
	if parts[2] == "" {
		return 0, "", fmt.Errorf("receipt handle inválido: %q", rh)
	}
	return id, parts[2], nil
}

// SendMessage insere a mensagem e dispara (com coalescing) o NOTIFY que
// acorda long-polls esperando por esta fila.
func (c *Client) SendMessage(ctx context.Context, queueName string, msg *types.Message) (result *types.Message, err error) {
	defer observability.StartObserve("send_message").Done(&err)

	if msg == nil {
		return nil, storage.ErrInvalidArgument("msg é nil")
	}
	if msg.Body == "" {
		return nil, storage.ErrInvalidArgument("body vazio")
	}

	q, gerr := c.GetQueue(ctx, queueName)
	if gerr != nil {
		return nil, gerr
	}

	maxSize := int(q.Attributes.MaximumMessageSize)
	if maxSize == 0 {
		maxSize = 262144
	}
	if len(msg.Body) > maxSize {
		return nil, storage.ErrMessageTooLarge(len(msg.Body), maxSize)
	}

	qid, rerr := c.resolveQueue(ctx, queueName)
	if rerr != nil {
		return nil, rerr
	}

	group := "default"
	if q.FIFO && msg.MessageGroupId != "" {
		group = msg.MessageGroupId
	}

	dedupID := msg.MessageDeduplicationId
	if dedupID == "" && q.FIFO && q.Attributes.ContentBasedDeduplication {
		sum := sha256.Sum256([]byte(msg.Body))
		dedupID = hex.EncodeToString(sum[:])
	}

	attrsJSON, merr := json.Marshal(msg.MessageAttributes)
	if merr != nil {
		return nil, fmt.Errorf("marshal message attributes: %w", merr)
	}

	tx, terr := c.pool.Begin(ctx)
	if terr != nil {
		return nil, fmt.Errorf("begin tx: %w", terr)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op após Commit

	// Insere primeiro, sempre — se for duplicata, desfazemos com DELETE
	// abaixo. Mais simples que decidir antes de saber o id, e o custo
	// (um id de sequence "gasto" em duplicatas) é irrelevante.
	var id int64
	var enqueuedAt time.Time
	if ierr := tx.QueryRow(ctx, `
		INSERT INTO messages (queue_id, group_key, body, message_attributes)
		VALUES ($1, $2, $3, $4)
		RETURNING id, enqueued_at
	`, qid.id, group, msg.Body, attrsJSON).Scan(&id, &enqueuedAt); ierr != nil {
		return nil, fmt.Errorf("insert message: %w", ierr)
	}

	if dedupID != "" {
		tag, dErr := tx.Exec(ctx, `
			INSERT INTO message_dedup (queue_id, dedup_id, message_id, expires_at)
			VALUES ($1, $2, $3, now() + make_interval(secs => $4))
			ON CONFLICT (queue_id, dedup_id) DO NOTHING
		`, qid.id, dedupID, id, int(dedupWindow.Seconds()))
		if dErr != nil {
			return nil, fmt.Errorf("dedup insert: %w", dErr)
		}
		if tag.RowsAffected() == 0 {
			// Duplicata dentro da janela: descarta a linha que acabamos de
			// inserir e devolve o id da mensagem original — mesmo
			// comportamento do PubAck.Duplicate do JetStream, que reaproveita
			// o Sequence original em vez de gerar um novo MessageId.
			if _, derr := tx.Exec(ctx, `DELETE FROM messages WHERE id = $1`, id); derr != nil {
				return nil, fmt.Errorf("rollback speculative insert: %w", derr)
			}
			var winnerID int64
			if serr := tx.QueryRow(ctx, `
				SELECT message_id FROM message_dedup WHERE queue_id = $1 AND dedup_id = $2
			`, qid.id, dedupID).Scan(&winnerID); serr != nil {
				return nil, fmt.Errorf("lookup dedup winner: %w", serr)
			}
			if cerr := tx.Commit(ctx); cerr != nil {
				return nil, fmt.Errorf("commit dedup: %w", cerr)
			}
			now := time.Now().UTC()
			sum := md5.Sum([]byte(msg.Body))
			return &types.Message{
				ID:                strconv.FormatInt(winnerID, 10),
				Body:              msg.Body,
				MD5OfBody:         hex.EncodeToString(sum[:]),
				EnqueuedAt:        now,
				MessageAttributes: msg.MessageAttributes,
				Attributes: map[string]string{
					"SentTimestamp": fmt.Sprintf("%d", now.UnixMilli()),
					"Duplicate":     "true",
				},
			}, nil
		}
	}

	if cerr := tx.Commit(ctx); cerr != nil {
		return nil, fmt.Errorf("commit: %w", cerr)
	}

	c.coalescer.trigger(queueName)

	sum := md5.Sum([]byte(msg.Body))
	return &types.Message{
		ID:                strconv.FormatInt(id, 10),
		Body:              msg.Body,
		MD5OfBody:         hex.EncodeToString(sum[:]),
		EnqueuedAt:        enqueuedAt,
		MessageAttributes: msg.MessageAttributes,
		Attributes: map[string]string{
			"SentTimestamp":           fmt.Sprintf("%d", enqueuedAt.UnixMilli()),
			"ApproximateReceiveCount": "0",
		},
	}, nil
}

// ReceiveMessage consome até max mensagens da fila com long-polling.
//
// Mapeamento SQS ↔ Postgres:
//
//	WaitTimeSeconds     → loop de claim + espera por NOTIFY (com poll de
//	                      segurança) até esgotar o prazo
//	VisibilityTimeout   → locked_until/visible_at (equivalente ao AckWait)
//	MaxNumberOfMessages → batch size do claim
//
// Filas Standard usam SKIP LOCKED direto (claimStandard); filas FIFO
// passam pelo mutex de grupo em queue_groups (claimFIFO) para preservar
// ordering-within-group como o SQS FIFO real.
func (c *Client) ReceiveMessage(
	ctx context.Context,
	queueName string,
	maxMessages int32,
	waitSeconds int32,
	visibilityTimeout int32,
) (result []types.Message, err error) {
	rec := observability.StartObserve("receive_message")
	defer rec.Done(&err)
	defer func() { observability.ObserveLongPollDuration(queueName, time.Since(rec.Start())) }()

	if maxMessages < 1 {
		maxMessages = 1
	}
	if maxMessages > 10 {
		maxMessages = 10
	}

	q, gerr := c.GetQueue(ctx, queueName)
	if gerr != nil {
		return nil, gerr
	}
	if visibilityTimeout <= 0 {
		visibilityTimeout = q.Attributes.VisibilityTimeout
	}
	if visibilityTimeout == 0 {
		visibilityTimeout = 30
	}
	if waitSeconds < 0 {
		waitSeconds = 0
	}
	if waitSeconds > 20 {
		waitSeconds = 20
	}

	qid, rerr := c.resolveQueue(ctx, queueName)
	if rerr != nil {
		return nil, rerr
	}

	observability.IncLongPoll(queueName)
	defer observability.DecLongPoll(queueName)

	waitCtx := ctx
	if waitSeconds > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, time.Duration(waitSeconds)*time.Second)
		defer cancel()
	}

	for {
		msgs, cerr := c.claim(ctx, qid, maxMessages, visibilityTimeout)
		if cerr != nil {
			return nil, cerr
		}
		if len(msgs) > 0 || waitSeconds == 0 {
			return msgs, nil
		}

		ch, cancelSub := c.hub.subscribe(queueName)
		timer := time.NewTimer(c.pollInterval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			cancelSub()
			return msgs, nil
		case <-ch:
			timer.Stop()
			cancelSub()
		case <-timer.C:
			cancelSub()
		}
	}
}

// claim reivindica até max mensagens em ordem de chegada (SKIP LOCKED),
// sem restrição de grupo — igual para filas Standard e FIFO.
//
// Ordering-within-group (garantia do SQS FIFO) sai de graça aqui: como
// group_key nunca influencia a claim, mensagens do mesmo grupo são sempre
// devolvidas na ordem em que foram inseridas (ORDER BY id), exatamente
// como o adapter NATS entrega hoje — que também não impõe limite de "1
// mensagem in-flight por grupo" (JetStream distribui livremente entre
// pulls concorrentes; a ordem relativa dentro do grupo é preservada
// porque nunca é violada na escrita, só na leitura simultânea por
// consumers DIFERENTES, o que nenhum dos dois backends restringe hoje).
func (c *Client) claim(ctx context.Context, q queueIdent, maxMessages, visibilityTimeout int32) ([]types.Message, error) {
	// UPDATE ... RETURNING não herda o ORDER BY da subquery de seleção —
	// o resultado pode vir em ordem física do heap (que MVCC embaralha a
	// cada UPDATE), não na ordem de chegada. A CTE + SELECT externo com
	// ORDER BY explícito é o que garante ordering-within-group do FIFO
	// (e ordem "razoável" em filas Standard).
	rows, err := c.pool.Query(ctx, `
		WITH claimed AS (
			UPDATE messages SET
				locked_until = now() + make_interval(secs => $3),
				visible_at = now() + make_interval(secs => $3),
				delivery_count = delivery_count + 1,
				claim_token = gen_random_uuid()
			WHERE id IN (
				SELECT id FROM messages
				WHERE queue_id = $1 AND visible_at <= now()
				ORDER BY id
				FOR UPDATE SKIP LOCKED
				LIMIT $2
			)
			RETURNING id, body, message_attributes, enqueued_at, delivery_count, claim_token::text
		)
		SELECT id, body, message_attributes, enqueued_at, delivery_count, claim_token
		FROM claimed
		ORDER BY id
	`, q.id, maxMessages, visibilityTimeout)
	if err != nil {
		return nil, fmt.Errorf("claim messages: %w", err)
	}
	defer rows.Close()

	var out []types.Message
	for rows.Next() {
		var id int64
		var body string
		var attrsJSON []byte
		var enqueuedAt time.Time
		var deliveryCount int32
		var claimToken string
		if serr := rows.Scan(&id, &body, &attrsJSON, &enqueuedAt, &deliveryCount, &claimToken); serr != nil {
			return nil, fmt.Errorf("scan claimed message: %w", serr)
		}
		out = append(out, buildMessage(id, body, attrsJSON, enqueuedAt, deliveryCount, claimToken))
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("claim rows: %w", rows.Err())
	}
	return out, nil
}

// buildMessage monta o types.Message a partir de uma linha reivindicada.
func buildMessage(id int64, body string, attrsJSON []byte, enqueuedAt time.Time, deliveryCount int32, claimToken string) types.Message {
	attrs := make(map[string]types.MessageAttribute)
	_ = json.Unmarshal(attrsJSON, &attrs)

	sum := md5.Sum([]byte(body))
	return types.Message{
		ID:                strconv.FormatInt(id, 10),
		ReceiptHandle:     encodeReceiptHandle(id, claimToken),
		Body:              body,
		MD5OfBody:         hex.EncodeToString(sum[:]),
		MessageAttributes: attrs,
		EnqueuedAt:        enqueuedAt,
		Attributes: map[string]string{
			"SentTimestamp":           fmt.Sprintf("%d", enqueuedAt.UnixMilli()),
			"ApproximateReceiveCount": strconv.FormatInt(int64(deliveryCount), 10),
		},
	}
}

// DeleteMessage remove (ack) uma mensagem usando o receipt handle.
//
// Comportamento AWS: idempotente (deletar 2x é OK); se o claim_token não
// bate mais (já deletada, ou visibility expirou e outro consumer já
// reivindicou), trata como no-op silencioso — mesmo tratamento fire-and-
// forget do publishAck no adapter NATS.
func (c *Client) DeleteMessage(ctx context.Context, queueName string, receiptHandle string) (err error) {
	defer observability.StartObserve("delete_message").Done(&err)

	if receiptHandle == "" {
		return storage.ErrInvalidArgument("receipt handle vazio")
	}
	id, token, derr := decodeReceiptHandle(receiptHandle)
	if derr != nil {
		return storage.ErrInvalidReceiptHandle(derr.Error())
	}
	q, rerr := c.resolveQueue(ctx, queueName)
	if rerr != nil {
		return rerr
	}

	_, qerr := c.pool.Exec(ctx, `
		DELETE FROM messages WHERE id = $1 AND claim_token = $2::uuid AND queue_id = $3
	`, id, token, q.id)
	if qerr != nil {
		return fmt.Errorf("delete message: %w", qerr)
	}
	return nil
}

// ChangeMessageVisibility redefine o tempo de visibilidade de uma mensagem.
func (c *Client) ChangeMessageVisibility(
	ctx context.Context,
	queueName string,
	receiptHandle string,
	visibilityTimeout int32,
) (err error) {
	defer observability.StartObserve("change_visibility").Done(&err)

	if receiptHandle == "" {
		return storage.ErrInvalidArgument("receipt handle vazio")
	}
	if visibilityTimeout < 0 || visibilityTimeout > 43200 {
		return storage.ErrInvalidArgument("visibilityTimeout fora do range [0, 43200]")
	}
	id, token, derr := decodeReceiptHandle(receiptHandle)
	if derr != nil {
		return storage.ErrInvalidReceiptHandle(derr.Error())
	}
	q, rerr := c.resolveQueue(ctx, queueName)
	if rerr != nil {
		return rerr
	}

	_, qerr := c.pool.Exec(ctx, `
		UPDATE messages SET
			visible_at = now() + make_interval(secs => $4),
			locked_until = now() + make_interval(secs => $4)
		WHERE id = $1 AND claim_token = $2::uuid AND queue_id = $3
	`, id, token, q.id, visibilityTimeout)
	if qerr != nil {
		return fmt.Errorf("change visibility: %w", qerr)
	}
	return nil
}

// QueueDepth devolve o número de mensagens na fila (visíveis + in-flight),
// próximo do ApproximateNumberOfMessages do SQS — mesma aproximação que o
// State.Msgs do adapter NATS já usa.
func (c *Client) QueueDepth(ctx context.Context, queueName string) (result int64, err error) {
	defer observability.StartObserve("queue_depth").Done(&err)

	q, rerr := c.resolveQueue(ctx, queueName)
	if rerr != nil {
		return 0, rerr
	}
	var depth int64
	if qerr := c.pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE queue_id = $1`, q.id).Scan(&depth); qerr != nil {
		return 0, fmt.Errorf("queue depth: %w", qerr)
	}
	observability.SetQueueDepth(queueName, float64(depth))
	return depth, nil
}
