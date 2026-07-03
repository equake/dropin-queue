package nats

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/anomalyco/generic_queue/shim/internal/observability"
	"github.com/anomalyco/generic_queue/shim/internal/storage"
	"github.com/anomalyco/generic_queue/shim/pkg/types"
)

// subjectForQueue retorna o subject JetStream para publicação em uma fila.
//
// Para filas Standard (não-FIFO) usamos um subject fixo "default" como
// agrupador lógico — todas as mensagens competem entre todos os consumers
// do pull consumer.
//
// Para filas FIFO (sufixo .fifo), particionamos por MessageGroupId. Sem
// groupId, caímos em "default" e geramos erro de validação antes de chegar
// aqui (validação fica em service layer).
//
// O consumer durável se inscreve no subject wildcard da fila, então o
// fan-out entre groups acontece naturalmente via subjects distintos.
func (c *Client) subjectForQueue(queueName string, msg *types.Message) string {
	group := "default"
	if msg != nil && msg.MessageGroupId != "" {
		group = sanitizeSubjectToken(msg.MessageGroupId)
	}
	return c.queueSubject(queueName) + "." + group
}

// sanitizeSubjectToken remove caracteres não permitidos em subject tokens.
//
// JetStream aceita [A-Za-z0-9-_.] em subject tokens (separados por '.').
// Para groupId com outros chars, substituímos por '_'.
func sanitizeSubjectToken(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch >= 'A' && ch <= 'Z',
			ch >= 'a' && ch <= 'z',
			ch >= '0' && ch <= '9',
			ch == '-', ch == '_', ch == '.':
			out = append(out, ch)
		default:
			out = append(out, '_')
		}
	}
	if len(out) > 200 {
		out = out[:200]
	}
	return string(out)
}

// messageHeaders converte MessageAttributes SQS em headers NATS.
//
// Header "X-Sqs-Atr-<name>" → value
// Header "X-Sqs-Atr-<name>-dt" → DataType (String|Number|Binary|String.List)
// Para Binary: value é base64.
// Para String.List: value começa com "list:" seguido de items joined por "|".
//
// Prefix X-Sqs-Atr- evita colisão com headers nativos NATS (Nats-Msg-Id).
// O sufixo -dt armazena o DataType para que o ReceiveMessage possa reconstruir
// o tipo original sem depender do nome do atributo.
func messageHeaders(attrs map[string]types.MessageAttribute) (nats.Header, error) {
	if len(attrs) == 0 {
		return nil, nil
	}
	h := make(nats.Header)
	for name, attr := range attrs {
		switch attr.DataType {
		case "String":
			h.Set("X-Sqs-Atr-"+name, attr.StringValue)
			h.Set("X-Sqs-Atr-"+name+"-dt", "String")
		case "Number":
			h.Set("X-Sqs-Atr-"+name, attr.StringValue)
			h.Set("X-Sqs-Atr-"+name+"-dt", "Number")
		case "String.List":
			h.Set("X-Sqs-Atr-"+name, "list:"+attr.StringValue)
			h.Set("X-Sqs-Atr-"+name+"-dt", "String.List")
		case "Binary":
			h.Set("X-Sqs-Atr-"+name, base64.StdEncoding.EncodeToString(attr.BinaryValue))
			h.Set("X-Sqs-Atr-"+name+"-dt", "Binary")
		default:
			return nil, fmt.Errorf("MessageAttribute %q com DataType não suportado: %q", name, attr.DataType)
		}
	}
	return h, nil
}

// SendMessage publica uma mensagem em uma fila.
//
// Fluxo:
//
//  1. Valida tamanho (MaximumMessageSize do atributo da fila).
//  2. Constrói subject: q.<queue>.<groupId|default>.
//  3. Constrói headers com MessageAttributes + Nats-Msg-Id (dedup FIFO).
//  4. js.PublishAsync (não-bloqueante) → espera ack síncrono.
//  5. Calcula MD5 do body → retorna.
//
// Idempotência FIFO: Nats-Msg-Id deduplica em janela de 2 minutos (default).
// Para idempotência Standard, o cliente controla com MessageDeduplicationId.
func (c *Client) SendMessage(ctx context.Context, queueName string, msg *types.Message) (*types.Message, error) {
	start := time.Now()
	defer func() { observability.ObserveStorage("send_message", nil, time.Since(start)) }()

	if msg == nil {
		return nil, storage.ErrInvalidArgument("msg é nil")
	}
	if msg.Body == "" {
		return nil, storage.ErrInvalidArgument("body vazio")
	}

	q, err := c.GetQueue(ctx, queueName)
	if err != nil {
		observability.ObserveStorage("send_message", err, time.Since(start))
		return nil, err
	}

	maxSize := int(q.Attributes.MaximumMessageSize)
	if maxSize == 0 {
		maxSize = 262144
	}
	if len(msg.Body) > maxSize {
		observability.ObserveStorage("send_message",
			fmt.Errorf("body %d bytes > max %d", len(msg.Body), maxSize), time.Since(start))
		return nil, storage.ErrMessageTooLarge(len(msg.Body), maxSize)
	}

	subject := c.subjectForQueue(queueName, msg)

	headers, herr := messageHeaders(msg.MessageAttributes)
	if herr != nil {
		observability.ObserveStorage("send_message", herr, time.Since(start))
		return nil, herr
	}

	publishOpts := []jetstream.PublishOpt{}
	if msg.MessageDeduplicationId != "" {
		publishOpts = append(publishOpts, jetstream.WithMsgID(msg.MessageDeduplicationId))
	}

	// PublishMsgAsync aceita *nats.Msg — permite setar headers nativamente.
	nmsg := &nats.Msg{
		Subject: subject,
		Data:    []byte(msg.Body),
		Header:  headers,
	}
	ackFuture, err := c.js.PublishMsgAsync(nmsg, publishOpts...)
	if err != nil {
		observability.ObserveStorage("send_message", err, time.Since(start))
		return nil, fmt.Errorf("publish async: %w", err)
	}

	if msg.Attributes == nil {
		msg.Attributes = make(map[string]string)
	}

	select {
	case <-ctx.Done():
		observability.ObserveStorage("send_message", ctx.Err(), time.Since(start))
		return nil, ctx.Err()
	case a := <-ackFuture.Ok():
		msg.ID = strconv.FormatUint(a.Sequence, 10)
		msg.EnqueuedAt = time.Now().UTC()
		if a.Duplicate {
			msg.Attributes["Duplicate"] = "true"
		}
	case e := <-ackFuture.Err():
		// FIFO dedup: mensagem duplicada, retornamos sucesso com ID derivado.
		if strings.Contains(strings.ToLower(e.Error()), "duplicate") {
			msg.ID = "dedup-" + msg.MessageDeduplicationId
			msg.EnqueuedAt = time.Now().UTC()
			msg.Attributes = map[string]string{
				"SentTimestamp": fmt.Sprintf("%d", msg.EnqueuedAt.UnixMilli()),
				"Duplicate":     "true",
			}
			return msg, nil
		}
		observability.ObserveStorage("send_message", e, time.Since(start))
		return nil, fmt.Errorf("publish ack: %w", e)
	}

	sum := md5.Sum([]byte(msg.Body))
	msg.MD5OfBody = hex.EncodeToString(sum[:])

	msg.Attributes["SentTimestamp"] = fmt.Sprintf("%d", msg.EnqueuedAt.UnixMilli())
	msg.Attributes["ApproximateReceiveCount"] = "0"

	observability.ObserveStorage("send_message", nil, time.Since(start))
	return msg, nil
}

// ReceiveMessage consome até max mensagens da fila com long-polling.
//
// Mapeamento NATS ↔ SQS:
//
//	WaitTimeSeconds        → FetchMaxWait (long-poll nativo)
//	VisibilityTimeout      → AckWait do consumer (visibility timeout nativo)
//	MaxNumberOfMessages    → batch size
//
// **Implementação**: usa um único consumer DURÁVEL por fila
// (nome "shim-consumer-<queueName>"). Esse consumer tem o mesmo comportamento
// de uma subscription persistente: ele acompanha sua posição no stream e
// só entrega mensagens não-entregues. Quando você deleta uma mensagem
// (ack), o consumer avança — a próxima ReceiveMessage NÃO vê essa mensagem.
//
// Trade-off vs SQS:
//
//   - SQS: cada ReceiveMessage é uma operação independente que retorna
//     qualquer mensagem disponível na fila (não importa quantas vezes
//     foi recebida antes, contanto que visibility timeout tenha expirado).
//   - Aqui: o consumer durável tem uma posição; após ack, a mensagem é
//     removida permanentemente. Isso é MAIS próximo de uma fila FIFO real
//     do que SQS Standard (que pode entregar duplicatas).
//
// Cada mensagem devolvida tem ReceiptHandle codificando (consumerName, seq).
//
// Se waitSeconds for 0 e não houver mensagens, retorna imediatamente
// com slice vazio. Se waitSeconds > 0, bloqueia até esse tempo aguardando
// mensagens novas (JetStream Fetch nativo).
func (c *Client) ReceiveMessage(
	ctx context.Context,
	queueName string,
	maxMessages int32,
	waitSeconds int32,
	visibilityTimeout int32,
) ([]types.Message, error) {
	start := time.Now()
	defer func() {
		observability.ObserveStorage("receive_message", nil, time.Since(start))
		observability.ObserveLongPollDuration(queueName, time.Since(start))
	}()

	if maxMessages < 1 {
		maxMessages = 1
	}
	if maxMessages > 10 {
		maxMessages = 10
	}

	q, err := c.GetQueue(ctx, queueName)
	if err != nil {
		observability.ObserveStorage("receive_message", err, time.Since(start))
		return nil, err
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

	streamName := "queue-" + sanitizeStreamName(queueName)
	stream, err := c.js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, storage.ErrQueueNotFound
		}
		observability.ObserveStorage("receive_message", err, time.Since(start))
		return nil, fmt.Errorf("get stream: %w", err)
	}

	observability.IncLongPoll(queueName)
	defer observability.DecLongPoll(queueName)

	// Consumer DURÁVEL nomeado por fila. Persiste entre chamadas
	// ReceiveMessage. Sem isso, cada chamada cria novo consumer que
	// vê TODAS as mensagens do stream.
	consumerName := "shim-consumer-" + sanitizeStreamName(queueName)
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:           consumerName,
		Durable:        consumerName,
		Description:    "generic_queue shim receive consumer",
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxAckPending:  1000,
		AckWait:        time.Duration(visibilityTimeout) * time.Second,
		FilterSubjects: []string{c.queueSubject(queueName) + ".>"},
		// InactiveThreshold NÃO é setado — consumer é persistente.
	})
	if err != nil {
		observability.ObserveStorage("receive_message", err, time.Since(start))
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	fetchOpts := []jetstream.FetchOpt{}
	if waitSeconds > 0 {
		fetchOpts = append(fetchOpts, jetstream.FetchMaxWait(time.Duration(waitSeconds)*time.Second))
	}
	mset, err := consumer.Fetch(int(maxMessages), fetchOpts...)
	if err != nil {
		observability.ObserveStorage("receive_message", err, time.Since(start))
		return nil, fmt.Errorf("fetch: %w", err)
	}

	out := make([]types.Message, 0, maxMessages)
	msgRefs := make([]jetstream.Msg, 0, maxMessages)

	shortPoll := waitSeconds == 0
	shortPollTimeout := 100 * time.Millisecond

	for {
		var deadline <-chan time.Time
		if shortPoll {
			deadline = time.After(shortPollTimeout)
		}
		select {
		case <-ctx.Done():
			c.storePending(consumerName, msgRefs)
			return out, nil
		case msg, ok := <-mset.Messages():
			if !ok {
				c.storePending(consumerName, msgRefs)
				if err := mset.Error(); err != nil && len(out) == 0 {
					return nil, fmt.Errorf("fetch error: %w", err)
				}
				return out, nil
			}
			parsed := parseJetStreamMsg(msg, consumerName)
			out = append(out, parsed)
			msgRefs = append(msgRefs, msg)
			if int32(len(out)) >= maxMessages {
				c.storePending(consumerName, msgRefs)
				return out, nil
			}
			if shortPoll {
				continue
			}
		case <-deadline:
			if shortPoll {
				c.storePending(consumerName, msgRefs)
				return out, nil
			}
			c.storePending(consumerName, msgRefs)
			return out, nil
		}
	}
}

// storePending guarda as jetstream.Msg em cache indexado por
// (consumerName, sequence) para uso posterior em DeleteMessage /
// ChangeMessageVisibility.
func (c *Client) storePending(consumerName string, msgs []jetstream.Msg) {
	if len(msgs) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.pendingMsgs[consumerName]; !ok {
		c.pendingMsgs[consumerName] = make(map[uint64]jetstream.Msg)
	}
	for _, m := range msgs {
		meta, _ := m.Metadata()
		if meta != nil {
			c.pendingMsgs[consumerName][meta.Sequence.Stream] = m
		}
	}
}

// parseJetStreamMsg converte jetstream.Msg em types.Message.
//
// Extrai body, headers (X-Sqs-Atr-* → MessageAttributes), metadata.
// ReceiptHandle é setado externamente (precisa do consumer name).
func parseJetStreamMsg(msg jetstream.Msg, consumerName string) types.Message {
	out := types.Message{
		Body:       string(msg.Data()),
		EnqueuedAt: time.Now().UTC(),
		Attributes: map[string]string{
			"ApproximateReceiveCount": "1",
		},
		MessageAttributes: make(map[string]types.MessageAttribute),
	}

	meta, _ := msg.Metadata()
	if meta != nil {
		out.ID = strconv.FormatUint(meta.Sequence.Stream, 10)
		if !meta.Timestamp.IsZero() {
			out.EnqueuedAt = meta.Timestamp
		}
		out.Attributes["SentTimestamp"] = fmt.Sprintf("%d", out.EnqueuedAt.UnixMilli())
	}

	sum := md5.Sum(msg.Data())
	out.MD5OfBody = hex.EncodeToString(sum[:])

	if hdr := msg.Headers(); hdr != nil {
		// Itera headers X-Sqs-Atr-* (excluindo -dt que armazena o DataType).
		for k, vs := range hdr {
			if !strings.HasPrefix(k, "X-Sqs-Atr-") || len(vs) == 0 {
				continue
			}
			if strings.HasSuffix(k, "-dt") {
				continue
			}
			name := strings.TrimPrefix(k, "X-Sqs-Atr-")
			raw := vs[0]
			dt := "String" // default
			if dtvs, ok := hdr["X-Sqs-Atr-"+name+"-dt"]; ok && len(dtvs) > 0 {
				dt = dtvs[0]
			}

			switch dt {
			case "String.List":
				out.MessageAttributes[name] = types.MessageAttribute{
					DataType:    "String.List",
					StringValue: strings.TrimPrefix(raw, "list:"),
				}
			case "Binary":
				decoded, err := base64.StdEncoding.DecodeString(raw)
				if err == nil {
					out.MessageAttributes[name] = types.MessageAttribute{
						DataType:    "Binary",
						BinaryValue: decoded,
					}
				}
			default:
				// String ou Number
				out.MessageAttributes[name] = types.MessageAttribute{
					DataType:    dt,
					StringValue: raw,
				}
			}
		}
	}

	out.ReceiptHandle = encodeReceiptHandle(consumerName, meta.Sequence.Stream)
	return out
}

// encodeReceiptHandle codifica um receipt handle opaco para o cliente.
//
// Formato: "rh1:<consumerName>:<streamSeq>" — versionado para permitir
// evolução futura.
//
// O consumerName identifica o consumer que detém a mensagem (necessário
// para ack/nak). O streamSeq é a sequence dentro do stream (única na fila).
func encodeReceiptHandle(consumerName string, streamSeq uint64) string {
	return fmt.Sprintf("rh1:%s:%d", consumerName, streamSeq)
}

// decodeReceiptHandle extrai consumerName e streamSeq do receipt handle.
//
// Devolve erro se formato inválido (receipt handle expirado ou corrompido).
func decodeReceiptHandle(rh string) (consumerName string, streamSeq uint64, err error) {
	parts := strings.Split(rh, ":")
	if len(parts) != 3 || parts[0] != "rh1" {
		return "", 0, fmt.Errorf("receipt handle inválido: %q", rh)
	}
	seq, perr := strconv.ParseUint(parts[2], 10, 64)
	if perr != nil {
		return "", 0, fmt.Errorf("receipt handle com sequence inválida: %q", parts[2])
	}
	return parts[1], seq, nil
}

// fetchAndAck busca uma mensagem específica por streamSeq em um consumer
// e aplica uma ação (ack/nak). Usado por DeleteMessage e ChangeMessageVisibility.
//
// **Implementação alternativa baseada em cache local** — em vez de confiar
// em NATS para retornar pending messages em um segundo Fetch (comportamento
// que observamos ser flaky), mantemos um cache em memória de mensagens
// recebidas indexado por (consumerName, sequence). Isso simplifica
// enormemente a lógica e elimina a dependência de comportamento interno
// do JetStream que pode mudar entre versões.
//
// Trade-off: cache cresce até InactiveThreshold=60s limpar o consumer;
// cleanup passivo via TTL no map evita leak.
//
// Retorna erros tipados (storage.ErrInvalidReceiptHandleT) diretamente
// sem wrapping para que errors.As funcione na camada service.
func (c *Client) fetchAndAck(
	ctx context.Context,
	queueName, consumerName string,
	targetSeq uint64,
	ackFunc func(jetstream.Msg) error,
) error {
	c.mu.Lock()
	cache, ok := c.pendingMsgs[consumerName]
	c.mu.Unlock()
	if !ok {
		// Consumer efêmero expirou ou nunca existiu — receipt expirado.
		return storage.ErrInvalidReceiptHandle("consumer não existe (receipt expirado)")
	}

	c.mu.Lock()
	msgRef, ok := cache[targetSeq]
	c.mu.Unlock()
	if !ok {
		return storage.ErrInvalidReceiptHandle(fmt.Sprintf("sequence %d não encontrada", targetSeq))
	}

	if err := ackFunc(msgRef); err != nil {
		if !errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
			return fmt.Errorf("ack action: %w", err)
		}
	}

	// Remove do cache após sucesso.
	c.mu.Lock()
	delete(c.pendingMsgs[consumerName], targetSeq)
	c.mu.Unlock()

	return nil
}

// DeleteMessage remove (ack) uma mensagem recebida usando o receipt handle.
//
// Comportamento AWS:
//
//   - Idempotente: deletar duas vezes é OK.
//   - Se visibility timeout expirou: ReceiptHandleIsInvalid.
//   - Se fila não existe: QueueDoesNotExist.
func (c *Client) DeleteMessage(ctx context.Context, queueName string, receiptHandle string) error {
	start := time.Now()
	defer func() { observability.ObserveStorage("delete_message", nil, time.Since(start)) }()

	if receiptHandle == "" {
		return storage.ErrInvalidArgument("receipt handle vazio")
	}
	consumerName, streamSeq, err := decodeReceiptHandle(receiptHandle)
	if err != nil {
		return storage.ErrInvalidReceiptHandle(err.Error())
	}

	err = c.fetchAndAck(ctx, queueName, consumerName, streamSeq, func(m jetstream.Msg) error {
		return m.Ack()
	})
	if err != nil {
		observability.ObserveStorage("delete_message", err, time.Since(start))
		return err
	}
	observability.ObserveStorage("delete_message", nil, time.Since(start))
	return nil
}

// ChangeMessageVisibility redefine o tempo de visibilidade de uma mensagem.
//
// Comportamento AWS:
//
//   - Estende ou reduz: NakWithDelay(visibilityTimeout) faz a mensagem
//     voltar visível após o delay.
//   - Se receipt handle inválido: ReceiptHandleIsInvalid.
func (c *Client) ChangeMessageVisibility(
	ctx context.Context,
	queueName string,
	receiptHandle string,
	visibilityTimeout int32,
) error {
	start := time.Now()
	defer func() { observability.ObserveStorage("change_visibility", nil, time.Since(start)) }()

	if receiptHandle == "" {
		return storage.ErrInvalidArgument("receipt handle vazio")
	}
	if visibilityTimeout < 0 || visibilityTimeout > 43200 {
		return storage.ErrInvalidArgument("visibilityTimeout fora do range [0, 43200]")
	}
	consumerName, streamSeq, err := decodeReceiptHandle(receiptHandle)
	if err != nil {
		return storage.ErrInvalidReceiptHandle(err.Error())
	}

	err = c.fetchAndAck(ctx, queueName, consumerName, streamSeq, func(m jetstream.Msg) error {
		return m.NakWithDelay(time.Duration(visibilityTimeout) * time.Second)
	})
	if err != nil {
		observability.ObserveStorage("change_visibility", err, time.Since(start))
		return err
	}
	observability.ObserveStorage("change_visibility", nil, time.Since(start))
	return nil
}

// QueueDepth devolve o número aproximado de mensagens disponíveis.
//
// Lê StreamInfo.State.Msgs — aproximado, igual ao SQS.
func (c *Client) QueueDepth(ctx context.Context, queueName string) (int64, error) {
	start := time.Now()
	defer func() { observability.ObserveStorage("queue_depth", nil, time.Since(start)) }()

	streamName := "queue-" + sanitizeStreamName(queueName)
	stream, err := c.js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return 0, storage.ErrQueueNotFound
		}
		return 0, fmt.Errorf("get stream: %w", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("stream info: %w", err)
	}

	depth := int64(info.State.Msgs)
	observability.SetQueueDepth(queueName, float64(depth))
	return depth, nil
}

// PurgeQueueStorage é wrapper para PurgeQueue em MessageStorage interface.
// A implementação real está em queues.go.
func (c *Client) PurgeQueueStorage(ctx context.Context, queueName string) error {
	return c.PurgeQueue(ctx, queueName)
}
