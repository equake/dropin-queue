package nats

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/storage"
	"github.com/equake/dropin-queue/shim/pkg/types"
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

	subject := c.subjectForQueue(queueName, msg)

	headers, herr := messageHeaders(msg.MessageAttributes)
	if herr != nil {
		return nil, herr
	}

	publishOpts := []jetstream.PublishOpt{}
	if msg.MessageDeduplicationId != "" {
		// Explicit dedup ID do cliente — tem prioridade.
		publishOpts = append(publishOpts, jetstream.WithMsgID(msg.MessageDeduplicationId))
	} else if q.Attributes.ContentBasedDeduplication && q.FIFO {
		// ContentBasedDeduplication em fila FIFO: AWS usa SHA-256 do body
		// como dedup ID (em janela de 5min). Sem isso, mensagens idênticas
		// dentro da janela não seriam deduplicadas.
		sum := sha256.Sum256([]byte(msg.Body))
		publishOpts = append(publishOpts, jetstream.WithMsgID(hex.EncodeToString(sum[:])))
	}

	// PublishMsgAsync aceita *nats.Msg — permite setar headers nativamente.
	nmsg := &nats.Msg{
		Subject: subject,
		Data:    []byte(msg.Body),
		Header:  headers,
	}
	ackFuture, perr := c.js.PublishMsgAsync(nmsg, publishOpts...)
	if perr != nil {
		return nil, fmt.Errorf("publish async: %w", perr)
	}

	if msg.Attributes == nil {
		msg.Attributes = make(map[string]string)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case a := <-ackFuture.Ok():
		msg.ID = strconv.FormatUint(a.Sequence, 10)
		msg.EnqueuedAt = time.Now().UTC()
		if a.Duplicate {
			msg.Attributes["Duplicate"] = "true"
		}
	case e := <-ackFuture.Err():
		// DiscardNew: fila cheia rejeita o publish. Erro tipado para a
		// camada service mapear para OverLimit (cliente faz backoff).
		if strings.Contains(strings.ToLower(e.Error()), "maximum messages exceeded") {
			return nil, storage.ErrQueueFull
		}
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
		return nil, fmt.Errorf("publish ack: %w", e)
	}

	sum := md5.Sum([]byte(msg.Body))
	msg.MD5OfBody = hex.EncodeToString(sum[:])

	msg.Attributes["SentTimestamp"] = fmt.Sprintf("%d", msg.EnqueuedAt.UnixMilli())
	msg.Attributes["ApproximateReceiveCount"] = "0"

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
// (nome "shim-consumer-<queueName>"), criado no CreateQueue e cacheado
// localmente — o caminho quente NÃO faz CreateOrUpdateConsumer (que custa
// 1 RTT ao broker por chamada). O consumer só é atualizado quando o
// request pede um VisibilityTimeout diferente do atual (caminho raro).
//
// Múltiplas réplicas do shim podem fazer Fetch no MESMO consumer durável
// concorrentemente — JetStream distribui as mensagens entre elas. Isso é
// o que permite escalar o shim horizontalmente.
//
// Cada mensagem devolvida tem ReceiptHandle carregando o reply subject
// do JetStream (ver encodeReceiptHandle) — o ack/nak funciona de qualquer
// réplica, sem estado local.
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

	observability.IncLongPoll(queueName)
	defer observability.DecLongPoll(queueName)

	consumer, cerr := c.ensureQueueConsumer(ctx, nil, queueName, visibilityTimeout)
	if cerr != nil {
		return nil, cerr
	}

	// FetchMaxWait é SEMPRE setado — inclusive no short-poll. Sem ele, o
	// pull request usa o default do servidor (30s) e fica pendente depois
	// que retornamos: a próxima mensagem da fila seria entregue a um fetch
	// que ninguém está lendo (invisível até AckWait expirar). O bug ficava
	// mascarado quando CreateOrUpdateConsumer rodava a cada receive
	// (cancelava os pulls antigos); com consumer cacheado ele aparecia.
	maxWait := shortPollMaxWait
	if waitSeconds > 0 {
		maxWait = time.Duration(waitSeconds) * time.Second
	}
	fetchOpts := []jetstream.FetchOpt{jetstream.FetchMaxWait(maxWait)}
	mset, ferr := consumer.Fetch(int(maxMessages), fetchOpts...)
	if ferr != nil {
		// Handle cacheado pode estar stale (fila deletada/recriada por
		// outra réplica). Invalida e tenta uma vez com consumer fresco.
		c.invalidateConsumer(queueConsumerName(queueName))
		consumer, cerr = c.ensureQueueConsumer(ctx, nil, queueName, visibilityTimeout)
		if cerr != nil {
			return nil, cerr
		}
		mset, ferr = consumer.Fetch(int(maxMessages), fetchOpts...)
		if ferr != nil {
			return nil, fmt.Errorf("fetch: %w", ferr)
		}
	}

	out := make([]types.Message, 0, maxMessages)

	// O canal fecha quando o fetch completa (batch cheio) ou MaxWait
	// expira NO SERVIDOR — não abandonamos o fetch com pull pendente.
	for {
		select {
		case <-ctx.Done():
			return out, nil
		case msg, ok := <-mset.Messages():
			if !ok {
				if merr := mset.Error(); merr != nil && len(out) == 0 &&
					!errors.Is(merr, nats.ErrTimeout) {
					return nil, fmt.Errorf("fetch error: %w", merr)
				}
				return out, nil
			}
			out = append(out, parseJetStreamMsg(msg))
			if int32(len(out)) >= maxMessages {
				return out, nil
			}
		}
	}
}

// shortPollMaxWait é o MaxWait do Fetch quando WaitTimeSeconds=0
// (short-poll SQS): tempo suficiente para o broker responder com as
// mensagens já disponíveis, curto o bastante para "retorna imediatamente".
const shortPollMaxWait = 100 * time.Millisecond

// queueConsumerName retorna o nome do consumer durável de uma fila.
//
// ATENÇÃO: mudar este formato invalida receipt handles em voo (eles
// carregam o reply subject, que embute o nome do consumer).
func queueConsumerName(queueName string) string {
	return "shim-consumer-" + sanitizeStreamName(queueName)
}

// ensureQueueConsumer devolve o consumer durável da fila.
//
// Caminho quente: cache local (zero RTTs). Caminho frio (primeira chamada
// nesta réplica, ou VisibilityTimeout diferente do atual): busca o stream
// e faz CreateOrUpdateConsumer.
//
// stream pode ser nil — será buscado se necessário (só no caminho frio).
//
// InactiveThreshold NÃO é setado — o consumer é persistente e acompanha
// a posição da fila; removê-lo perderia o estado de entrega.
func (c *Client) ensureQueueConsumer(
	ctx context.Context,
	stream jetstream.Stream,
	queueName string,
	visibilityTimeout int32,
) (jetstream.Consumer, error) {
	name := queueConsumerName(queueName)
	ackWait := time.Duration(visibilityTimeout) * time.Second

	if consumer, ok := c.cachedQueueConsumer(name, ackWait); ok {
		return consumer, nil
	}

	if stream == nil {
		streamName := queueStreamName(queueName)
		s, err := c.js.Stream(ctx, streamName)
		if err != nil {
			if errors.Is(err, jetstream.ErrStreamNotFound) {
				return nil, storage.ErrQueueNotFound
			}
			return nil, fmt.Errorf("get stream: %w", err)
		}
		stream = s
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:           name,
		Durable:        name,
		Description:    "dropin-queue shim receive consumer",
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxAckPending:  c.maxAckPending,
		AckWait:        ackWait,
		FilterSubjects: []string{c.queueSubject(queueName) + ".>"},
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}
	c.cacheQueueConsumer(name, consumer, ackWait)
	return consumer, nil
}

// parseJetStreamMsg converte jetstream.Msg em types.Message.
//
// Extrai body, headers (X-Sqs-Atr-* → MessageAttributes), metadata.
// ReceiptHandle carrega o reply subject do JetStream (stateless).
func parseJetStreamMsg(msg jetstream.Msg) types.Message {
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
		// NumDelivered do JetStream conta entregas reais (1 na primeira,
		// incrementa a cada redelivery) — igual ao ApproximateReceiveCount.
		if meta.NumDelivered > 0 {
			out.Attributes["ApproximateReceiveCount"] = strconv.FormatUint(meta.NumDelivered, 10)
		}
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

	out.ReceiptHandle = encodeReceiptHandle(msg.Reply())
	return out
}

// encodeReceiptHandle codifica um receipt handle opaco para o cliente.
//
// Formato: "rh2:<base64url(replySubject)>" — versionado para permitir
// evolução futura.
//
// O reply subject ($JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.
// <ts>.<pending>) é tudo que o JetStream precisa para ack/nak — publicar
// "+ACK"/"-NAK" nele confirma/devolve a mensagem. Como não depende de
// NENHUM estado local do shim, qualquer réplica processa o DeleteMessage,
// não importa qual recebeu a mensagem. É isso que torna o shim stateless.
func encodeReceiptHandle(replySubject string) string {
	return "rh2:" + base64.RawURLEncoding.EncodeToString([]byte(replySubject))
}

// decodeReceiptHandle extrai o reply subject do receipt handle.
//
// Devolve erro se formato inválido (receipt handle corrompido ou de
// versão antiga do shim).
func decodeReceiptHandle(rh string) (replySubject string, err error) {
	raw, ok := strings.CutPrefix(rh, "rh2:")
	if !ok {
		return "", fmt.Errorf("receipt handle inválido: %q", rh)
	}
	decoded, derr := base64.RawURLEncoding.DecodeString(raw)
	if derr != nil {
		return "", fmt.Errorf("receipt handle corrompido: %q", rh)
	}
	reply := string(decoded)
	if !strings.HasPrefix(reply, "$JS.ACK.") {
		return "", fmt.Errorf("receipt handle com reply subject inválido")
	}
	return reply, nil
}

// validateReceiptQueue confere que o receipt handle pertence à fila.
//
// O reply subject tem o stream no 3º token (formato v1, 9 tokens) ou no
// 5º (formato v2 com domain+account hash, 12 tokens). Rejeitar receipts
// de outra fila evita deletar mensagem errada por engano do cliente.
func validateReceiptQueue(replySubject, queueName string) error {
	tokens := strings.Split(replySubject, ".")
	var stream string
	switch {
	case len(tokens) == 9:
		stream = tokens[2]
	case len(tokens) >= 12:
		stream = tokens[4]
	default:
		return storage.ErrInvalidReceiptHandle("reply subject com formato desconhecido")
	}
	expected := queueStreamName(queueName)
	if stream != expected {
		return storage.ErrInvalidReceiptHandle(
			fmt.Sprintf("receipt handle pertence a outra fila (%s)", stream))
	}
	return nil
}

// publishAck publica um payload de ack ("+ACK"/"-NAK ...") no reply
// subject e faz flush para garantir que o broker recebeu.
//
// Mesmo mecanismo que jetstream.Msg.Ack() usa por baixo — mas sem
// precisar da jetstream.Msg original, então funciona de qualquer réplica.
// Ack repetido é ignorado pelo servidor (idempotente, igual SQS).
func (c *Client) publishAck(ctx context.Context, replySubject string, payload []byte) error {
	if err := c.nc.Publish(replySubject, payload); err != nil {
		return fmt.Errorf("publish ack: %w", err)
	}
	// FlushWithContext exige ctx com deadline; garante uma se não houver.
	flushCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		flushCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	if err := c.nc.FlushWithContext(flushCtx); err != nil {
		return fmt.Errorf("flush ack: %w", err)
	}
	return nil
}

// DeleteMessage remove (ack) uma mensagem recebida usando o receipt handle.
//
// Comportamento AWS:
//
//   - Idempotente: deletar duas vezes é OK.
//   - Se visibility timeout expirou: ReceiptHandleIsInvalid.
//   - Se fila não existe: QueueDoesNotExist.
func (c *Client) DeleteMessage(ctx context.Context, queueName string, receiptHandle string) (err error) {
	defer observability.StartObserve("delete_message").Done(&err)

	if receiptHandle == "" {
		return storage.ErrInvalidArgument("receipt handle vazio")
	}
	reply, derr := decodeReceiptHandle(receiptHandle)
	if derr != nil {
		return storage.ErrInvalidReceiptHandle(derr.Error())
	}
	if verr := validateReceiptQueue(reply, queueName); verr != nil {
		return verr
	}
	return c.publishAck(ctx, reply, []byte("+ACK"))
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
) (err error) {
	defer observability.StartObserve("change_visibility").Done(&err)

	if receiptHandle == "" {
		return storage.ErrInvalidArgument("receipt handle vazio")
	}
	if visibilityTimeout < 0 || visibilityTimeout > 43200 {
		return storage.ErrInvalidArgument("visibilityTimeout fora do range [0, 43200]")
	}
	reply, derr := decodeReceiptHandle(receiptHandle)
	if derr != nil {
		return storage.ErrInvalidReceiptHandle(derr.Error())
	}
	if verr := validateReceiptQueue(reply, queueName); verr != nil {
		return verr
	}

	// "-NAK {\"delay\": ns}" é o payload wire que NakWithDelay usa —
	// devolve a mensagem para redelivery após o delay (= novo visibility
	// timeout). VisibilityTimeout=0 usa "-NAK" puro: redelivery imediato.
	payload := []byte("-NAK")
	if visibilityTimeout > 0 {
		payload = []byte(fmt.Sprintf(`-NAK {"delay": %d}`,
			(time.Duration(visibilityTimeout) * time.Second).Nanoseconds()))
	}
	return c.publishAck(ctx, reply, payload)
}

// QueueDepth devolve o número aproximado de mensagens disponíveis.
//
// Lê StreamInfo.State.Msgs. Com WorkQueuePolicy, mensagens acked são
// removidas do stream — State.Msgs conta apenas não-consumidas (visíveis
// + in-flight), próximo do ApproximateNumberOfMessages do SQS.
func (c *Client) QueueDepth(ctx context.Context, queueName string) (result int64, err error) {
	defer observability.StartObserve("queue_depth").Done(&err)

	streamName := queueStreamName(queueName)
	stream, serr := c.js.Stream(ctx, streamName)
	if serr != nil {
		if errors.Is(serr, jetstream.ErrStreamNotFound) {
			return 0, storage.ErrQueueNotFound
		}
		return 0, fmt.Errorf("get stream: %w", serr)
	}

	info, ierr := stream.Info(ctx)
	if ierr != nil {
		return 0, fmt.Errorf("stream info: %w", ierr)
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
