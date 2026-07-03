package nats

import (
	"context"
	"errors"

	"github.com/anomalyco/generic_queue/shim/internal/storage"
	"github.com/anomalyco/generic_queue/shim/pkg/types"
)

// SendMessage publica uma mensagem em uma fila.
//
// **Ainda não implementado — Semana 3.**
// Será implementado usando js.PublishAsync com:
//
//   - subject = "q.<queueName>.<messageGroupId>" (FIFO) ou "q.<queueName>.default"
//   - headers[Nats-Msg-Id] = MessageDeduplicationId (para dedup FIFO)
//   - payload = body da mensagem
//   - retorno = ack com Sequence e timestamp
func (c *Client) SendMessage(ctx context.Context, queueName string, msg *types.Message) (*types.Message, error) {
	return nil, errors.New("SendMessage: não implementado (Semana 3)")
}

// ReceiveMessage consome até max mensagens da fila com long-polling.
//
// **Ainda não implementado — Semana 3.**
// Será implementado usando js.Fetch com:
//
//   - batch = maxMessages
//   - MaxWait = waitSeconds (long-poll nativo JetStream)
//   - AckWait = visibilityTimeout (visibility timeout nativo)
//   - retorno = mensagens com receipt handle (metadata.Sequence.Stream + ack.Reply)
func (c *Client) ReceiveMessage(ctx context.Context, queueName string, maxMessages, waitSeconds, visibilityTimeout int32) ([]types.Message, error) {
	return nil, errors.New("ReceiveMessage: não implementado (Semana 3)")
}

// DeleteMessage confirma o ack de uma mensagem.
//
// **Ainda não implementado — Semana 3.**
func (c *Client) DeleteMessage(ctx context.Context, queueName string, receiptHandle string) error {
	return errors.New("DeleteMessage: não implementado (Semana 3)")
}

// ChangeMessageVisibility redefine o tempo de visibilidade.
//
// **Ainda não implementado — Semana 3.**
func (c *Client) ChangeMessageVisibility(ctx context.Context, queueName string, receiptHandle string, visibilityTimeout int32) error {
	return errors.New("ChangeMessageVisibility: não implementado (Semana 3)")
}

// QueueDepth devolve o número aproximado de mensagens disponíveis.
//
// **Ainda não implementado — Semana 3.**
// Será implementado lendo StreamInfo.State.Msgs.
func (c *Client) QueueDepth(ctx context.Context, queueName string) (int64, error) {
	return 0, storage.ErrBrokerUnavailable
}
