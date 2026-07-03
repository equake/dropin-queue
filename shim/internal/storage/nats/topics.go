package nats

import (
	"context"
	"errors"

	"github.com/anomalyco/generic_queue/shim/pkg/types"
)

// CreateTopic cria um stream JetStream que representa o tópico SNS.
//
// **Ainda não implementado — Semana 4.**
// Será implementado usando AddStream com subject "t.<topicName>" e
// subscrevendo cada subscriber como consumer durável que republica
// para o destino (queue SQS ou HTTP POST).
func (c *Client) CreateTopic(ctx context.Context, t types.Topic) (*types.Topic, error) {
	return nil, errors.New("CreateTopic: não implementado (Semana 4)")
}

// GetTopic busca um tópico existente.
func (c *Client) GetTopic(ctx context.Context, name string) (*types.Topic, error) {
	return nil, errors.New("GetTopic: não implementado (Semana 4)")
}

// ListTopics lista todos os tópicos.
func (c *Client) ListTopics(ctx context.Context, prefix string) ([]types.Topic, error) {
	return nil, errors.New("ListTopics: não implementado (Semana 4)")
}

// DeleteTopic remove um tópico.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	return errors.New("DeleteTopic: não implementado (Semana 4)")
}

// Subscribe adiciona uma inscrição.
func (c *Client) Subscribe(ctx context.Context, sub types.Subscription) (*types.Subscription, error) {
	return nil, errors.New("Subscribe: não implementado (Semana 4)")
}

// ListSubscriptions lista inscrições.
func (c *Client) ListSubscriptions(ctx context.Context, topicName string) ([]types.Subscription, error) {
	return nil, errors.New("ListSubscriptions: não implementado (Semana 4)")
}

// Unsubscribe remove uma inscrição.
func (c *Client) Unsubscribe(ctx context.Context, subscriptionARN string) error {
	return errors.New("Unsubscribe: não implementado (Semana 4)")
}

// Publish publica uma mensagem em um tópico (fan-out).
func (c *Client) Publish(ctx context.Context, topicName string, msg *types.Message) (*types.Message, error) {
	return nil, errors.New("Publish: não implementado (Semana 4)")
}
