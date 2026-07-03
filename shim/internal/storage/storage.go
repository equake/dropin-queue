// Package storage define as interfaces de acesso ao broker de mensageria.
//
// A camada service (sqs/sns) depende apenas destas interfaces. Trocar o
// broker subjacente (NATS JetStream, Kafka, RabbitMQ) significa implementar
// uma nova struct que satisfaça estas interfaces — sem mudar nada acima.
//
// Princípios:
//
//   - Toda operação bloqueia até o broker responder ou ctx ser cancelado.
//   - Erros retornam valores sentinela (ErrQueueNotFound, etc.) ou erro
//     bruto do broker; a camada service faz tradução para AWS error codes.
//   - Não há retry automático aqui — quem decide é a camada service, com
//     suas próprias políticas (idempotência, exponential backoff, etc.).
package storage

import (
	"context"
	"errors"

	"github.com/anomalyco/generic_queue/shim/pkg/types"
)

// Erros sentinela que o storage pode devolver. A camada service traduz
// para AWS error codes compatíveis (QueueDoesNotExist, etc.).
var (
	// ErrQueueNotFound: CreateQueue/Get/Purge/etc em fila inexistente.
	ErrQueueNotFound = errors.New("queue not found")

	// ErrQueueAlreadyExists: CreateQueue em fila que já existe.
	ErrQueueAlreadyExists = errors.New("queue already exists")

	// ErrTopicNotFound: SNS CreateTopic idempotency check falhou ou
	// operação em tópico inexistente.
	ErrTopicNotFound = errors.New("topic not found")

	// ErrMessageNotFound: DeleteMessage/ChangeMessageVisibility com
	// receipt handle inválido ou expirado.
	ErrMessageNotFound = errors.New("message not found")

	// ErrBrokerUnavailable: broker offline ou ctx cancelado.
	ErrBrokerUnavailable = errors.New("broker unavailable")
)

// QueueStorage é a interface que abstrai operações de filas no broker.
//
// Toda fila no broker corresponde a um "stream" ou "queue" (terminologia
// varia). Os métodos aqui expõem apenas a API SQS — detalhes do broker
// ficam escondidos.
type QueueStorage interface {
	// CreateQueue cria uma fila nova ou devolve a existente se já houver
	// uma com mesmo nome (idempotente como SQS).
	CreateQueue(ctx context.Context, q types.Queue) (*types.Queue, error)

	// GetQueue busca uma fila por nome. Devolve ErrQueueNotFound se não
	// existir.
	GetQueue(ctx context.Context, name string) (*types.Queue, error)

	// ListQueues lista todas as filas. Filtro opcional por prefixo
	// (SQS ListQueues com QueueNamePrefix).
	ListQueues(ctx context.Context, prefix string) ([]types.Queue, error)

	// DeleteQueue remove uma fila. Idempotente: ErrQueueNotFound é OK.
	DeleteQueue(ctx context.Context, name string) error

	// PurgeQueue esvazia a fila sem removê-la.
	PurgeQueue(ctx context.Context, name string) error

	// SetQueueAttributes atualiza atributos mutáveis (VisibilityTimeout,
	// MessageRetentionPeriod, etc.).
	SetQueueAttributes(ctx context.Context, name string, attrs types.QueueAttributes) error
}

// MessageStorage é a interface que abstrai operações de mensagens.
//
// Separada de QueueStorage porque mensagens têm ciclo de vida próprio
// (visibility timeout, ack, etc.) e podem ser mockadas independentemente
// em testes.
type MessageStorage interface {
	// SendMessage publica uma mensagem em uma fila. Devolve o ID da mensagem
	// e o MD5 do body.
	SendMessage(ctx context.Context, queueName string, msg *types.Message) (*types.Message, error)

	// ReceiveMessage consome até max mensagens da fila. Se waitSeconds > 0,
	// bloqueia até esse tempo aguardando mensagens (long-polling). Cada
	// mensagem devolvida tem visibility timeout armado.
	ReceiveMessage(ctx context.Context, queueName string, maxMessages int32, waitSeconds int32, visibilityTimeout int32) ([]types.Message, error)

	// DeleteMessage remove uma mensagem usando o receipt handle.
	DeleteMessage(ctx context.Context, queueName string, receiptHandle string) error

	// ChangeMessageVisibility redefine o tempo de visibilidade de uma
	// mensagem já recebida (estende ou reduz).
	ChangeMessageVisibility(ctx context.Context, queueName string, receiptHandle string, visibilityTimeout int32) error

	// QueueDepth devolve o número aproximado de mensagens disponíveis
	// (não inclui invisíveis). Usado para métricas e GetQueueAttributes.
	QueueDepth(ctx context.Context, queueName string) (int64, error)
}

// TopicStorage é a interface para tópicos SNS (fan-out).
type TopicStorage interface {
	// CreateTopic cria ou devolve um tópico existente.
	CreateTopic(ctx context.Context, t types.Topic) (*types.Topic, error)

	// GetTopic busca tópico por nome.
	GetTopic(ctx context.Context, name string) (*types.Topic, error)

	// ListTopics lista todos os tópicos.
	ListTopics(ctx context.Context, prefix string) ([]types.Topic, error)

	// DeleteTopic remove um tópico.
	DeleteTopic(ctx context.Context, name string) error

	// Subscribe adiciona uma inscrição a um tópico.
	Subscribe(ctx context.Context, sub types.Subscription) (*types.Subscription, error)

	// ListSubscriptions lista inscrições (opcionalmente filtradas por tópico).
	ListSubscriptions(ctx context.Context, topicName string) ([]types.Subscription, error)

	// Unsubscribe remove uma inscrição pelo ARN.
	Unsubscribe(ctx context.Context, subscriptionARN string) error

	// Publish publica uma mensagem em um tópico. O fan-out para subscribers
	// é responsabilidade do storage; subscription filter policies também.
	Publish(ctx context.Context, topicName string, msg *types.Message) (*types.Message, error)
}

// Storage agrega todas as interfaces — útil para injetar em services.
type Storage interface {
	Queues() QueueStorage
	Messages() MessageStorage
	Topics() TopicStorage

	// Close fecha conexões com o broker.
	Close() error
}
