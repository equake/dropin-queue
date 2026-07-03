// Package types contém tipos públicos compartilhados entre pacotes do shim.
// Estes tipos são os blocos fundamentais para representar filas, mensagens,
// tópicos e configurações que trafegam entre as camadas de protocolo,
// serviço e storage.
package types

import "time"

// Queue representa uma fila SQS (Standard ou FIFO).
//
// Uma Queue é a abstração de alto nível. O mapeamento para o broker
// subjacente (NATS JetStream) é responsabilidade do pacote storage.
type Queue struct {
	// Name é o nome da fila (sem prefixo ARN). Único por account.
	Name string

	// URL é a URL completa para acessar a fila via API. Construída como
	// http(s)://<endpoint>/<account-id>/<name>.
	URL string

	// ARN é o Amazon Resource Name da fila, formato:
	// arn:aws:sqs:<region>:<account-id>:<name>
	ARN string

	// AccountID é o ID da conta dona da fila (12 dígitos).
	AccountID string

	// Region é a região AWS simulada.
	Region string

	// Attributes são os atributos SQS da fila (VisibilityTimeout, MessageRetentionPeriod,
	// MaximumMessageSize, DelaySeconds, ReceiveMessageWaitTimeSeconds, etc.).
	Attributes QueueAttributes

	// Tags são tags opcionais associadas à fila.
	Tags map[string]string

	// FIFO indica se a fila é FIFO (SQS FIFO queues).
	FIFO bool

	// CreatedAt é o momento de criação da fila.
	CreatedAt time.Time
}

// QueueAttributes contém os atributos configuráveis de uma fila SQS.
// Os campos seguem os nomes oficiais do AWS SQS para fácil interoperabilidade.
type QueueAttributes struct {
	// VisibilityTimeout (segundos): tempo que mensagem fica invisível após ReceiveMessage.
	// Default: 30. Range: 0-43200 (12h). SQS Standard só.
	VisibilityTimeout int32

	// MessageRetentionPeriod (segundos): quanto tempo mensagem fica armazenada.
	// Default: 345600 (4 dias). Range: 60-1209600 (14 dias).
	MessageRetentionPeriod int32

	// MaximumMessageSize (bytes): tamanho máximo de cada mensagem.
	// Default: 262144 (256 KiB). Range: 1024-262144.
	MaximumMessageSize int32

	// DelaySeconds (segundos): delay antes da mensagem ficar disponível.
	// Default: 0. Range: 0-900 (15min). SQS Standard só.
	DelaySeconds int32

	// ReceiveMessageWaitTimeSeconds: long polling máximo.
	// Default: 0. Range: 0-20.
	ReceiveMessageWaitTimeSeconds int32

	// ContentBasedDeduplication (FIFO only): dedup baseado em hash do body.
	ContentBasedDeduplication bool
}

// DefaultQueueAttributes retorna os defaults oficiais do SQS Standard.
func DefaultQueueAttributes() QueueAttributes {
	return QueueAttributes{
		VisibilityTimeout:             30,
		MessageRetentionPeriod:        345600,
		MaximumMessageSize:            262144,
		DelaySeconds:                  0,
		ReceiveMessageWaitTimeSeconds: 0,
		ContentBasedDeduplication:     false,
	}
}

// Message representa uma mensagem enfileirada em uma Queue.
type Message struct {
	// ID é o handle único da mensagem (SQS MessageId). Diferente do ReceiptHandle.
	ID string

	// ReceiptHandle é o token usado para DeleteMessage/ChangeMessageVisibility.
	// Muda a cada ReceiveMessage; só é válido enquanto visibility timeout não expirar.
	ReceiptHandle string

	// Body é o conteúdo da mensagem.
	Body string

	// MD5OfBody é o MD5 hex do body. Retornado no ReceiveMessage para o cliente
	// poder validar integridade.
	MD5OfBody string

	// Attributes são atributos da mensagem (SentTimestamp, ApproximateReceiveCount, etc.).
	Attributes map[string]string

	// MessageAttributes são MessageAttributes no formato SQS (Name, DataType, Value).
	MessageAttributes map[string]MessageAttribute

	// EnqueuedAt é o timestamp em que a mensagem foi enfileirada.
	EnqueuedAt time.Time

	// MessageGroupId (FIFO only): agrupa mensagens que devem ser processadas
	// em ordem. Filas Standard ignoram.
	MessageGroupId string

	// MessageDeduplicationId (FIFO only): dedup nativo JetStream via
	// Nats-Msg-Id header (TTL 2min default).
	MessageDeduplicationId string
}

// MessageAttribute representa um MessageAttribute SQS.
//
// SQS aceita tipos String, String.List, Number, Binary. Para Binary,
// usamos Base64 no campo StringValue.
type MessageAttribute struct {
	DataType    string
	StringValue string
	BinaryValue []byte
}

// Topic representa um tópico SNS.
type Topic struct {
	Name      string
	ARN       string
	AccountID string
	Region    string
	CreatedAt time.Time
}

// Subscription representa uma inscrição SNS → destino (sqs/http/https/etc.).
type Subscription struct {
	ARN          string
	TopicARN     string
	Protocol     string // "sqs", "http", "https", "email", "email-json", "sms", "application"
	Endpoint     string
	FilterPolicy string
	RawDelivery  bool
	Pending      bool // awaiting ConfirmSubscription
	CreatedAt    time.Time
}
