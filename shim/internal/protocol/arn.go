package protocol

import "fmt"

// ARN (Amazon Resource Name) identifica unicamente um recurso AWS.
//
// Formato SQS:
//
//	arn:aws:sqs:<region>:<account-id>:<queue-name>
//
// Formato SNS:
//
//	arn:aws:sns:<region>:<account-id>:<topic-name>
type ARN string

// NewSQSARN constrói um ARN para fila SQS.
func NewSQSARN(region, accountID, queueName string) ARN {
	return ARN(fmt.Sprintf("arn:aws:sqs:%s:%s:%s", region, accountID, queueName))
}

// NewSNSARN constrói um ARN para tópico SNS.
func NewSNSARN(region, accountID, topicName string) ARN {
	return ARN(fmt.Sprintf("arn:aws:sns:%s:%s:%s", region, accountID, topicName))
}

// String retorna o ARN como string.
func (a ARN) String() string { return string(a) }

// QueueURL é a URL HTTP para acessar uma fila SQS.
//
// Formato AWS:
//
//	http(s)://<endpoint>/<account-id>/<queue-name>
type QueueURL string

// NewQueueURL constrói a URL canônica de uma fila.
//
// endpoint: scheme + host (sem porta final se for padrão). Ex: "http://localhost:4566".
func NewQueueURL(endpoint, accountID, queueName string) QueueURL {
	return QueueURL(fmt.Sprintf("%s/%s/%s", endpoint, accountID, queueName))
}

// String retorna a URL como string.
func (u QueueURL) String() string { return string(u) }
