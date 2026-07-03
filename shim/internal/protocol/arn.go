package protocol

import (
	"fmt"
	"strings"
)

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

// ResourceNameFromARN extrai o nome do recurso (queue OU topic, formato
// idêntico) de um ARN parseando "<service>:<region>:<account>:<name>".
//
// Funciona para SQS E SNS porque formato é estruturalmente idêntico:
//
//	arn:aws:sqs:us-east-1:000000000000:my-queue   → "my-queue"
//	arn:aws:sns:us-east-1:000000000000:my-topic   → "my-topic"
//
// Pré-refactor (refactor/kiss-dry-pass-1): extractTopicName em sns/
// service.go e extractTopicName em storage/nats/topics.go tinham
// implementações idênticas byte-a-byte. Centralizado em protocol/.
//
// Devolve "" se ARN malformado (sem 6 tokens).
func ResourceNameFromARN(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) < 6 {
		return ""
	}
	return parts[5]
}

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

// QueueNameFromURL extrai o nome da fila do último segmento do path
// de uma QueueURL (ou ARN arn:aws:sqs:...:queueName se input começar
// com "arn:").
//
// Formatos suportados:
//
//	arn:aws:sqs:us-east-1:000000000000:my-queue   → "my-queue"
//	http://localhost:4566/000000000000/my-queue   → "my-queue"
//	https://sqs.us-east-1.amazonaws.com/.../foo   → "foo"
//
// Pré-refactor (refactor/kiss-dry-pass-1): queueNameFromURL em sqs/
// service.go (path-only) + extractQueueNameFromEndpoint em storage/
// nats/topics.go (arn ou path). Mesma lógica, dois lugares.
//
// Devolve "" se endpoint/URL malformado.
func QueueNameFromURL(endpoint string) string {
	if strings.HasPrefix(endpoint, "arn:") {
		return ResourceNameFromARN(endpoint)
	}
	idx := strings.LastIndex(endpoint, "/")
	if idx < 0 || idx == len(endpoint)-1 {
		return endpoint
	}
	return endpoint[idx+1:]
}
