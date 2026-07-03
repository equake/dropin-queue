// Package protocol implementa os parsers e serializers dos protocolos
// AWS SQS/SNS, em suas variantes Query (form-encoded + XML) e JSON.
//
// A camada superior (server) identifica o protocolo pelo header
// 'X-Amz-Target' (JSON) ou ausência dele (Query), e delega para
// este pacote.
//
// Cada operação AWS tem dois "shapes":
//
//   - Request: parâmetros que o cliente envia (form-encoded ou JSON)
//   - Response: resposta do servidor (XML ou JSON)
//
// Este pacote traduz ambos para representações internas (url.Values ou
// map[string]any) e de volta. Validação semântica fica na camada service.
package protocol

import (
	"errors"
	"fmt"
)

// Action é o nome canônico de uma operação AWS.
type Action string

// Operações SQS (subset inicial).
const (
	ActionCreateQueue             Action = "CreateQueue"
	ActionGetQueueUrl             Action = "GetQueueUrl"
	ActionGetQueueAttributes      Action = "GetQueueAttributes"
	ActionListQueues              Action = "ListQueues"
	ActionSendMessage             Action = "SendMessage"
	ActionReceiveMessage          Action = "ReceiveMessage"
	ActionDeleteMessage           Action = "DeleteMessage"
	ActionChangeMessageVisibility Action = "ChangeMessageVisibility"
	ActionPurgeQueue              Action = "PurgeQueue"
	ActionDeleteQueue             Action = "DeleteQueue"
	ActionSetQueueAttributes      Action = "SetQueueAttributes"
	ActionSendMessageBatch        Action = "SendMessageBatch"
	ActionDeleteMessageBatch      Action = "DeleteMessageBatch"
)

// Operações SNS (subset).
const (
	ActionCreateTopic              Action = "CreateTopic"
	ActionGetTopicAttributes       Action = "GetTopicAttributes"
	ActionListTopics               Action = "ListTopics"
	ActionSubscribe                Action = "Subscribe"
	ActionUnsubscribe              Action = "Unsubscribe"
	ActionPublish                  Action = "Publish"
	ActionListSubscriptions        Action = "ListSubscriptions"
	ActionListSubscriptionsByTopic Action = "ListSubscriptionsByTopic"
	ActionDeleteTopic              Action = "DeleteTopic"
	ActionConfirmSubscription      Action = "ConfirmSubscription"
)

// ServiceName identifica o serviço AWS (sqs, sns).
type ServiceName string

const (
	ServiceSQS ServiceName = "sqs"
	ServiceSNS ServiceName = "sns"
)

// Protocol identifica o protocolo wire usado.
type Protocol string

const (
	// ProtocolQuery: form-encoded + XML (SQS legado, ainda default em vários SDKs).
	ProtocolQuery Protocol = "query"

	// ProtocolJSON: AWS JSON 1.0 (JSON request + JSON response).
	ProtocolJSON Protocol = "json"
)

// AWSProtocolVersion é a versão de protocolo que o shim se declara compatível.
// Definido em SQS_API_VERSION env ou constante.
const AWSProtocolVersion = "2012-11-05"

// SNSProtocolVersion é a versão do protocolo SNS.
const SNSProtocolVersion = "2010-03-31"

// RequestKind identifica qual operação foi pedida, validada contra a
// lista de ações conhecidas por serviço.
type RequestKind struct {
	Service   ServiceName
	Action    Action
	Protocol  Protocol
	AccountID string
	Region    string
}

// Validate verifica se a RequestKind tem campos válidos.
func (r RequestKind) Validate() error {
	if r.Service != ServiceSQS && r.Service != ServiceSNS {
		return fmt.Errorf("serviço inválido: %q", r.Service)
	}
	if r.Action == "" {
		return errors.New("action vazio")
	}
	switch r.Protocol {
	case ProtocolQuery, ProtocolJSON:
		// ok
	default:
		return fmt.Errorf("protocolo inválido: %q", r.Protocol)
	}
	if r.AccountID != "" && len(r.AccountID) != 12 {
		return fmt.Errorf("account-id deve ter 12 dígitos: %q", r.AccountID)
	}
	if r.Region == "" {
		return errors.New("region vazio")
	}
	return nil
}

// knownSQSActions lista todas as ações SQS válidas.
var knownSQSActions = map[Action]struct{}{
	ActionCreateQueue: {}, ActionGetQueueUrl: {}, ActionGetQueueAttributes: {},
	ActionListQueues: {}, ActionSendMessage: {}, ActionReceiveMessage: {},
	ActionDeleteMessage: {}, ActionChangeMessageVisibility: {},
	ActionPurgeQueue: {}, ActionDeleteQueue: {}, ActionSetQueueAttributes: {},
	ActionSendMessageBatch: {}, ActionDeleteMessageBatch: {},
}

// knownSNSActions lista todas as ações SNS válidas.
var knownSNSActions = map[Action]struct{}{
	ActionCreateTopic: {}, ActionGetTopicAttributes: {}, ActionListTopics: {},
	ActionSubscribe: {}, ActionUnsubscribe: {}, ActionPublish: {},
	ActionListSubscriptions: {}, ActionListSubscriptionsByTopic: {},
	ActionDeleteTopic: {}, ActionConfirmSubscription: {},
}

// IsValidAction retorna true se a ação é conhecida para o serviço.
func IsValidAction(service ServiceName, action Action) bool {
	switch service {
	case ServiceSQS:
		_, ok := knownSQSActions[action]
		return ok
	case ServiceSNS:
		_, ok := knownSNSActions[action]
		return ok
	}
	return false
}
