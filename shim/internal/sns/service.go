// Package sns implementa a lógica das operações SNS.
//
// Esta camada fica entre o protocol layer (parser/serializer AWS) e o
// storage (broker). Aqui moram:
//
//   - Validação semântica dos parâmetros
//   - Construção de ARNs
//   - Tradução de erros do storage para AWS error codes
//   - Integração com métricas
//
// Cada operação AWS corresponde a um método neste pacote.
package sns

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/equake/dropin-queue/shim/internal/awserr"
	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/protocol"
	"github.com/equake/dropin-queue/shim/internal/storage"
	"github.com/equake/dropin-queue/shim/pkg/types"
)

// AWSError é alias para awserr.Error (preserva call sites do pacote sns).
//
// Pré-refactor (refactor/kiss-dry-pass-1) existia AWSError local em sqs/
// e sns/ — agora compartilhado, com classificação sender-fault registrada
// explicitamente via RegisterSenderFaults em init().
type AWSError = awserr.Error

// AWSErrorCode são os códigos oficiais AWS SNS para erros que devolvemos.
//
// Específicos do SNS (TopicAlreadyExists etc.) ficam aqui; codes
// compartilhados com SQS vêm de awserr.
const (
	// Específicos SNS:
	ErrCodeTopicAlreadyExists       = "TopicAlreadyExists"
	ErrCodeTopicDoesNotExist        = "TopicDoesNotExist"
	ErrCodeInvalidProtocol          = "InvalidParameter"
	ErrCodeSubscriptionDoesNotExist = "SubscriptionDoesNotExist"

	// Compartilhados com AWS API (definidos em awserr):
	ErrCodeInvalidParameterValue = awserr.CodeInvalidParameterValue
	ErrCodeMissingParameter      = awserr.CodeMissingParameter
	ErrCodeInternalError         = awserr.CodeInternalError
	ErrCodeUnsupportedOperation  = awserr.CodeUnsupportedOperation
)

func init() {
	// Registra codes específicos do SNS como sender-fault (4xx).
	awserr.RegisterSenderFaults(
		ErrCodeTopicAlreadyExists,
		ErrCodeTopicDoesNotExist,
		ErrCodeInvalidProtocol,
		ErrCodeSubscriptionDoesNotExist,
	)
}

// AsAWSError extrai um AWSError de um erro genérico.
//
// Refatorado no refactor/kiss-dry-pass-1: delega ao awserr.FromStorage.
// O notFoundCode passado é "TopicDoesNotExist" (específico SNS).
func AsAWSError(err error) *AWSError {
	return awserr.FromStorage(err, ErrCodeTopicDoesNotExist)
}

// Service implementa as operações SNS.
//
// É goroutine-safe — os métodos delegam para storage que também é.
type Service struct {
	storage     storage.Storage
	accountID   string
	region      string
	endpointURL string // ex: "http://localhost:4566"
}

// New cria um Service SNS.
func New(s storage.Storage, accountID, region, endpointURL string) *Service {
	return &Service{
		storage:     s,
		accountID:   accountID,
		region:      region,
		endpointURL: strings.TrimSuffix(endpointURL, "/"),
	}
}

// Region retorna a região configurada (usada pelos handlers HTTP para construir ARNs).
func (s *Service) Region() string { return s.region }

// AccountID retorna o account ID configurado.
func (s *Service) AccountID() string { return s.accountID }

// --- CreateTopic ---

// CreateTopicParams contém os parâmetros de CreateTopic.
type CreateTopicParams struct {
	Name string
	Tags map[string]string // opcional
}

// CreateTopicResult contém o ARN do tópico criado (ou existente).
type CreateTopicResult struct {
	TopicARN string
}

// CreateTopic cria ou devolve um tópico existente.
//
// Comportamento AWS: idempotente (devolve mesmo ARN se já existir).
func (s *Service) CreateTopic(ctx context.Context, params *CreateTopicParams) (*CreateTopicResult, error) {
	if params == nil || params.Name == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "Name é obrigatório",
		}
	}
	if !isValidTopicName(params.Name) {
		return nil, &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: fmt.Sprintf("Name inválido: %q (1-256 chars alfanuméricos + . _ -)", params.Name),
		}
	}

	t := types.Topic{
		Name:      params.Name,
		AccountID: s.accountID,
		Region:    s.region,
	}

	created, err := s.storage.Topics().CreateTopic(ctx, t)
	if err != nil {
		return nil, err
	}

	arn := protocol.NewSNSARN(s.region, s.accountID, created.Name).String()
	observability.L().Info("tópico SNS criado",
		"topic", created.Name,
		"arn", arn,
	)
	return &CreateTopicResult{TopicARN: arn}, nil
}

// CreateTopicParamsFromQuery normaliza Query → CreateTopicParams.
func CreateTopicParamsFromQuery(params url.Values) *CreateTopicParams {
	return &CreateTopicParams{
		Name: params.Get("Name"),
		Tags: extractTagsQuery(params),
	}
}

// CreateTopicParamsFromJSON normaliza JSON → CreateTopicParams.
func CreateTopicParamsFromJSON(params map[string]any) *CreateTopicParams {
	p := &CreateTopicParams{}
	if s, ok := params["Name"].(string); ok {
		p.Name = s
	}
	if raw, ok := params["Tags"]; ok {
		if arr, ok := raw.([]any); ok {
			for _, item := range arr {
				if m, ok := item.(map[string]any); ok {
					k, _ := m["Key"].(string)
					v, _ := m["Value"].(string)
					if k != "" {
						if p.Tags == nil {
							p.Tags = make(map[string]string)
						}
						p.Tags[k] = v
					}
				}
			}
		}
	}
	return p
}

// --- GetTopicAttributes ---

// GetTopicAttributesParams contém os parâmetros.
type GetTopicAttributesParams struct {
	TopicARN string
}

// GetTopicAttributesResult contém os atributos do tópico.
type GetTopicAttributesResult struct {
	Attributes map[string]string
}

// GetTopicAttributes devolve os atributos do tópico.
func (s *Service) GetTopicAttributes(ctx context.Context, params *GetTopicAttributesParams) (*GetTopicAttributesResult, error) {
	if params == nil || params.TopicARN == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "TopicArn é obrigatório",
		}
	}

	topicName := extractTopicName(params.TopicARN)
	if topicName == "" {
		return nil, &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: fmt.Sprintf("TopicArn inválido: %q", params.TopicARN),
		}
	}

	t, err := s.storage.Topics().GetTopic(ctx, topicName)
	if err != nil {
		return nil, err
	}

	attrs := map[string]string{
		"TopicArn":                params.TopicARN,
		"Owner":                   s.accountID,
		"Policy":                  `{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"__default_statement_ID","Effect":"Allow","Principal":{"AWS":"*"},"Action":["SNS:Publish","SNS:RemovePermission","SNS:DeleteTopic","SNS:GetTopicAttributes","SNS:AddPermission","SNS:ListSubscriptions","SNS:ListSubscriptionsByTopic","SNS:Subscribe"],"Resource":"arn:aws:sns:*:*:` + t.Name + `","Condition":{"StringEquals":{"AWS:SourceOwner":"` + s.accountID + `"}}}]}`,
		"DisplayName":             "",
		"DeliveryPolicy":          "",
		"SubscriptionsConfirmed":  "0",
		"SubscriptionsDeleted":    "0",
		"SubscriptionsPending":    "0",
		"EffectiveDeliveryPolicy": `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"}}, "https":{"defaultHealthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"}}}`,
		"CreatedTimestamp":        fmt.Sprintf("%d", t.CreatedAt.UnixMilli()),
	}
	return &GetTopicAttributesResult{Attributes: attrs}, nil
}

// GetTopicAttributesParamsFromQuery normaliza Query → GetTopicAttributesParams.
func GetTopicAttributesParamsFromQuery(params url.Values) *GetTopicAttributesParams {
	return &GetTopicAttributesParams{TopicARN: params.Get("TopicArn")}
}

// GetTopicAttributesParamsFromJSON normaliza JSON → GetTopicAttributesParams.
func GetTopicAttributesParamsFromJSON(params map[string]any) *GetTopicAttributesParams {
	p := &GetTopicAttributesParams{}
	if s, ok := params["TopicArn"].(string); ok {
		p.TopicARN = s
	}
	return p
}

// --- ListTopics ---

// ListTopicsParams contém os parâmetros.
type ListTopicsParams struct {
	NextToken string
}

// ListTopicsResult contém os tópicos listados.
type ListTopicsResult struct {
	Topics    []types.Topic
	NextToken string
}

// ListTopics lista todos os tópicos.
func (s *Service) ListTopics(ctx context.Context, _ *ListTopicsParams) (*ListTopicsResult, error) {
	topics, err := s.storage.Topics().ListTopics(ctx, "")
	if err != nil {
		return nil, err
	}
	// TODO: pagination via NextToken (Semana 5+)
	return &ListTopicsResult{Topics: topics}, nil
}

// ListTopicsParamsFromQuery normaliza Query → ListTopicsParams.
func ListTopicsParamsFromQuery(params url.Values) *ListTopicsParams {
	return &ListTopicsParams{NextToken: params.Get("NextToken")}
}

// ListTopicsParamsFromJSON normaliza JSON → ListTopicsParams.
func ListTopicsParamsFromJSON(params map[string]any) *ListTopicsParams {
	p := &ListTopicsParams{}
	if s, ok := params["NextToken"].(string); ok {
		p.NextToken = s
	}
	return p
}

// --- DeleteTopic ---

// DeleteTopicParams contém os parâmetros.
type DeleteTopicParams struct {
	TopicARN string
}

// DeleteTopic remove um tópico (e todas as inscrições dele).
func (s *Service) DeleteTopic(ctx context.Context, params *DeleteTopicParams) error {
	if params == nil || params.TopicARN == "" {
		return &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "TopicArn é obrigatório",
		}
	}
	topicName := extractTopicName(params.TopicARN)
	if topicName == "" {
		return &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: fmt.Sprintf("TopicArn inválido: %q", params.TopicARN),
		}
	}
	return s.storage.Topics().DeleteTopic(ctx, topicName)
}

// DeleteTopicParamsFromQuery normaliza Query → DeleteTopicParams.
func DeleteTopicParamsFromQuery(params url.Values) *DeleteTopicParams {
	return &DeleteTopicParams{TopicARN: params.Get("TopicArn")}
}

// DeleteTopicParamsFromJSON normaliza JSON → DeleteTopicParams.
func DeleteTopicParamsFromJSON(params map[string]any) *DeleteTopicParams {
	p := &DeleteTopicParams{}
	if s, ok := params["TopicArn"].(string); ok {
		p.TopicARN = s
	}
	return p
}

// --- Subscribe ---

// SubscribeParams contém os parâmetros de Subscribe.
type SubscribeParams struct {
	TopicARN              string
	Protocol              string // "sqs", "http", "https", "email", "email-json", "sms", "application"
	Endpoint              string
	FilterPolicy          string // JSON string, opcional
	RawDelivery           bool   // opcional (default false)
	ReturnSubscriptionARN bool   // opcional (default true)
}

// SubscribeResult contém o ARN da inscrição criada.
type SubscribeResult struct {
	SubscriptionARN string
}

// Subscribe adiciona uma inscrição a um tópico.
//
// Protocolos suportados no MVP: sqs (entrega via SendMessage na queue).
// Outros protocolos (http/https/email/etc.) ficam Pending=true e não
// recebem deliveries até ConfirmSubscription (não implementado no MVP).
func (s *Service) Subscribe(ctx context.Context, params *SubscribeParams) (*SubscribeResult, error) {
	if params == nil || params.TopicARN == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "TopicArn é obrigatório",
		}
	}
	if params.Protocol == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "Protocol é obrigatório",
		}
	}
	if params.Endpoint == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "Endpoint é obrigatório",
		}
	}
	if !isValidProtocol(params.Protocol) {
		return nil, &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: fmt.Sprintf("Protocol não suportado: %q (válidos: sqs, http, https, email, email-json, sms, application)", params.Protocol),
		}
	}

	// Validar FilterPolicy como JSON se fornecido.
	if params.FilterPolicy != "" {
		var fp map[string]any
		if err := validateJSON(params.FilterPolicy, &fp); err != nil {
			return nil, &AWSError{
				Code:    ErrCodeInvalidParameterValue,
				Message: fmt.Sprintf("FilterPolicy inválido: %v", err),
			}
		}
	}

	sub := types.Subscription{
		TopicARN:     params.TopicARN,
		Protocol:     params.Protocol,
		Endpoint:     params.Endpoint,
		FilterPolicy: params.FilterPolicy,
		RawDelivery:  params.RawDelivery,
	}
	// SQS nunca fica pending (auto-confirma); HTTP/HTTPS ficam pending.
	sub.Pending = (params.Protocol == "http" || params.Protocol == "https")

	created, err := s.storage.Topics().Subscribe(ctx, sub)
	if err != nil {
		return nil, err
	}

	if !params.ReturnSubscriptionARN {
		// AWS devolve "pending confirmation" placeholder para HTTP/HTTPS
		// não confirmados; para SQS já confirmado devolve o ARN real.
		if created.Pending {
			return &SubscribeResult{
				SubscriptionARN: "pending confirmation",
			}, nil
		}
	}

	observability.L().Info("subscribe SNS",
		"topic", params.TopicARN,
		"protocol", params.Protocol,
		"arn", created.ARN,
	)
	return &SubscribeResult{SubscriptionARN: created.ARN}, nil
}

// SubscribeParamsFromQuery normaliza Query → SubscribeParams.
//
// Em Query, atributos de subscription (FilterPolicy, RawDelivery) chegam via
// Attributes.N.Name/Value (formato herdado de SQS SetQueueAttributes) ou
// via Attributes.entry.N.key/Attributes.entry.N.value (formato que boto3
// utiliza para SNS). Suportamos ambos, além de FilterPolicy como parâmetro
// top-level para clientes que enviam desta forma.
func SubscribeParamsFromQuery(params url.Values) *SubscribeParams {
	p := &SubscribeParams{
		TopicARN:              params.Get("TopicArn"),
		Protocol:              params.Get("Protocol"),
		Endpoint:              params.Get("Endpoint"),
		ReturnSubscriptionARN: params.Get("ReturnSubscriptionArn") == "true",
	}
	// FilterPolicy pode vir:
	//   1. Top-level: FilterPolicy=...
	//   2. Attributes.N.Name=FilterPolicy & Attributes.N.Value=... (formato AWS legado)
	//   3. Attributes.entry.N.key=FilterPolicy & Attributes.entry.N.value=... (boto3)
	if fp := params.Get("FilterPolicy"); fp != "" {
		p.FilterPolicy = fp
	} else {
		// (2) Attributes.N.Name / Attributes.N.Value
		for k, v := range params {
			if strings.HasPrefix(k, "Attributes.") && strings.HasSuffix(k, ".Name") && len(v) > 0 && v[0] == "FilterPolicy" {
				idx := k[len("Attributes.") : len(k)-len(".Name")]
				valKey := "Attributes." + idx + ".Value"
				if val, ok := params[valKey]; ok && len(val) > 0 {
					p.FilterPolicy = val[0]
				}
			}
		}
		// (3) Attributes.entry.N.key / Attributes.entry.N.value
		if p.FilterPolicy == "" {
			for k, v := range params {
				if strings.HasPrefix(k, "Attributes.entry.") && strings.HasSuffix(k, ".key") && len(v) > 0 && v[0] == "FilterPolicy" {
					idx := k[len("Attributes.entry.") : len(k)-len(".key")]
					valKey := "Attributes.entry." + idx + ".value"
					if val, ok := params[valKey]; ok && len(val) > 0 {
						p.FilterPolicy = val[0]
					}
				}
			}
		}
	}
	return p
}

// SubscribeParamsFromJSON normaliza JSON → SubscribeParams.
func SubscribeParamsFromJSON(params map[string]any) *SubscribeParams {
	p := &SubscribeParams{}
	if s, ok := params["TopicArn"].(string); ok {
		p.TopicARN = s
	}
	if s, ok := params["Protocol"].(string); ok {
		p.Protocol = s
	}
	if s, ok := params["Endpoint"].(string); ok {
		p.Endpoint = s
	}
	// FilterPolicy pode vir como parâmetro top-level (Query herdado) ou dentro
	// de Attributes (JSON 1.0 — boto3 envia assim).
	if s, ok := params["FilterPolicy"].(string); ok {
		p.FilterPolicy = s
	} else if attrs, ok := params["Attributes"].(map[string]any); ok {
		if s, ok := attrs["FilterPolicy"].(string); ok {
			p.FilterPolicy = s
		}
	}
	if f, ok := params["ReturnSubscriptionArn"].(bool); ok {
		p.ReturnSubscriptionARN = f
	}
	return p
}

// --- Unsubscribe ---

// UnsubscribeParams contém os parâmetros.
type UnsubscribeParams struct {
	SubscriptionARN string
}

// Unsubscribe remove uma inscrição.
func (s *Service) Unsubscribe(ctx context.Context, params *UnsubscribeParams) error {
	if params == nil || params.SubscriptionARN == "" {
		return &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "SubscriptionArn é obrigatório",
		}
	}
	if params.SubscriptionARN == "pending confirmation" {
		// AWS aceita Unsubscribe em pending e remove a inscrição.
		// Sem o ARN real não podemos remover — devolvemos sucesso (idempotente).
		return nil
	}
	return s.storage.Topics().Unsubscribe(ctx, params.SubscriptionARN)
}

// UnsubscribeParamsFromQuery normaliza Query → UnsubscribeParams.
func UnsubscribeParamsFromQuery(params url.Values) *UnsubscribeParams {
	return &UnsubscribeParams{SubscriptionARN: params.Get("SubscriptionArn")}
}

// UnsubscribeParamsFromJSON normaliza JSON → UnsubscribeParams.
func UnsubscribeParamsFromJSON(params map[string]any) *UnsubscribeParams {
	p := &UnsubscribeParams{}
	if s, ok := params["SubscriptionArn"].(string); ok {
		p.SubscriptionARN = s
	}
	return p
}

// --- ListSubscriptions ---

// ListSubscriptionsParams contém os parâmetros.
type ListSubscriptionsParams struct {
	NextToken string
}

// ListSubscriptionsResult contém as inscrições.
type ListSubscriptionsResult struct {
	Subscriptions []types.Subscription
	NextToken     string
}

// ListSubscriptions lista todas as inscrições (de todos os tópicos).
func (s *Service) ListSubscriptions(ctx context.Context, _ *ListSubscriptionsParams) (*ListSubscriptionsResult, error) {
	subs, err := s.storage.Topics().ListSubscriptions(ctx, "")
	if err != nil {
		return nil, err
	}
	return &ListSubscriptionsResult{Subscriptions: subs}, nil
}

// ListSubscriptionsParamsFromQuery normaliza Query → ListSubscriptionsParams.
func ListSubscriptionsParamsFromQuery(params url.Values) *ListSubscriptionsParams {
	return &ListSubscriptionsParams{NextToken: params.Get("NextToken")}
}

// ListSubscriptionsParamsFromJSON normaliza JSON → ListSubscriptionsParams.
func ListSubscriptionsParamsFromJSON(params map[string]any) *ListSubscriptionsParams {
	p := &ListSubscriptionsParams{}
	if s, ok := params["NextToken"].(string); ok {
		p.NextToken = s
	}
	return p
}

// --- ListSubscriptionsByTopic ---

// ListSubscriptionsByTopicParams contém os parâmetros.
type ListSubscriptionsByTopicParams struct {
	TopicARN  string
	NextToken string
}

// ListSubscriptionsByTopic lista inscrições de um tópico específico.
func (s *Service) ListSubscriptionsByTopic(ctx context.Context, params *ListSubscriptionsByTopicParams) (*ListSubscriptionsResult, error) {
	if params == nil || params.TopicARN == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "TopicArn é obrigatório",
		}
	}
	topicName := extractTopicName(params.TopicARN)
	if topicName == "" {
		return nil, &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: fmt.Sprintf("TopicArn inválido: %q", params.TopicARN),
		}
	}
	subs, err := s.storage.Topics().ListSubscriptions(ctx, topicName)
	if err != nil {
		return nil, err
	}
	return &ListSubscriptionsResult{Subscriptions: subs}, nil
}

// ListSubscriptionsByTopicParamsFromQuery normaliza Query → ListSubscriptionsByTopicParams.
func ListSubscriptionsByTopicParamsFromQuery(params url.Values) *ListSubscriptionsByTopicParams {
	return &ListSubscriptionsByTopicParams{
		TopicARN:  params.Get("TopicArn"),
		NextToken: params.Get("NextToken"),
	}
}

// ListSubscriptionsByTopicParamsFromJSON normaliza JSON → ListSubscriptionsByTopicParams.
func ListSubscriptionsByTopicParamsFromJSON(params map[string]any) *ListSubscriptionsByTopicParams {
	p := &ListSubscriptionsByTopicParams{}
	if s, ok := params["TopicArn"].(string); ok {
		p.TopicARN = s
	}
	if s, ok := params["NextToken"].(string); ok {
		p.NextToken = s
	}
	return p
}

// --- Publish ---

// PublishParams contém os parâmetros de Publish.
type PublishParams struct {
	TopicARN          string
	Message           string
	Subject           string // opcional
	MessageStructure  string // opcional: "json" para estrutura especial
	MessageAttributes map[string]protocol.MessageAttributeValue
}

// PublishResult contém o ID da mensagem publicada.
type PublishResult struct {
	MessageID  string
	SequenceNo string
}

// Publish publica uma mensagem em um tópico e faz fan-out para subscribers.
func (s *Service) Publish(ctx context.Context, params *PublishParams) (*PublishResult, error) {
	if params == nil || params.TopicARN == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "TopicArn é obrigatório",
		}
	}
	if params.Message == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "Message é obrigatório",
		}
	}
	// Atributos contam no limite de 256 KiB junto com o body (igual AWS) —
	// sem isso seriam um bypass ilimitado do limite de tamanho.
	attrsSize := 0
	for name, v := range params.MessageAttributes {
		attrsSize += len(name) + len(v.DataType) + len(v.StringValue) + len(v.BinaryValue)
	}
	if len(params.Message)+attrsSize > 256*1024 {
		return nil, &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: fmt.Sprintf("Message + atributos excede 256 KiB (%d bytes)", len(params.Message)+attrsSize),
		}
	}

	topicName := extractTopicName(params.TopicARN)
	if topicName == "" {
		return nil, &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: fmt.Sprintf("TopicArn inválido: %q", params.TopicARN),
		}
	}

	msg := &types.Message{
		Body:              params.Message,
		MessageAttributes: protocol.MessageAttributeToTypes(params.MessageAttributes),
	}
	// Subject vai como MessageAttribute "Subject" (alguns clients esperam).
	if params.Subject != "" {
		if msg.MessageAttributes == nil {
			msg.MessageAttributes = make(map[string]types.MessageAttribute)
		}
		msg.MessageAttributes["Subject"] = types.MessageAttribute{
			DataType:    "String",
			StringValue: params.Subject,
		}
	}

	saved, err := s.storage.Topics().Publish(ctx, topicName, msg)
	if err != nil {
		return nil, err
	}
	return &PublishResult{MessageID: saved.ID}, nil
}

// PublishParamsFromQuery normaliza Query → PublishParams.
func PublishParamsFromQuery(params url.Values) *PublishParams {
	p := &PublishParams{
		TopicARN:          params.Get("TopicArn"),
		Message:           params.Get("Message"),
		Subject:           params.Get("Subject"),
		MessageAttributes: protocol.ExtractQueryMessageAttributes(params),
	}
	return p
}

// PublishParamsFromJSON normaliza JSON → PublishParams.
func PublishParamsFromJSON(params map[string]any) *PublishParams {
	p := &PublishParams{}
	if s, ok := params["TopicArn"].(string); ok {
		p.TopicARN = s
	}
	if s, ok := params["Message"].(string); ok {
		p.Message = s
	}
	if s, ok := params["Subject"].(string); ok {
		p.Subject = s
	}
	if s, ok := params["MessageStructure"].(string); ok {
		p.MessageStructure = s
	}
	p.MessageAttributes = protocol.ExtractJSONMessageAttributes(params)
	return p
}

// --- ConfirmSubscription (stub) ---

// ConfirmSubscriptionParams contém os parâmetros.
type ConfirmSubscriptionParams struct {
	TopicARN string
	Token    string
}

// ConfirmSubscription valida uma inscrição HTTP/HTTPS pendente.
//
// MVP: retorna UnsupportedOperation. Clientes que dependem deste fluxo
// devem usar SQS subscriptions (que são auto-confirmadas).
func (s *Service) ConfirmSubscription(ctx context.Context, params *ConfirmSubscriptionParams) (*SubscribeResult, error) {
	return nil, &AWSError{
		Code:    ErrCodeUnsupportedOperation,
		Message: "ConfirmSubscription não implementado no MVP (use subscription sqs que é auto-confirmada)",
	}
}

// ConfirmSubscriptionParamsFromQuery normaliza Query → ConfirmSubscriptionParams.
func ConfirmSubscriptionParamsFromQuery(params url.Values) *ConfirmSubscriptionParams {
	return &ConfirmSubscriptionParams{
		TopicARN: params.Get("TopicArn"),
		Token:    params.Get("Token"),
	}
}

// ConfirmSubscriptionParamsFromJSON normaliza JSON → ConfirmSubscriptionParams.
func ConfirmSubscriptionParamsFromJSON(params map[string]any) *ConfirmSubscriptionParams {
	p := &ConfirmSubscriptionParams{}
	if s, ok := params["TopicArn"].(string); ok {
		p.TopicARN = s
	}
	if s, ok := params["Token"].(string); ok {
		p.Token = s
	}
	return p
}

// --- helpers ---

// extractTopicName extrai o nome do tópico de um ARN SNS.
//
// Formato: arn:aws:sns:<region>:<account>:<name>
func extractTopicName(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) < 6 {
		return ""
	}
	return parts[5]
}

// extractTagsQuery extrai pares Tag.N.Key/Value do form-encoded SNS.
func extractTagsQuery(params url.Values) map[string]string {
	out := make(map[string]string)
	prefix := "Tags."
	for k, vs := range params {
		if !strings.HasPrefix(k, prefix) || len(vs) == 0 {
			continue
		}
		rest := k[len(prefix):]
		dot := strings.IndexByte(rest, '.')
		if dot < 0 {
			continue
		}
		idx := rest[:dot]
		field := rest[dot+1:]
		switch field {
		case "Key":
			name := vs[0]
			valueKey := prefix + idx + ".Value"
			if vvs, ok := params[valueKey]; ok && len(vvs) > 0 {
				out[name] = vvs[0]
			}
		}
	}
	return out
}

// isValidTopicName verifica regras de nomenclatura SNS.
//
// SNS aceita alfanuméricos, . _ -. 1-256 chars.
func isValidTopicName(name string) bool {
	if len(name) < 1 || len(name) > 256 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'A' && c <= 'Z',
			c >= 'a' && c <= 'z',
			c >= '0' && c <= '9',
			c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}

// isValidProtocol verifica se o protocolo é suportado.
func isValidProtocol(p string) bool {
	switch p {
	case "sqs", "http", "https", "email", "email-json", "sms", "application":
		return true
	}
	return false
}

// validateJSON valida que uma string é JSON válido e faz unmarshal em out.
func validateJSON(s string, out any) error {
	if s == "" {
		return nil
	}
	return json.Unmarshal([]byte(s), out)
}

// --- AWSError ---
//
// Pré-refactor (refactor/kiss-dry-pass-1): struct AWSError + Error() +
// IsSenderFault() + AsAWSError() existiam duplicados entre sns/service.go
// e sqs/service.go (~50 linhas cada). Agora AWSError = alias para
// awserr.Error; IsSenderFault e AsAWSError delegados ao pacote awserr.

// (Fim)
