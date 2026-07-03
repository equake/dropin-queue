// Package sqs implementa a lógica das operações SQS.
//
// Esta camada fica entre o protocol layer (parser/serializer AWS) e o
// storage (broker). Aqui moram:
//
//   - Validação semântica dos parâmetros
//   - Construção de ARNs e URLs
//   - Tradução de erros do storage para AWS error codes
//   - Integração com métricas
//
// Cada operação AWS corresponde a um método neste pacote.
// Por enquanto só CreateQueue está implementado; demais virão nas
// próximas semanas.
package sqs

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/protocol"
	"github.com/equake/dropin-queue/shim/internal/storage"
	"github.com/equake/dropin-queue/shim/pkg/types"
)

// AWSErrorCode são os códigos oficiais AWS SQS para erros que devolvemos.
const (
	// ErrCodeQueueAlreadyExists: tentativa de criar fila existente com
	// atributos diferentes.
	ErrCodeQueueAlreadyExists = "QueueAlreadyExists"

	// ErrCodeQueueDoesNotExist: operação em fila inexistente.
	ErrCodeQueueDoesNotExist = "QueueDoesNotExist"

	// ErrCodeInvalidParameterValue: parâmetro inválido (nome de fila,
	// atributo com valor fora do range, etc.).
	ErrCodeInvalidParameterValue = "InvalidParameterValue"

	// ErrCodeMissingParameter: parâmetro obrigatório ausente.
	ErrCodeMissingParameter = "MissingParameter"

	// ErrCodeOverLimit: limite de mensagens/filas excedido.
	ErrCodeOverLimit = "OverLimit"

	// ErrCodeUnsupportedOperation: operação não implementada pelo shim.
	ErrCodeUnsupportedOperation = "UnsupportedOperation"

	// ErrCodeInternalError: erro interno inesperado.
	ErrCodeInternalError = "InternalError"

	// ErrCodeMessageTooLarge: body da mensagem excede MaximumMessageSize.
	ErrCodeMessageTooLarge = "MessageTooLarge"

	// ErrCodeReceiptHandleIsInvalid: receipt handle malformado ou expirado.
	ErrCodeReceiptHandleIsInvalid = "ReceiptHandleIsInvalid"

	// ErrCodePurgeQueueInProgress: PurgeQueue já está rodando para essa fila
	// (SQS devolve isso se PurgeQueue for chamado em <60s após o anterior).
	ErrCodePurgeQueueInProgress = "PurgeQueueInProgress"

	// ErrCodeBatchEntryIdsNotDistinct: SendMessageBatch/DeleteMessageBatch com
	// Ids duplicados dentro do mesmo batch.
	ErrCodeBatchEntryIdsNotDistinct = "BatchEntryIdsNotDistinct"

	// ErrCodeTooManyEntriesInBatch: SendMessageBatch/DeleteMessageBatch com
	// mais de 10 entries.
	ErrCodeTooManyEntriesInBatch = "TooManyEntriesInBatch"

	// ErrCodeEmptyBatchRequest: SendMessageBatch/DeleteMessageBatch sem entries.
	ErrCodeEmptyBatchRequest = "EmptyBatchRequest"
)

// Service implementa as operações SQS.
//
// É goroutine-safe — os métodos delegam para storage que também é.
type Service struct {
	storage     storage.Storage
	accountID   string
	region      string
	endpointURL string // ex: "http://localhost:4566" — usado para construir QueueURL

	// agora é função injetada para permitir mock em testes de tempo.
	agora func() time.Time
}

// New cria um Service SQS.
//
// endpointURL: scheme+host+porta do shim (sem path). Ex: "http://localhost:4566".
func New(s storage.Storage, accountID, region, endpointURL string) *Service {
	return &Service{
		storage:     s,
		accountID:   accountID,
		region:      region,
		endpointURL: strings.TrimSuffix(endpointURL, "/"),
		agora:       time.Now,
	}
}

// SetNow injeta clock (usado em testes).
func (s *Service) SetNow(f func() time.Time) { s.agora = f }

// SQSActionResult é o envelope de resultado para SQS Query.
//
// Cada handler específico (CreateQueueResult, ListQueuesResult, etc.)
// satisfaz esta interface para serialização XML uniforme.
type SQSActionResult interface {
	// XMLName local deve ser a tag raiz do result.
	actionResultTag() string
}

// --- CreateQueue ---

// CreateQueueParams contém os parâmetros parseados de CreateQueue.
type CreateQueueParams struct {
	// QueueName: 1-80 chars alfanuméricos + dash + underscore.
	QueueName string

	// Attributes: chave=valor (VisibilityTimeout, MessageRetentionPeriod, etc.).
	Attributes map[string]string

	// Tags: chave=valor (opcional).
	Tags map[string]string
}

// CreateQueueResult é o resultado XML de CreateQueue.
type CreateQueueResult struct {
	QueueURL string `xml:"QueueUrl"`
}

func (CreateQueueResult) actionResultTag() string { return "" } // handled by encoder

// CreateQueue implementa a operação CreateQueue.
//
// Aceita parâmetros vindos do protocolo Query (form-encoded) ou JSON 1.0.
// O parâmetro `params` é um CreateQueueParams já normalizado (built pelo
// servidor ao detectar o protocolo); a camada service não conhece o wire format.
//
// Comportamento AWS:
//   - Se fila existe com mesmos atributos → idempotente, devolve a URL.
//   - Se fila existe com atributos diferentes → QueueAlreadyExists.
//   - Atributos não fornecidos → defaults SQS.
//
// Validações:
//   - QueueName: 1-80 chars, alfanuméricos + . _ -
//   - Atributos: nomes conhecidos, valores em range
//   - FIFO suffix .fifo se Attributes.FifoQueue=true
func (s *Service) CreateQueue(ctx context.Context, params *CreateQueueParams) (*types.Queue, error) {
	start := s.agora()

	if params == nil {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "params não pode ser nil",
		}
	}

	if params.QueueName == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "QueueName é obrigatório",
		}
	}
	if !isValidQueueName(params.QueueName) {
		return nil, &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: fmt.Sprintf("QueueName inválido: %q (1-80 chars, alfanuméricos, -, _, .)", params.QueueName),
		}
	}

	q := types.Queue{
		Name:      params.QueueName,
		AccountID: s.accountID,
		Region:    s.region,
		Attributes: types.QueueAttributes{
			VisibilityTimeout:             parseInt32Default(params.Attributes["VisibilityTimeout"], types.DefaultQueueAttributes().VisibilityTimeout),
			MessageRetentionPeriod:        parseInt32Default(params.Attributes["MessageRetentionPeriod"], types.DefaultQueueAttributes().MessageRetentionPeriod),
			MaximumMessageSize:            parseInt32Default(params.Attributes["MaximumMessageSize"], types.DefaultQueueAttributes().MaximumMessageSize),
			DelaySeconds:                  parseInt32Default(params.Attributes["DelaySeconds"], types.DefaultQueueAttributes().DelaySeconds),
			ReceiveMessageWaitTimeSeconds: parseInt32Default(params.Attributes["ReceiveMessageWaitTimeSeconds"], types.DefaultQueueAttributes().ReceiveMessageWaitTimeSeconds),
			ContentBasedDeduplication:     params.Attributes["ContentBasedDeduplication"] == "true",
		},
		Tags:      params.Tags,
		FIFO:      strings.HasSuffix(params.QueueName, ".fifo") || params.Attributes["FifoQueue"] == "true",
		CreatedAt: s.agora(),
	}

	// Valida ranges dos atributos.
	if err := validateAttributes(q.Attributes); err != nil {
		return nil, err
	}

	// Cria (ou recupera se já existe — idempotência SQS).
	created, err := s.storage.Queues().CreateQueue(ctx, q)
	if err != nil {
		return nil, &AWSError{
			Code:    ErrCodeInternalError,
			Message: err.Error(),
		}
	}

	// Preenche URL e ARN.
	created.URL = protocol.NewQueueURL(s.endpointURL, s.accountID, created.Name).String()
	created.ARN = protocol.NewSQSARN(s.region, s.accountID, created.Name).String()

	observability.L().Info("fila SQS criada",
		"queue", created.Name,
		"url", created.URL,
		"fifo", created.FIFO,
		"duration_ms", s.agora().Sub(start).Milliseconds(),
	)
	return created, nil
}

// CreateQueueParamsFromQuery normaliza parâmetros vindos do protocolo Query
// (form-encoded) para CreateQueueParams.
//
// Espera url.Values com chaves:
//   - "QueueName" → QueueName
//   - "Attribute.N.Name" + "Attribute.N.Value" → Attributes
//   - "Tag.N.Name" + "Tag.N.Value" → Tags
func CreateQueueParamsFromQuery(params url.Values) *CreateQueueParams {
	return &CreateQueueParams{
		QueueName:  params.Get("QueueName"),
		Attributes: extractAttributes(params, "Attribute"),
		Tags:       extractAttributes(params, "Tag"),
	}
}

// CreateQueueParamsFromJSON normaliza parâmetros vindos do protocolo JSON 1.0
// para CreateQueueParams.
//
// Aceita ambos formatos AWS JSON 1.0:
//   - Documentado: "Attribute": [{"Name":"VisibilityTimeout","Value":"30"}]
//   - Compacto boto3: "Attributes": {"VisibilityTimeout": "30"}
//
// Mesma coisa para Tag/Tags.
func CreateQueueParamsFromJSON(params map[string]any) *CreateQueueParams {
	p := &CreateQueueParams{}
	if s, ok := params["QueueName"].(string); ok {
		p.QueueName = s
	}
	p.Attributes = protocol.ExtractJSONAttributes(params, "Attribute", "Name")
	p.Tags = protocol.ExtractJSONAttributes(params, "Tag", "Key")
	return p
}

// --- GetQueueUrl ---

// GetQueueUrlParams contém os parâmetros de GetQueueUrl.
type GetQueueUrlParams struct {
	QueueName string
}

// GetQueueUrl devolve a URL canônica de uma fila pelo nome.
//
// Comportamento AWS:
//   - QueueName é obrigatório.
//   - QueueDoesNotExist se não existir.
//   - Caso exista, devolve URL no formato '<endpoint>/<account>/<name>'.
func (s *Service) GetQueueUrl(ctx context.Context, params *GetQueueUrlParams) (*types.Queue, error) {
	if params == nil || params.QueueName == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "QueueName é obrigatório",
		}
	}

	q, err := s.storage.Queues().GetQueue(ctx, params.QueueName)
	if err != nil {
		return nil, err // AsAWSError will translate ErrQueueNotFound
	}
	q.URL = protocol.NewQueueURL(s.endpointURL, s.accountID, q.Name).String()
	q.ARN = protocol.NewSQSARN(s.region, s.accountID, q.Name).String()
	return q, nil
}

// GetQueueUrlParamsFromQuery normaliza parâmetros Query para GetQueueUrl.
func GetQueueUrlParamsFromQuery(params url.Values) *GetQueueUrlParams {
	return &GetQueueUrlParams{
		QueueName: params.Get("QueueName"),
	}
}

// GetQueueUrlParamsFromJSON normaliza parâmetros JSON para GetQueueUrl.
func GetQueueUrlParamsFromJSON(params map[string]any) *GetQueueUrlParams {
	p := &GetQueueUrlParams{}
	if s, ok := params["QueueName"].(string); ok {
		p.QueueName = s
	}
	return p
}

// --- GetQueueAttributes ---

// GetQueueAttributesParams contém os parâmetros de GetQueueAttributes.
type GetQueueAttributesParams struct {
	QueueName      string
	AttributeNames []string // ["All"] ou lista de nomes específicos
}

// GetQueueAttributes devolve os atributos de uma fila.
//
// Se AttributeNames contém "All" ou está vazia, devolve todos os atributos
// suportados. Caso contrário, filtra pelos nomes fornecidos.
func (s *Service) GetQueueAttributes(ctx context.Context, params *GetQueueAttributesParams) (map[string]string, error) {
	if params == nil || params.QueueName == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "QueueName é obrigatório",
		}
	}

	q, err := s.storage.Queues().GetQueue(ctx, params.QueueName)
	if err != nil {
		return nil, err
	}

	// Aproxima depth — leitura best-effort do stream. Em erro, devolve 0.
	depth := "0"
	if d, derr := s.storage.Messages().QueueDepth(ctx, params.QueueName); derr == nil {
		depth = fmt.Sprintf("%d", d)
	}

	allAttrs := map[string]string{
		"VisibilityTimeout":                     fmt.Sprintf("%d", q.Attributes.VisibilityTimeout),
		"MessageRetentionPeriod":                fmt.Sprintf("%d", q.Attributes.MessageRetentionPeriod),
		"MaximumMessageSize":                    fmt.Sprintf("%d", q.Attributes.MaximumMessageSize),
		"DelaySeconds":                          fmt.Sprintf("%d", q.Attributes.DelaySeconds),
		"ReceiveMessageWaitTimeSeconds":         fmt.Sprintf("%d", q.Attributes.ReceiveMessageWaitTimeSeconds),
		"QueueArn":                              protocol.NewSQSARN(s.region, s.accountID, q.Name).String(),
		"ApproximateNumberOfMessages":           depth,
		"ApproximateNumberOfMessagesNotVisible": "0",
		"CreatedTimestamp":                      fmt.Sprintf("%d", q.CreatedAt.UnixMilli()),
	}

	// "All" ou vazio → tudo.
	wantAll := len(params.AttributeNames) == 0
	for _, n := range params.AttributeNames {
		if n == "All" {
			wantAll = true
			break
		}
	}
	if wantAll {
		return allAttrs, nil
	}

	// Filtra pelos nomes pedidos.
	out := make(map[string]string)
	for _, n := range params.AttributeNames {
		if v, ok := allAttrs[n]; ok {
			out[n] = v
		}
		// AWS devolve silenciosamente atributos não-conhecidos; ok.
	}
	return out, nil
}

// GetQueueAttributesParamsFromQuery normaliza Query → GetQueueAttributesParams.
func GetQueueAttributesParamsFromQuery(params url.Values) *GetQueueAttributesParams {
	p := &GetQueueAttributesParams{
		QueueName: params.Get("QueueName"),
	}
	// AttributeName.1, AttributeName.2, etc.
	for i := 1; ; i++ {
		key := fmt.Sprintf("AttributeName.%d", i)
		v, ok := params[key]
		if !ok || len(v) == 0 {
			break
		}
		p.AttributeNames = append(p.AttributeNames, v[0])
	}
	return p
}

// GetQueueAttributesParamsFromJSON normaliza JSON → GetQueueAttributesParams.
//
// boto3 envia QueueUrl (não QueueName) para GetQueueAttributes no JSON 1.0.
// Extraímos o nome do path da URL.
func GetQueueAttributesParamsFromJSON(params map[string]any) *GetQueueAttributesParams {
	p := &GetQueueAttributesParams{}
	if s, ok := params["QueueName"].(string); ok && s != "" {
		p.QueueName = s
	} else if s, ok := params["QueueUrl"].(string); ok && s != "" {
		p.QueueName = queueNameFromURL(s)
	}
	if raw, ok := params["AttributeName"]; ok {
		if arr, ok := raw.([]any); ok {
			for _, item := range arr {
				if s, ok := item.(string); ok {
					p.AttributeNames = append(p.AttributeNames, s)
				}
			}
		}
	}
	return p
}

// --- ListQueues ---

// ListQueuesParams contém os parâmetros de ListQueues.
type ListQueuesParams struct {
	QueueNamePrefix string
	MaxResults      int32
	NextToken       string
}

// ListQueuesResult é o resultado de ListQueues.
type ListQueuesResult struct {
	QueueUrls []string
	NextToken string
}

// ListQueues lista filas, opcionalmente filtradas por prefixo.
func (s *Service) ListQueues(ctx context.Context, params *ListQueuesParams) (*ListQueuesResult, error) {
	if params == nil {
		params = &ListQueuesParams{}
	}

	queues, err := s.storage.Queues().ListQueues(ctx, params.QueueNamePrefix)
	if err != nil {
		return nil, err
	}

	urls := make([]string, 0, len(queues))
	for _, q := range queues {
		q.URL = protocol.NewQueueURL(s.endpointURL, s.accountID, q.Name).String()
		urls = append(urls, q.URL)
	}

	// TODO: pagination com NextToken (Semana 3).
	return &ListQueuesResult{
		QueueUrls: urls,
	}, nil
}

// ListQueuesParamsFromQuery normaliza Query → ListQueuesParams.
func ListQueuesParamsFromQuery(params url.Values) *ListQueuesParams {
	return &ListQueuesParams{
		QueueNamePrefix: params.Get("QueueNamePrefix"),
		MaxResults:      parseInt32Default(params.Get("MaxResults"), 0),
		NextToken:       params.Get("NextToken"),
	}
}

// ListQueuesParamsFromJSON normaliza JSON → ListQueuesParams.
func ListQueuesParamsFromJSON(params map[string]any) *ListQueuesParams {
	p := &ListQueuesParams{}
	if s, ok := params["QueueNamePrefix"].(string); ok {
		p.QueueNamePrefix = s
	}
	if s, ok := params["NextToken"].(string); ok {
		p.NextToken = s
	}
	if f, ok := params["MaxResults"].(float64); ok {
		p.MaxResults = int32(f)
	}
	return p
}

// --- DeleteQueue ---

// DeleteQueueParams contém os parâmetros de DeleteQueue.
type DeleteQueueParams struct {
	QueueName string // pode vir como nome ou como QueueUrl; o server extrai
}

// DeleteQueue remove uma fila. Idempotente: não retorna erro se não existe.
func (s *Service) DeleteQueue(ctx context.Context, params *DeleteQueueParams) error {
	if params == nil || params.QueueName == "" {
		return &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "QueueName ou QueueUrl é obrigatório",
		}
	}
	return s.storage.Queues().DeleteQueue(ctx, params.QueueName)
}

// queueNameFromURL extrai o nome da fila da QueueURL.
//
// Formato: http(s)://endpoint/<account>/<name>
// Nome é o último segmento do path.
func queueNameFromURL(queueURL string) string {
	idx := strings.LastIndex(queueURL, "/")
	if idx < 0 || idx == len(queueURL)-1 {
		return queueURL
	}
	return queueURL[idx+1:]
}

// --- SendMessageBatch ---

// Limites AWS para SendMessageBatch / DeleteMessageBatch.
const (
	// MaxBatchEntries é o máximo de entries por batch (AWS limit).
	MaxBatchEntries = 10

	// MaxBatchTotalBytes é o tamanho máximo total dos bodies em um batch.
	// 256 KiB conforme AWS docs.
	MaxBatchTotalBytes = 256 * 1024

	// MaxMessageAttributes é o máximo de message attributes por mensagem
	// (limite AWS). Sem esse limite, atributos seriam um vetor de
	// amplificação de payload fora do controle de MaximumMessageSize.
	MaxMessageAttributes = 10
)

// messageAttributesSize soma o tamanho on-wire dos atributos (nome +
// DataType + valor). AWS conta os atributos DENTRO do limite de 256 KiB
// da mensagem — sem contar, atributos viram bypass ilimitado do limite.
func messageAttributesSize(attrs map[string]protocol.MessageAttributeValue) int {
	total := 0
	for name, v := range attrs {
		total += len(name) + len(v.DataType) + len(v.StringValue) + len(v.BinaryValue)
	}
	return total
}

// validateMessageAttributes valida a contagem de message attributes.
func validateMessageAttributes(attrs map[string]protocol.MessageAttributeValue) *AWSError {
	if len(attrs) > MaxMessageAttributes {
		return &AWSError{
			Code: ErrCodeInvalidParameterValue,
			Message: fmt.Sprintf("Number of message attributes [%d] exceeds the allowed maximum [%d]",
				len(attrs), MaxMessageAttributes),
		}
	}
	return nil
}

// BatchSendEntry é uma entry do SendMessageBatch.
//
// Formato unificado (não depende do protocolo wire).
type BatchSendEntry struct {
	Id                     string
	MessageBody            string
	DelaySeconds           int32
	MessageGroupId         string
	MessageDeduplicationId string
	MessageAttributes      map[string]protocol.MessageAttributeValue
}

// BatchDeleteEntry é uma entry do DeleteMessageBatch.
type BatchDeleteEntry struct {
	Id                string
	ReceiptHandle     string
	VisibilityTimeout int32 // 0 → delete direto; >0 → change visibility + delete (não é delete "real", use ChangeMessageVisibilityBatch — não implementado)
}

// BatchResultEntry representa uma entry que teve sucesso.
type BatchResultEntry struct {
	Id         string
	MessageID  string // SendMessageBatch: id da mensagem publicada
	MD5OfBody  string // SendMessageBatch: MD5 do body
	SequenceNo string // FIFO only (SendMessageBatch)
}

// BatchFailureEntry representa uma entry que falhou.
type BatchFailureEntry struct {
	Id          string
	Code        string // AWS error code (e.g. "MessageTooLarge", "ReceiptHandleIsInvalid")
	Message     string
	SenderFault bool
}

// SendMessageBatchParams contém os parâmetros do SendMessageBatch.
type SendMessageBatchParams struct {
	QueueName string
	QueueURL  string
	Entries   []BatchSendEntry
}

// SendMessageBatchResult contém entries bem-sucedidas e falhadas.
type SendMessageBatchResult struct {
	Successful []BatchResultEntry
	Failed     []BatchFailureEntry
}

// SendMessageBatch publica até 10 mensagens em uma fila em uma única chamada.
//
// Comportamento AWS:
//
//   - 1 ≤ len(Entries) ≤ 10.
//   - Ids únicos dentro do batch.
//   - Soma dos bodies ≤ 256 KiB.
//   - Filas FIFO: cada entry precisa de MessageGroupId, DelaySeconds=0.
//   - Partial success: falhas per-entry vão em Failed[] (BatchResultError code),
//     não falham o batch inteiro.
//
// Validações a nível de batch (retornam erro único, sem entries processadas):
//   - QueueName ausente
//   - 0 entries → EmptyBatchRequest
//   - >10 entries → TooManyEntriesInBatch
//   - Ids duplicados → BatchEntryIdsNotDistinct
//   - Soma bodies > 256 KiB → MessageTooLarge
func (s *Service) SendMessageBatch(ctx context.Context, params *SendMessageBatchParams) (*SendMessageBatchResult, error) {
	if params == nil || len(params.Entries) == 0 {
		return nil, &AWSError{
			Code:    ErrCodeEmptyBatchRequest,
			Message: "Entries deve conter ao menos 1 entry",
		}
	}
	if len(params.Entries) > MaxBatchEntries {
		return nil, &AWSError{
			Code:    ErrCodeTooManyEntriesInBatch,
			Message: fmt.Sprintf("Entries excede máximo de %d", MaxBatchEntries),
		}
	}

	if params.QueueName == "" && params.QueueURL != "" {
		params.QueueName = queueNameFromURL(params.QueueURL)
	}
	if params.QueueName == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "QueueUrl ou QueueName é obrigatório",
		}
	}

	// Valida Ids únicos dentro do batch.
	seenIds := make(map[string]bool, len(params.Entries))
	totalBodyBytes := 0
	for i, e := range params.Entries {
		if e.Id == "" {
			return nil, &AWSError{
				Code:    ErrCodeInvalidParameterValue,
				Message: fmt.Sprintf("Entries[%d].Id é obrigatório", i),
			}
		}
		if seenIds[e.Id] {
			return nil, &AWSError{
				Code:    ErrCodeBatchEntryIdsNotDistinct,
				Message: fmt.Sprintf("Entries contém Ids duplicados: %q", e.Id),
			}
		}
		seenIds[e.Id] = true
		if aerr := validateMessageAttributes(e.MessageAttributes); aerr != nil {
			return nil, &AWSError{
				Code:    aerr.Code,
				Message: fmt.Sprintf("Entries[%d]: %s", i, aerr.Message),
			}
		}
		// Atributos contam no limite de 256 KiB do batch (igual AWS).
		totalBodyBytes += len(e.MessageBody) + messageAttributesSize(e.MessageAttributes)
	}
	if totalBodyBytes > MaxBatchTotalBytes {
		return nil, &AWSError{
			Code:    ErrCodeMessageTooLarge,
			Message: fmt.Sprintf("Soma dos bodies excede %d bytes", MaxBatchTotalBytes),
		}
	}

	// Carrega fila para validar FIFO.
	q, err := s.storage.Queues().GetQueue(ctx, params.QueueName)
	if err != nil {
		return nil, err
	}

	result := &SendMessageBatchResult{
		Successful: make([]BatchResultEntry, 0, len(params.Entries)),
		Failed:     make([]BatchFailureEntry, 0),
	}

	for _, e := range params.Entries {
		// Validações per-entry que falham entries individuais (não o batch inteiro).
		if e.MessageBody == "" {
			result.Failed = append(result.Failed, BatchFailureEntry{
				Id:          e.Id,
				Code:        ErrCodeMissingParameter,
				Message:     "MessageBody é obrigatório",
				SenderFault: true,
			})
			continue
		}
		if e.DelaySeconds < 0 || e.DelaySeconds > 900 {
			result.Failed = append(result.Failed, BatchFailureEntry{
				Id:          e.Id,
				Code:        ErrCodeInvalidParameterValue,
				Message:     "DelaySeconds deve estar em [0, 900]",
				SenderFault: true,
			})
			continue
		}
		if q.FIFO {
			if e.DelaySeconds > 0 {
				result.Failed = append(result.Failed, BatchFailureEntry{
					Id:          e.Id,
					Code:        ErrCodeInvalidParameterValue,
					Message:     "DelaySeconds não é suportado em filas FIFO",
					SenderFault: true,
				})
				continue
			}
			if e.MessageGroupId == "" {
				result.Failed = append(result.Failed, BatchFailureEntry{
					Id:          e.Id,
					Code:        ErrCodeMissingParameter,
					Message:     "MessageGroupId é obrigatório em filas FIFO",
					SenderFault: true,
				})
				continue
			}
		}

		msg := &types.Message{
			Body:                   e.MessageBody,
			MessageAttributes:      maValueToTypes(e.MessageAttributes),
			MessageGroupId:         e.MessageGroupId,
			MessageDeduplicationId: e.MessageDeduplicationId,
		}

		saved, sendErr := s.storage.Messages().SendMessage(ctx, params.QueueName, msg)
		if sendErr != nil {
			awsErr := AsAWSError(sendErr)
			result.Failed = append(result.Failed, BatchFailureEntry{
				Id:          e.Id,
				Code:        awsErr.Code,
				Message:     awsErr.Message,
				SenderFault: awsErr.IsSenderFault(),
			})
			continue
		}

		entry := BatchResultEntry{
			Id:        e.Id,
			MessageID: saved.ID,
			MD5OfBody: saved.MD5OfBody,
		}
		if q.FIFO {
			entry.SequenceNo = saved.ID
		}
		result.Successful = append(result.Successful, entry)
	}

	return result, nil
}

// SendMessageBatchParamsFromQuery normaliza Query → SendMessageBatchParams.
//
// Extrai QueueUrl e SendMessageBatchRequestEntry.N.Id/MessageBody/etc.
func SendMessageBatchParamsFromQuery(params url.Values) *SendMessageBatchParams {
	p := &SendMessageBatchParams{
		QueueURL:  params.Get("QueueUrl"),
		QueueName: params.Get("QueueName"),
	}
	if p.QueueName == "" && p.QueueURL != "" {
		p.QueueName = queueNameFromURL(p.QueueURL)
	}

	entries := protocol.ExtractQuerySendMessageBatchEntries(params)
	p.Entries = make([]BatchSendEntry, 0, len(entries))
	for _, e := range entries {
		p.Entries = append(p.Entries, BatchSendEntry{
			Id:                     e.Id,
			MessageBody:            e.MessageBody,
			DelaySeconds:           e.DelaySeconds,
			MessageGroupId:         e.MessageGroupId,
			MessageDeduplicationId: e.MessageDeduplicationId,
			MessageAttributes:      e.MessageAttributes,
		})
	}
	return p
}

// SendMessageBatchParamsFromJSON normaliza JSON → SendMessageBatchParams.
func SendMessageBatchParamsFromJSON(params map[string]any) *SendMessageBatchParams {
	p := &SendMessageBatchParams{}
	if s, ok := params["QueueUrl"].(string); ok {
		p.QueueURL = s
		p.QueueName = queueNameFromURL(s)
	}
	if s, ok := params["QueueName"].(string); ok {
		p.QueueName = s
	}

	entries := protocol.ExtractJSONSendMessageBatchEntries(params)
	p.Entries = make([]BatchSendEntry, 0, len(entries))
	for _, e := range entries {
		p.Entries = append(p.Entries, BatchSendEntry{
			Id:                     e.Id,
			MessageBody:            e.MessageBody,
			DelaySeconds:           e.DelaySeconds,
			MessageGroupId:         e.MessageGroupId,
			MessageDeduplicationId: e.MessageDeduplicationId,
			MessageAttributes:      e.MessageAttributes,
		})
	}
	return p
}

// --- DeleteMessageBatch ---

// DeleteMessageBatchParams contém os parâmetros.
type DeleteMessageBatchParams struct {
	QueueName string
	QueueURL  string
	Entries   []BatchDeleteEntry
}

// DeleteMessageBatchResult contém entries bem-sucedidas e falhadas.
type DeleteMessageBatchResult struct {
	Successful []BatchResultEntry // só com Id preenchido
	Failed     []BatchFailureEntry
}

// DeleteMessageBatch remove até 10 mensagens em uma única chamada.
//
// Mesmas restrições de SendMessageBatch (10 entries, Ids únicos).
// Cada entry é deletada independentemente — falha em uma não afeta as outras.
func (s *Service) DeleteMessageBatch(ctx context.Context, params *DeleteMessageBatchParams) (*DeleteMessageBatchResult, error) {
	if params == nil || len(params.Entries) == 0 {
		return nil, &AWSError{
			Code:    ErrCodeEmptyBatchRequest,
			Message: "Entries deve conter ao menos 1 entry",
		}
	}
	if len(params.Entries) > MaxBatchEntries {
		return nil, &AWSError{
			Code:    ErrCodeTooManyEntriesInBatch,
			Message: fmt.Sprintf("Entries excede máximo de %d", MaxBatchEntries),
		}
	}

	if params.QueueName == "" && params.QueueURL != "" {
		params.QueueName = queueNameFromURL(params.QueueURL)
	}
	if params.QueueName == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "QueueUrl ou QueueName é obrigatório",
		}
	}

	seenIds := make(map[string]bool, len(params.Entries))
	for i, e := range params.Entries {
		if e.Id == "" {
			return nil, &AWSError{
				Code:    ErrCodeInvalidParameterValue,
				Message: fmt.Sprintf("Entries[%d].Id é obrigatório", i),
			}
		}
		if seenIds[e.Id] {
			return nil, &AWSError{
				Code:    ErrCodeBatchEntryIdsNotDistinct,
				Message: fmt.Sprintf("Entries contém Ids duplicados: %q", e.Id),
			}
		}
		seenIds[e.Id] = true
	}

	// Valida que a fila existe — caso contrário storage.DeleteMessage iria
	// falhar per-entry com ReceiptHandleIsInvalid, que é misleading.
	if _, err := s.storage.Queues().GetQueue(ctx, params.QueueName); err != nil {
		return nil, err
	}

	result := &DeleteMessageBatchResult{
		Successful: make([]BatchResultEntry, 0, len(params.Entries)),
		Failed:     make([]BatchFailureEntry, 0),
	}

	for _, e := range params.Entries {
		if e.ReceiptHandle == "" {
			result.Failed = append(result.Failed, BatchFailureEntry{
				Id:          e.Id,
				Code:        ErrCodeMissingParameter,
				Message:     "ReceiptHandle é obrigatório",
				SenderFault: true,
			})
			continue
		}

		if err := s.storage.Messages().DeleteMessage(ctx, params.QueueName, e.ReceiptHandle); err != nil {
			awsErr := AsAWSError(err)
			result.Failed = append(result.Failed, BatchFailureEntry{
				Id:          e.Id,
				Code:        awsErr.Code,
				Message:     awsErr.Message,
				SenderFault: awsErr.IsSenderFault(),
			})
			continue
		}

		result.Successful = append(result.Successful, BatchResultEntry{Id: e.Id})
	}

	return result, nil
}

// DeleteMessageBatchParamsFromQuery normaliza Query → DeleteMessageBatchParams.
func DeleteMessageBatchParamsFromQuery(params url.Values) *DeleteMessageBatchParams {
	p := &DeleteMessageBatchParams{
		QueueURL:  params.Get("QueueUrl"),
		QueueName: params.Get("QueueName"),
	}
	if p.QueueName == "" && p.QueueURL != "" {
		p.QueueName = queueNameFromURL(p.QueueURL)
	}

	entries := protocol.ExtractQueryDeleteMessageBatchEntries(params)
	p.Entries = make([]BatchDeleteEntry, 0, len(entries))
	for _, e := range entries {
		p.Entries = append(p.Entries, BatchDeleteEntry{
			Id:                e.Id,
			ReceiptHandle:     e.ReceiptHandle,
			VisibilityTimeout: e.VisibilityTimeout,
		})
	}
	return p
}

// DeleteMessageBatchParamsFromJSON normaliza JSON → DeleteMessageBatchParams.
func DeleteMessageBatchParamsFromJSON(params map[string]any) *DeleteMessageBatchParams {
	p := &DeleteMessageBatchParams{}
	if s, ok := params["QueueUrl"].(string); ok {
		p.QueueURL = s
		p.QueueName = queueNameFromURL(s)
	}
	if s, ok := params["QueueName"].(string); ok {
		p.QueueName = s
	}

	entries := protocol.ExtractJSONDeleteMessageBatchEntries(params)
	p.Entries = make([]BatchDeleteEntry, 0, len(entries))
	for _, e := range entries {
		p.Entries = append(p.Entries, BatchDeleteEntry{
			Id:                e.Id,
			ReceiptHandle:     e.ReceiptHandle,
			VisibilityTimeout: e.VisibilityTimeout,
		})
	}
	return p
}

// --- SendMessage ---

// SendMessageParams contém os parâmetros de SendMessage.
type SendMessageParams struct {
	QueueName               string
	QueueURL                string // alternativa a QueueName
	Body                    string
	DelaySeconds            int32
	MessageAttributes       map[string]protocol.MessageAttributeValue
	MessageSystemAttributes map[string]string
	MessageGroupId          string // FIFO only
	MessageDeduplicationId  string // FIFO only
}

// SendMessageResult é o resultado de SendMessage (Query ou JSON).
type SendMessageResult struct {
	MessageID  string
	MD5OfBody  string
	SequenceNo string // FIFO only — JetStream Sequence.Stream como string
}

// SendMessage publica uma mensagem em uma fila.
//
// Comportamento AWS:
//
//   - Idempotência FIFO via MessageDeduplicationId (Nats-Msg-Id nativo).
//   - Validação de tamanho (MaximumMessageSize da fila).
//   - DelaySeconds 0-900 (Standard only; FIFO rejeita).
func (s *Service) SendMessage(ctx context.Context, params *SendMessageParams) (*SendMessageResult, error) {
	if params == nil || params.Body == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "MessageBody é obrigatório",
		}
	}

	// Resolve QueueName.
	if params.QueueName == "" && params.QueueURL != "" {
		params.QueueName = queueNameFromURL(params.QueueURL)
	}
	if params.QueueName == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "QueueUrl ou QueueName é obrigatório",
		}
	}

	// Valida DelaySeconds range.
	if params.DelaySeconds < 0 || params.DelaySeconds > 900 {
		return nil, &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: "DelaySeconds deve estar em [0, 900]",
		}
	}

	// Valida MessageAttributes (máx 10, tamanho conta no limite).
	if aerr := validateMessageAttributes(params.MessageAttributes); aerr != nil {
		return nil, aerr
	}

	// Carrega fila para validar FIFO + DelaySeconds.
	q, err := s.storage.Queues().GetQueue(ctx, params.QueueName)
	if err != nil {
		return nil, err
	}

	// Tamanho total (body + atributos) contra o MaximumMessageSize da
	// fila — igual AWS. O storage revalida só o body; atributos são
	// verificados aqui, onde o formato wire é conhecido.
	maxSize := int(q.Attributes.MaximumMessageSize)
	if maxSize == 0 {
		maxSize = MaxBatchTotalBytes
	}
	if len(params.Body)+messageAttributesSize(params.MessageAttributes) > maxSize {
		return nil, &AWSError{
			Code:    ErrCodeMessageTooLarge,
			Message: fmt.Sprintf("Mensagem (body + atributos) excede %d bytes", maxSize),
		}
	}
	if q.FIFO {
		if params.DelaySeconds > 0 {
			return nil, &AWSError{
				Code:    ErrCodeInvalidParameterValue,
				Message: "DelaySeconds não é suportado em filas FIFO",
			}
		}
		if params.MessageGroupId == "" {
			return nil, &AWSError{
				Code:    ErrCodeMissingParameter,
				Message: "MessageGroupId é obrigatório em filas FIFO",
			}
		}
	}

	msg := &types.Message{
		Body:                   params.Body,
		MessageAttributes:      maValueToTypes(params.MessageAttributes),
		MessageGroupId:         params.MessageGroupId,
		MessageDeduplicationId: params.MessageDeduplicationId,
	}

	saved, err := s.storage.Messages().SendMessage(ctx, params.QueueName, msg)
	if err != nil {
		return nil, err
	}

	res := &SendMessageResult{
		MessageID: saved.ID,
		MD5OfBody: saved.MD5OfBody,
	}
	if q.FIFO {
		res.SequenceNo = saved.ID // Sequence.Stream é o ID da mensagem
	}
	return res, nil
}

// SendMessageParamsFromQuery normaliza Query → SendMessageParams.
//
// Extrai:
//   - QueueUrl (preferred em Query)
//   - MessageBody
//   - DelaySeconds
//   - MessageAttribute.N.* (Name, DataType, StringValue, BinaryValue, StringListValue.M)
//   - MessageGroupId, MessageDeduplicationId
func SendMessageParamsFromQuery(params url.Values) *SendMessageParams {
	p := &SendMessageParams{
		QueueURL:               params.Get("QueueUrl"),
		QueueName:              params.Get("QueueName"),
		Body:                   params.Get("MessageBody"),
		DelaySeconds:           parseInt32Default(params.Get("DelaySeconds"), 0),
		MessageAttributes:      protocol.ExtractQueryMessageAttributes(params),
		MessageGroupId:         params.Get("MessageGroupId"),
		MessageDeduplicationId: params.Get("MessageDeduplicationId"),
	}
	if p.QueueName == "" && p.QueueURL != "" {
		p.QueueName = queueNameFromURL(p.QueueURL)
	}
	return p
}

// SendMessageParamsFromJSON normaliza JSON → SendMessageParams.
func SendMessageParamsFromJSON(params map[string]any) *SendMessageParams {
	p := &SendMessageParams{}
	if s, ok := params["QueueUrl"].(string); ok {
		p.QueueURL = s
		p.QueueName = queueNameFromURL(s)
	}
	if s, ok := params["QueueName"].(string); ok {
		p.QueueName = s
	}
	if s, ok := params["MessageBody"].(string); ok {
		p.Body = s
	}
	if f, ok := params["DelaySeconds"].(float64); ok {
		p.DelaySeconds = int32(f)
	}
	if s, ok := params["MessageGroupId"].(string); ok {
		p.MessageGroupId = s
	}
	if s, ok := params["MessageDeduplicationId"].(string); ok {
		p.MessageDeduplicationId = s
	}
	p.MessageAttributes = protocol.ExtractJSONMessageAttributes(params)
	return p
}

// maValueToTypes converte map[string]protocol.MessageAttributeValue →
// map[string]types.MessageAttribute.
func maValueToTypes(in map[string]protocol.MessageAttributeValue) map[string]types.MessageAttribute {
	if in == nil {
		return nil
	}
	out := make(map[string]types.MessageAttribute, len(in))
	for k, v := range in {
		out[k] = types.MessageAttribute{
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}
	return out
}

// --- ReceiveMessage ---

// ReceiveMessageParams contém os parâmetros de ReceiveMessage.
type ReceiveMessageParams struct {
	QueueName               string
	QueueURL                string
	MaxNumberOfMessages     int32
	VisibilityTimeout       int32    // 0 → default da fila
	WaitTimeSeconds         int32    // 0-20; 0 = short-polling
	ReceiveRequestAttemptId string   // idempotência (Semana 4)
	AttributeNames          []string // "All" | ["SentTimestamp", "ApproximateReceiveCount", ...]
	MessageAttributeNames   []string // "All" | ["foo", "bar", ...]
}

// ReceiveMessageResult contém mensagens devolvidas + estado da chamada.
type ReceiveMessageResult struct {
	Messages []types.Message
}

// ReceiveMessage consome mensagens com long-polling.
//
// Comportamento AWS:
//
//   - VisibilityTimeout 0 → usa default da fila (30s SQS).
//   - WaitTimeSeconds 0-20; 0 = poll curto.
//   - MaxNumberOfMessages 1-10 (clamp).
//   - System attributes (SentTimestamp etc.) sempre incluídos; extras via
//     AttributeNames.
//   - MessageAttributeNames: ["All"] ou lista explícita.
//
// Sem mensagens: retorna Messages vazio (não erro).
func (s *Service) ReceiveMessage(ctx context.Context, params *ReceiveMessageParams) (*ReceiveMessageResult, error) {
	if params == nil {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "params é nil",
		}
	}

	// Resolve QueueName.
	if params.QueueName == "" && params.QueueURL != "" {
		params.QueueName = queueNameFromURL(params.QueueURL)
	}
	if params.QueueName == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "QueueUrl ou QueueName é obrigatório",
		}
	}

	// Validação de ranges.
	if params.MaxNumberOfMessages < 0 {
		return nil, &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: "MaxNumberOfMessages não pode ser negativo",
		}
	}
	if params.MaxNumberOfMessages == 0 {
		params.MaxNumberOfMessages = 1
	}
	if params.MaxNumberOfMessages > 10 {
		params.MaxNumberOfMessages = 10
	}
	if params.WaitTimeSeconds < 0 || params.WaitTimeSeconds > 20 {
		return nil, &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: "WaitTimeSeconds deve estar em [0, 20]",
		}
	}
	if params.VisibilityTimeout < 0 || params.VisibilityTimeout > 43200 {
		return nil, &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: "VisibilityTimeout deve estar em [0, 43200]",
		}
	}

	msgs, err := s.storage.Messages().ReceiveMessage(
		ctx,
		params.QueueName,
		params.MaxNumberOfMessages,
		params.WaitTimeSeconds,
		params.VisibilityTimeout,
	)
	if err != nil {
		return nil, err
	}

	// Filtra System Attributes conforme AttributeNames.
	wantAllSys := len(params.AttributeNames) == 0
	for _, n := range params.AttributeNames {
		if n == "All" {
			wantAllSys = true
			break
		}
	}
	// Filtra MessageAttributes conforme MessageAttributeNames.
	wantAllMA := len(params.MessageAttributeNames) == 0
	for _, n := range params.MessageAttributeNames {
		if n == "All" {
			wantAllMA = true
			break
		}
	}

	out := make([]types.Message, 0, len(msgs))
	for _, m := range msgs {
		if !wantAllSys {
			filtered := make(map[string]string)
			for _, n := range params.AttributeNames {
				if v, ok := m.Attributes[n]; ok {
					filtered[n] = v
				}
			}
			m.Attributes = filtered
		}
		if !wantAllMA {
			filtered := make(map[string]types.MessageAttribute)
			for _, n := range params.MessageAttributeNames {
				if v, ok := m.MessageAttributes[n]; ok {
					filtered[n] = v
				}
			}
			m.MessageAttributes = filtered
		}
		out = append(out, m)
	}
	return &ReceiveMessageResult{Messages: out}, nil
}

// ReceiveMessageParamsFromQuery normaliza Query → ReceiveMessageParams.
func ReceiveMessageParamsFromQuery(params url.Values) *ReceiveMessageParams {
	p := &ReceiveMessageParams{
		QueueURL:                params.Get("QueueUrl"),
		QueueName:               params.Get("QueueName"),
		MaxNumberOfMessages:     parseInt32Default(params.Get("MaxNumberOfMessages"), 1),
		VisibilityTimeout:       parseInt32Default(params.Get("VisibilityTimeout"), 0),
		WaitTimeSeconds:         parseInt32Default(params.Get("WaitTimeSeconds"), 0),
		ReceiveRequestAttemptId: params.Get("ReceiveRequestAttemptId"),
	}
	if p.QueueName == "" && p.QueueURL != "" {
		p.QueueName = queueNameFromURL(p.QueueURL)
	}

	// AttributeName.N e MessageAttributeName.N
	for i := 1; ; i++ {
		key := fmt.Sprintf("AttributeName.%d", i)
		if v, ok := params[key]; ok && len(v) > 0 {
			p.AttributeNames = append(p.AttributeNames, v[0])
		} else {
			break
		}
	}
	for i := 1; ; i++ {
		key := fmt.Sprintf("MessageAttributeName.%d", i)
		if v, ok := params[key]; ok && len(v) > 0 {
			p.MessageAttributeNames = append(p.MessageAttributeNames, v[0])
		} else {
			break
		}
	}
	return p
}

// ReceiveMessageParamsFromJSON normaliza JSON → ReceiveMessageParams.
func ReceiveMessageParamsFromJSON(params map[string]any) *ReceiveMessageParams {
	p := &ReceiveMessageParams{}
	if s, ok := params["QueueUrl"].(string); ok {
		p.QueueURL = s
		p.QueueName = queueNameFromURL(s)
	}
	if s, ok := params["QueueName"].(string); ok {
		p.QueueName = s
	}
	if f, ok := params["MaxNumberOfMessages"].(float64); ok {
		p.MaxNumberOfMessages = int32(f)
	}
	if f, ok := params["VisibilityTimeout"].(float64); ok {
		p.VisibilityTimeout = int32(f)
	}
	if f, ok := params["WaitTimeSeconds"].(float64); ok {
		p.WaitTimeSeconds = int32(f)
	}
	if s, ok := params["ReceiveRequestAttemptId"].(string); ok {
		p.ReceiveRequestAttemptId = s
	}
	if raw, ok := params["AttributeName"]; ok {
		if arr, ok := raw.([]any); ok {
			for _, item := range arr {
				if s, ok := item.(string); ok {
					p.AttributeNames = append(p.AttributeNames, s)
				}
			}
		}
	}
	if raw, ok := params["MessageAttributeName"]; ok {
		if arr, ok := raw.([]any); ok {
			for _, item := range arr {
				if s, ok := item.(string); ok {
					p.MessageAttributeNames = append(p.MessageAttributeNames, s)
				}
			}
		}
	}
	if p.MaxNumberOfMessages == 0 {
		p.MaxNumberOfMessages = 1
	}
	return p
}

// --- DeleteMessage ---

// DeleteMessageParams contém os parâmetros de DeleteMessage.
type DeleteMessageParams struct {
	QueueName     string
	QueueURL      string
	ReceiptHandle string
}

// DeleteMessage remove uma mensagem (ack).
//
// Comportamento AWS:
//
//   - Idempotente: deletar 2x é OK.
//   - Receipt handle expirado: ReceiptHandleIsInvalid.
func (s *Service) DeleteMessage(ctx context.Context, params *DeleteMessageParams) error {
	if params == nil || params.ReceiptHandle == "" {
		return &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "ReceiptHandle é obrigatório",
		}
	}
	if params.QueueName == "" && params.QueueURL != "" {
		params.QueueName = queueNameFromURL(params.QueueURL)
	}
	if params.QueueName == "" {
		return &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "QueueUrl ou QueueName é obrigatório",
		}
	}
	return s.storage.Messages().DeleteMessage(ctx, params.QueueName, params.ReceiptHandle)
}

// DeleteMessageParamsFromQuery normaliza Query → DeleteMessageParams.
func DeleteMessageParamsFromQuery(params url.Values) *DeleteMessageParams {
	p := &DeleteMessageParams{
		QueueName:     params.Get("QueueName"),
		QueueURL:      params.Get("QueueUrl"),
		ReceiptHandle: params.Get("ReceiptHandle"),
	}
	if p.QueueName == "" && p.QueueURL != "" {
		p.QueueName = queueNameFromURL(p.QueueURL)
	}
	return p
}

// DeleteMessageParamsFromJSON normaliza JSON → DeleteMessageParams.
func DeleteMessageParamsFromJSON(params map[string]any) *DeleteMessageParams {
	p := &DeleteMessageParams{}
	if s, ok := params["QueueUrl"].(string); ok {
		p.QueueURL = s
		p.QueueName = queueNameFromURL(s)
	}
	if s, ok := params["QueueName"].(string); ok {
		p.QueueName = s
	}
	if s, ok := params["ReceiptHandle"].(string); ok {
		p.ReceiptHandle = s
	}
	return p
}

// --- ChangeMessageVisibility ---

// ChangeMessageVisibilityParams contém os parâmetros.
type ChangeMessageVisibilityParams struct {
	QueueName         string
	QueueURL          string
	ReceiptHandle     string
	VisibilityTimeout int32
}

// ChangeMessageVisibility redefine visibility timeout de uma mensagem.
func (s *Service) ChangeMessageVisibility(ctx context.Context, params *ChangeMessageVisibilityParams) error {
	if params == nil || params.ReceiptHandle == "" {
		return &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "ReceiptHandle é obrigatório",
		}
	}
	if params.VisibilityTimeout < 0 || params.VisibilityTimeout > 43200 {
		return &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: "VisibilityTimeout deve estar em [0, 43200]",
		}
	}
	if params.QueueName == "" && params.QueueURL != "" {
		params.QueueName = queueNameFromURL(params.QueueURL)
	}
	if params.QueueName == "" {
		return &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "QueueUrl ou QueueName é obrigatório",
		}
	}
	return s.storage.Messages().ChangeMessageVisibility(ctx, params.QueueName, params.ReceiptHandle, params.VisibilityTimeout)
}

// ChangeMessageVisibilityParamsFromQuery normaliza Query → ChangeMessageVisibilityParams.
func ChangeMessageVisibilityParamsFromQuery(params url.Values) *ChangeMessageVisibilityParams {
	p := &ChangeMessageVisibilityParams{
		QueueName:         params.Get("QueueName"),
		QueueURL:          params.Get("QueueUrl"),
		ReceiptHandle:     params.Get("ReceiptHandle"),
		VisibilityTimeout: parseInt32Default(params.Get("VisibilityTimeout"), 0),
	}
	if p.QueueName == "" && p.QueueURL != "" {
		p.QueueName = queueNameFromURL(p.QueueURL)
	}
	return p
}

// ChangeMessageVisibilityParamsFromJSON normaliza JSON → ChangeMessageVisibilityParams.
func ChangeMessageVisibilityParamsFromJSON(params map[string]any) *ChangeMessageVisibilityParams {
	p := &ChangeMessageVisibilityParams{}
	if s, ok := params["QueueUrl"].(string); ok {
		p.QueueURL = s
		p.QueueName = queueNameFromURL(s)
	}
	if s, ok := params["QueueName"].(string); ok {
		p.QueueName = s
	}
	if s, ok := params["ReceiptHandle"].(string); ok {
		p.ReceiptHandle = s
	}
	if f, ok := params["VisibilityTimeout"].(float64); ok {
		p.VisibilityTimeout = int32(f)
	}
	return p
}

// --- PurgeQueue ---

// PurgeQueueParams contém os parâmetros.
type PurgeQueueParams struct {
	QueueName string
	QueueURL  string
}

// PurgeQueue esvazia a fila sem removê-la.
//
// Comportamento AWS: devolve sucesso sempre (mesmo se fila vazia).
// SQS devolve PurgeQueueInProgress se chamado em <60s após o anterior;
// não implementamos essa limitação no MVP.
func (s *Service) PurgeQueue(ctx context.Context, params *PurgeQueueParams) error {
	if params == nil {
		return &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "params é nil",
		}
	}
	if params.QueueName == "" && params.QueueURL != "" {
		params.QueueName = queueNameFromURL(params.QueueURL)
	}
	if params.QueueName == "" {
		return &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "QueueUrl ou QueueName é obrigatório",
		}
	}
	return s.storage.Queues().PurgeQueue(ctx, params.QueueName)
}

// PurgeQueueParamsFromQuery normaliza Query → PurgeQueueParams.
func PurgeQueueParamsFromQuery(params url.Values) *PurgeQueueParams {
	p := &PurgeQueueParams{
		QueueName: params.Get("QueueName"),
		QueueURL:  params.Get("QueueUrl"),
	}
	if p.QueueName == "" && p.QueueURL != "" {
		p.QueueName = queueNameFromURL(p.QueueURL)
	}
	return p
}

// PurgeQueueParamsFromJSON normaliza JSON → PurgeQueueParams.
func PurgeQueueParamsFromJSON(params map[string]any) *PurgeQueueParams {
	p := &PurgeQueueParams{}
	if s, ok := params["QueueUrl"].(string); ok {
		p.QueueURL = s
		p.QueueName = queueNameFromURL(s)
	}
	if s, ok := params["QueueName"].(string); ok {
		p.QueueName = s
	}
	return p
}

// --- SetQueueAttributes ---

// SetQueueAttributesParams contém os parâmetros.
type SetQueueAttributesParams struct {
	QueueName  string
	QueueURL   string
	Attributes map[string]string
}

// SetQueueAttributes atualiza atributos de uma fila existente.
func (s *Service) SetQueueAttributes(ctx context.Context, params *SetQueueAttributesParams) error {
	if params == nil || len(params.Attributes) == 0 {
		return &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "Attribute.N.Name/Value é obrigatório",
		}
	}
	if params.QueueName == "" && params.QueueURL != "" {
		params.QueueName = queueNameFromURL(params.QueueURL)
	}
	if params.QueueName == "" {
		return &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "QueueUrl ou QueueName é obrigatório",
		}
	}

	attrs := types.QueueAttributes{
		VisibilityTimeout:             parseInt32Default(params.Attributes["VisibilityTimeout"], 0),
		MessageRetentionPeriod:        parseInt32Default(params.Attributes["MessageRetentionPeriod"], 0),
		MaximumMessageSize:            parseInt32Default(params.Attributes["MaximumMessageSize"], 0),
		DelaySeconds:                  parseInt32Default(params.Attributes["DelaySeconds"], 0),
		ReceiveMessageWaitTimeSeconds: parseInt32Default(params.Attributes["ReceiveMessageWaitTimeSeconds"], 0),
		ContentBasedDeduplication:     params.Attributes["ContentBasedDeduplication"] == "true",
	}
	if err := validateAttributes(attrs); err != nil {
		return err
	}
	return s.storage.Queues().SetQueueAttributes(ctx, params.QueueName, attrs)
}

// SetQueueAttributesParamsFromQuery normaliza Query → SetQueueAttributesParams.
func SetQueueAttributesParamsFromQuery(params url.Values) *SetQueueAttributesParams {
	p := &SetQueueAttributesParams{
		QueueName:  params.Get("QueueName"),
		QueueURL:   params.Get("QueueUrl"),
		Attributes: extractAttributes(params, "Attribute"),
	}
	if p.QueueName == "" && p.QueueURL != "" {
		p.QueueName = queueNameFromURL(p.QueueURL)
	}
	return p
}

// SetQueueAttributesParamsFromJSON normaliza JSON → SetQueueAttributesParams.
func SetQueueAttributesParamsFromJSON(params map[string]any) *SetQueueAttributesParams {
	p := &SetQueueAttributesParams{}
	if s, ok := params["QueueUrl"].(string); ok {
		p.QueueURL = s
		p.QueueName = queueNameFromURL(s)
	}
	if s, ok := params["QueueName"].(string); ok {
		p.QueueName = s
	}
	p.Attributes = protocol.ExtractJSONAttributes(params, "Attribute", "Name")
	return p
}

// DeleteQueueParamsFromQuery normaliza Query → DeleteQueueParams.
// Aceita QueueName OU QueueUrl (o AWS aceita ambos).
func DeleteQueueParamsFromQuery(params url.Values) *DeleteQueueParams {
	name := params.Get("QueueName")
	if name == "" {
		name = queueNameFromURL(params.Get("QueueUrl"))
	}
	return &DeleteQueueParams{QueueName: name}
}

// DeleteQueueParamsFromJSON normaliza JSON → DeleteQueueParams.
func DeleteQueueParamsFromJSON(params map[string]any) *DeleteQueueParams {
	p := &DeleteQueueParams{}
	if s, ok := params["QueueName"].(string); ok && s != "" {
		p.QueueName = s
	} else if s, ok := params["QueueUrl"].(string); ok && s != "" {
		p.QueueName = queueNameFromURL(s)
	}
	return p
}

// extractAttributes extrai pares N.Name=..., N.Value=... das form values.
//
// AWS SQS Query usa:
//
//	Attribute.1.Name=VisibilityTimeout&Attribute.1.Value=30
//	Attribute.2.Name=MaximumMessageSize&Attribute.2.Value=262144
func extractAttributes(params url.Values, key string) map[string]string {
	out := make(map[string]string)
	for k, vs := range params {
		prefix := key + "."
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := k[len(prefix):]
		// rest = "1.Name" ou "1.Value"
		idx := strings.IndexByte(rest, '.')
		if idx < 0 {
			continue
		}
		field := rest[idx+1:]
		if field == "Name" && len(vs) > 0 {
			name := vs[0]
			// O valor correspondente está em <name>.Value na mesma posição.
			// Como url.Values é map, precisamos buscar valueKey = "Attribute.<idx>.Value".
			valueKey := key + "." + rest[:idx] + ".Value"
			if vvs, ok := params[valueKey]; ok && len(vvs) > 0 {
				out[name] = vvs[0]
			}
		}
	}
	return out
}

// isValidQueueName verifica se o nome segue as regras SQS.
//
// Regras:
//   - 1 a 80 caracteres
//   - alphanumeric + hyphen (-) + underscore (_)
//   - .fifo suffix para filas FIFO
func isValidQueueName(name string) bool {
	if len(name) < 1 || len(name) > 80 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'A' && c <= 'Z',
			c >= 'a' && c <= 'z',
			c >= '0' && c <= '9',
			c == '-', c == '_', c == '.':
			// ok
		default:
			return false
		}
	}
	// .fifo só pode ter no final
	if strings.Contains(name, ".fifo") && !strings.HasSuffix(name, ".fifo") {
		return false
	}
	return true
}

// validateAttributes verifica ranges dos atributos conforme AWS SQS docs.
func validateAttributes(a types.QueueAttributes) error {
	if a.VisibilityTimeout < 0 || a.VisibilityTimeout > 43200 {
		return &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: "VisibilityTimeout deve estar em [0, 43200]",
		}
	}
	if a.MessageRetentionPeriod != 0 && (a.MessageRetentionPeriod < 60 || a.MessageRetentionPeriod > 1209600) {
		return &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: "MessageRetentionPeriod deve estar em [60, 1209600]",
		}
	}
	if a.MaximumMessageSize != 0 && (a.MaximumMessageSize < 1024 || a.MaximumMessageSize > 262144) {
		return &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: "MaximumMessageSize deve estar em [1024, 262144]",
		}
	}
	if a.DelaySeconds < 0 || a.DelaySeconds > 900 {
		return &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: "DelaySeconds deve estar em [0, 900]",
		}
	}
	if a.ReceiveMessageWaitTimeSeconds < 0 || a.ReceiveMessageWaitTimeSeconds > 20 {
		return &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: "ReceiveMessageWaitTimeSeconds deve estar em [0, 20]",
		}
	}
	return nil
}

func parseInt32Default(s string, def int32) int32 {
	if s == "" {
		return def
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return def
	}
	return int32(n)
}

// AWSError representa um erro com código oficial AWS.
type AWSError struct {
	Code    string
	Message string
}

// Error implementa error.
func (e *AWSError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// IsSenderFault retorna true se o erro é culpa do cliente (4xx-style).
func (e *AWSError) IsSenderFault() bool {
	switch e.Code {
	case ErrCodeQueueAlreadyExists, ErrCodeQueueDoesNotExist,
		ErrCodeInvalidParameterValue, ErrCodeMissingParameter,
		ErrCodeOverLimit, ErrCodeMessageTooLarge,
		ErrCodeBatchEntryIdsNotDistinct, ErrCodeTooManyEntriesInBatch,
		ErrCodeEmptyBatchRequest:
		return true
	}
	return false
}

// AsAWSError tenta extrair um AWSError de um erro genérico.
// Se não conseguir, cria um AWSError InternalError.
func AsAWSError(err error) *AWSError {
	if err == nil {
		return nil
	}
	var awsErr *AWSError
	if errors.As(err, &awsErr) {
		return awsErr
	}
	if errors.Is(err, storage.ErrQueueNotFound) {
		return &AWSError{Code: ErrCodeQueueDoesNotExist, Message: err.Error()}
	}
	if errors.Is(err, storage.ErrQueueAlreadyExists) {
		return &AWSError{Code: ErrCodeQueueAlreadyExists, Message: err.Error()}
	}
	if errors.Is(err, storage.ErrQueueFull) {
		// Backlog da fila cheio (DiscardNew rejeitou o publish). OverLimit
		// sinaliza ao cliente para fazer backoff — nunca descartamos
		// mensagens antigas silenciosamente.
		return &AWSError{Code: ErrCodeOverLimit, Message: "fila atingiu o limite de backlog; tente novamente"}
	}
	var tooLarge storage.ErrMessageTooLargeT
	if errors.As(err, &tooLarge) {
		return &AWSError{Code: ErrCodeMessageTooLarge, Message: err.Error()}
	}
	var invalidRH storage.ErrInvalidReceiptHandleT
	if errors.As(err, &invalidRH) {
		return &AWSError{Code: ErrCodeReceiptHandleIsInvalid, Message: err.Error()}
	}
	var invalidArg storage.ErrInvalidArgumentT
	if errors.As(err, &invalidArg) {
		return &AWSError{Code: ErrCodeInvalidParameterValue, Message: err.Error()}
	}
	return &AWSError{Code: ErrCodeInternalError, Message: err.Error()}
}
