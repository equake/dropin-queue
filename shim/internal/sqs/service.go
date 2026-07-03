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

	"github.com/anomalyco/generic_queue/shim/internal/observability"
	"github.com/anomalyco/generic_queue/shim/internal/protocol"
	"github.com/anomalyco/generic_queue/shim/internal/storage"
	"github.com/anomalyco/generic_queue/shim/pkg/types"
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
			VisibilityTimeout:            parseInt32Default(params.Attributes["VisibilityTimeout"], types.DefaultQueueAttributes().VisibilityTimeout),
			MessageRetentionPeriod:       parseInt32Default(params.Attributes["MessageRetentionPeriod"], types.DefaultQueueAttributes().MessageRetentionPeriod),
			MaximumMessageSize:           parseInt32Default(params.Attributes["MaximumMessageSize"], types.DefaultQueueAttributes().MaximumMessageSize),
			DelaySeconds:                 parseInt32Default(params.Attributes["DelaySeconds"], types.DefaultQueueAttributes().DelaySeconds),
			ReceiveMessageWaitTimeSeconds: parseInt32Default(params.Attributes["ReceiveMessageWaitTimeSeconds"], types.DefaultQueueAttributes().ReceiveMessageWaitTimeSeconds),
			ContentBasedDeduplication:    params.Attributes["ContentBasedDeduplication"] == "true",
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
	QueueName       string
	AttributeNames  []string // ["All"] ou lista de nomes específicos
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

	allAttrs := map[string]string{
		"VisibilityTimeout":            fmt.Sprintf("%d", q.Attributes.VisibilityTimeout),
		"MessageRetentionPeriod":       fmt.Sprintf("%d", q.Attributes.MessageRetentionPeriod),
		"MaximumMessageSize":           fmt.Sprintf("%d", q.Attributes.MaximumMessageSize),
		"DelaySeconds":                 fmt.Sprintf("%d", q.Attributes.DelaySeconds),
		"ReceiveMessageWaitTimeSeconds": fmt.Sprintf("%d", q.Attributes.ReceiveMessageWaitTimeSeconds),
		"QueueArn":                     protocol.NewSQSARN(s.region, s.accountID, q.Name).String(),
		"ApproximateNumberOfMessages":  "0", // TODO Semana 3: usar QueueDepth
		"ApproximateNumberOfMessagesNotVisible": "0",
		"CreatedTimestamp":             fmt.Sprintf("%d", q.CreatedAt.UnixMilli()),
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
		} else {
			// AWS devolve silenciosamente atributos não-conhecidos; ok.
		}
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
		ErrCodeOverLimit:
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
	var tooLarge *storage.ErrMessageTooLargeT
	if errors.As(err, &tooLarge) {
		return &AWSError{Code: ErrCodeMessageTooLarge, Message: err.Error()}
	}
	var invalidRH *storage.ErrInvalidReceiptHandleT
	if errors.As(err, &invalidRH) {
		return &AWSError{Code: ErrCodeReceiptHandleIsInvalid, Message: err.Error()}
	}
	var invalidArg *storage.ErrInvalidArgumentT
	if errors.As(err, &invalidArg) {
		return &AWSError{Code: ErrCodeInvalidParameterValue, Message: err.Error()}
	}
	return &AWSError{Code: ErrCodeInternalError, Message: err.Error()}
}
