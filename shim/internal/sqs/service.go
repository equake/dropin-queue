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
// Comportamento AWS:
//   - Se fila existe com mesmos atributos → idempotente, devolve a URL.
//   - Se fila existe com atributos diferentes → QueueAlreadyExists.
//   - Atributos não fornecidos → defaults SQS.
//
// Validações:
//   - QueueName: 1-80 chars, alfanuméricos + . _ -
//   - Atributos: nomes conhecidos, valores em range
//   - FIFO suffix .fifo se Attributes.FifoQueue=true
func (s *Service) CreateQueue(ctx context.Context, params url.Values) (*types.Queue, error) {
	start := s.agora()

	p, err := parseCreateQueueParams(params)
	if err != nil {
		return nil, err
	}

	q := types.Queue{
		Name:      p.QueueName,
		AccountID: s.accountID,
		Region:    s.region,
		Attributes: types.QueueAttributes{
			VisibilityTimeout:            parseInt32Default(p.Attributes["VisibilityTimeout"], types.DefaultQueueAttributes().VisibilityTimeout),
			MessageRetentionPeriod:       parseInt32Default(p.Attributes["MessageRetentionPeriod"], types.DefaultQueueAttributes().MessageRetentionPeriod),
			MaximumMessageSize:           parseInt32Default(p.Attributes["MaximumMessageSize"], types.DefaultQueueAttributes().MaximumMessageSize),
			DelaySeconds:                 parseInt32Default(p.Attributes["DelaySeconds"], types.DefaultQueueAttributes().DelaySeconds),
			ReceiveMessageWaitTimeSeconds: parseInt32Default(p.Attributes["ReceiveMessageWaitTimeSeconds"], types.DefaultQueueAttributes().ReceiveMessageWaitTimeSeconds),
			ContentBasedDeduplication:    p.Attributes["ContentBasedDeduplication"] == "true",
		},
		Tags:    p.Tags,
		FIFO:    strings.HasSuffix(p.QueueName, ".fifo") || p.Attributes["FifoQueue"] == "true",
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

// parseCreateQueueParams extrai e valida os parâmetros de CreateQueue.
func parseCreateQueueParams(params url.Values) (*CreateQueueParams, error) {
	p := &CreateQueueParams{
		QueueName:  params.Get("QueueName"),
		Attributes: extractAttributes(params, "Attribute"),
		Tags:       extractAttributes(params, "Tag"),
	}

	if p.QueueName == "" {
		return nil, &AWSError{
			Code:    ErrCodeMissingParameter,
			Message: "QueueName é obrigatório",
		}
	}
	if !isValidQueueName(p.QueueName) {
		return nil, &AWSError{
			Code:    ErrCodeInvalidParameterValue,
			Message: fmt.Sprintf("QueueName inválido: %q (1-80 chars, alfanuméricos, -, _, .)", p.QueueName),
		}
	}
	return p, nil
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
	return &AWSError{Code: ErrCodeInternalError, Message: err.Error()}
}
