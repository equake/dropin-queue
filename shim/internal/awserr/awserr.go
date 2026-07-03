// Package awserr fornece o tipo AWSError compartilhado entre os serviços
// SQS e SNS.
//
// Pré-refactor (refactor/kiss-dry-pass-1): sqs/service.go e sns/service.go
// tinham cada um sua própria:
//
//   - struct AWSError { Code, Message string }
//   - func (e *AWSError) Error() string
//   - func (e *AWSError) IsSenderFault() bool  (com switches divergindo
//     entre SQS e SNS — esquecia um = erro classificado como 5xx em um
//     serviço e 4xx no outro. Clientes AWS fazem retry baseado nisso)
//   - func AsAWSError(err) *AWSError
//   - 4 constantes ErrCode* idênticas byte-a-byte (InvalidParameterValue,
//     MissingParameter, InternalError, UnsupportedOperation)
//
// Pós-refactor: 1 lugar. Pacotes sqs/sns fazem type alias (type
// AWSError = awserr.Error) para preservar call sites sem binary break.
package awserr

import (
	"errors"
	"fmt"
	"sync"

	"github.com/equake/dropin-queue/shim/internal/storage"
)

// Error representa um erro AWS com código oficial.
//
// Formato wire:
//
//	<Code>Exception + <Message>
//	<?xml ...><ErrorResponse><Error><Type>Sender|Receiver</Type>
//	<Code>...</Code><Message>...</Message></Error><RequestId>...</RequestId>
type Error struct {
	Code    string
	Message string
}

// Error implementa error.
func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// --- Code helpers ergonômicos (construtores curtos). ---

// InvalidParameter é sender fault — código compartilhado.
func InvalidParameter(msg string) *Error {
	return &Error{Code: CodeInvalidParameterValue, Message: msg}
}

// MissingParameter é sender fault.
func MissingParameter(msg string) *Error {
	return &Error{Code: CodeMissingParameter, Message: msg}
}

// Internal é receiver fault — código compartilhado.
func Internal(msg string) *Error {
	return &Error{Code: CodeInternalError, Message: msg}
}

// UnsupportedOperation é receiver fault (do ponto de vista do cliente
// chamando operação não implementada pelo servidor).
func UnsupportedOperation(action string) *Error {
	return &Error{
		Code:    CodeUnsupportedOperation,
		Message: fmt.Sprintf("Action %q ainda não implementada", action),
	}
}

// --- Sender-fault registry. ---

var (
	senderRegistryOnce sync.Once
	senderCodes        = map[string]bool{
		CodeInvalidParameterValue: true,
		CodeMissingParameter:      true,
	}
)

// RegisterSenderFaults registra um conjunto de codes adicionais como
// sender-fault. Chamado em init() por cada pacote de serviço (sqs, sns).
//
// Idempotente — múltiplas chamadas são seguras; mapa final é único.
//
// NOTA: registro acontece uma vez no startup (cada pacote chama em init).
// Não é seguro chamar em runtime sob request load.
func RegisterSenderFaults(codes ...string) {
	senderRegistryOnce.Do(func() {
		// already initialized; nothing to bootstrap
	})
	for _, c := range codes {
		senderCodes[c] = true
	}
}

// IsSenderFault classifica um erro como culpa do cliente (4xx) ou do
// servidor (5xx). Clientes AWS fazem retry exponencial baseado nisso.
//
// Codes "puros" (InvalidParameterValue, MissingParameter) já são pré-
// registrados como sender fault. Pacotes sqs/sns registram seus codes
// específicos em init().
func (e *Error) IsSenderFault() bool {
	if e == nil {
		return false
	}
	return senderCodes[e.Code]
}

// --- Mapeamento storage error → awserr.Error. ---

// FromStorage mapeia qualquer erro storage em awserr.Error, usando
// mapeamentos comuns. notFoundCode é o code usado para ErrQueueNotFound
// / ErrTopicNotFound (cada serviço passa seu específico: QueueDoesNotExist
// ou TopicDoesNotExist).
//
// Centralizado aqui para SQS e SNS reutilizarem a mesma tradução dos
// erros tipados de storage/. Esquecer um mapeamento = erro vira
// InternalError genérico.
func FromStorage(err error, notFoundCode string) *Error {
	if err == nil {
		return nil
	}
	// Se já é um *Error, devolve como está.
	var awsErr *Error
	if errors.As(err, &awsErr) {
		return awsErr
	}

	switch {
	case errors.Is(err, storage.ErrQueueNotFound),
		errors.Is(err, storage.ErrTopicNotFound):
		return &Error{Code: notFoundCode, Message: err.Error()}
	case errors.Is(err, storage.ErrQueueAlreadyExists):
		return &Error{Code: CodeQueueAlreadyExists, Message: err.Error()}
	case errors.Is(err, storage.ErrQueueFull):
		return &Error{Code: CodeOverLimit,
			Message: "fila atingiu o limite de backlog; tente novamente"}
	}

	var tooLarge storage.ErrMessageTooLargeT
	if errors.As(err, &tooLarge) {
		return &Error{Code: CodeMessageTooLarge, Message: err.Error()}
	}
	var invalidRH storage.ErrInvalidReceiptHandleT
	if errors.As(err, &invalidRH) {
		return &Error{Code: CodeReceiptHandleIsInvalid, Message: err.Error()}
	}
	var invalidArg storage.ErrInvalidArgumentT
	if errors.As(err, &invalidArg) {
		return &Error{Code: CodeInvalidParameterValue, Message: err.Error()}
	}
	return &Error{Code: CodeInternalError, Message: err.Error()}
}

// --- Error codes compartilhados SQS e SNS (são wire-format AWS). ---
//
// Onde divergem (QueueAlreadyExists, TopicAlreadyExists, etc.), as
// constantes ficam em cada pacote (sqs/, sns/) e os pacotes chamam
// RegisterSenderFaults em init() para registrar seus codes específicos
// como sender-fault.

const (
	// CodeInvalidParameterValue: parâmetro inválido (sender fault).
	CodeInvalidParameterValue = "InvalidParameterValue"

	// CodeMissingParameter: parâmetro obrigatório ausente (sender fault).
	CodeMissingParameter = "MissingParameter"

	// CodeInternalError: erro interno inesperado (receiver fault).
	CodeInternalError = "InternalError"

	// CodeUnsupportedOperation: operação não implementada (sender fault —
	// cliente chamou algo que não existe).
	CodeUnsupportedOperation = "UnsupportedOperation"

	// Códigos abaixo eram de sqs/ mas como são storage-level
	// (QueueAlreadyExists = ErrQueueAlreadyExists mapped; QueueFull =
	// ErrQueueFull mapped) ficam em awserr por conveniência. Pacote sns
	// não os usa direto pois não tem "Queue" no namespace.
	//
	// Se sns precisar de Topic-specific codes no futuro, segue o mesmo
	// padrão (constante no sns pkg + RegisterSenderFaults em init).

	// CodeQueueAlreadyExists: tentativa de criar fila existente.
	CodeQueueAlreadyExists = "QueueAlreadyExists"

	// CodeOverLimit: limite de mensagens/filas excedido (sender fault —
	// cliente faz backoff).
	CodeOverLimit = "OverLimit"

	// CodeMessageTooLarge: body da mensagem excede MaximumMessageSize.
	CodeMessageTooLarge = "MessageTooLarge"

	// CodeReceiptHandleIsInvalid: receipt handle malformado ou expirado.
	CodeReceiptHandleIsInvalid = "ReceiptHandleIsInvalid"
)
