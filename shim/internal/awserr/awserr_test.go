package awserr

import (
	"errors"
	"testing"

	"github.com/equake/dropin-queue/shim/internal/storage"
)

// TestError_Error valida o formato "<Code>: <Message>" exigido pela
// convenção AWS (em logs e no XML o Code aparece separado, mas em
// Go logs a.Error() == "<Code>: <Message>" é útil).
func TestError_Error(t *testing.T) {
	e := &Error{Code: "QueueAlreadyExists", Message: "queue exists"}
	want := "QueueAlreadyExists: queue exists"
	if got := e.Error(); got != want {
		t.Errorf("Error(): got %q, want %q", got, want)
	}
}

// TestIsSenderFault_Defaults valida codes pré-registrados (sender fault
// por default) são classificados como 4xx.
func TestIsSenderFault_Defaults(t *testing.T) {
	cases := []struct {
		code string
		want bool
	}{
		{CodeInvalidParameterValue, true},
		{CodeMissingParameter, true},
		{CodeInternalError, false},
		{CodeUnsupportedOperation, false},
	}
	for _, c := range cases {
		e := &Error{Code: c.code}
		if got := e.IsSenderFault(); got != c.want {
			t.Errorf("IsSenderFault(%s): got %v, want %v", c.code, got, c.want)
		}
	}
}

// TestRegisterSenderFaults valida que codes adicionais podem ser
// registrados e classificados como sender fault.
func TestRegisterSenderFaults(t *testing.T) {
	const testCode = "TestCustomSenderFault"
	RegisterSenderFaults(testCode)

	e := &Error{Code: testCode}
	if !e.IsSenderFault() {
		t.Errorf("%s deveria ser sender fault após RegisterSenderFaults", testCode)
	}
}

// TestFromStorage_NilInput valida que nil nunca vira awserr.Error.
func TestFromStorage_NilInput(t *testing.T) {
	if got := FromStorage(nil, "AnythingDoesNotExist"); got != nil {
		t.Errorf("FromStorage(nil): got %v, want nil", got)
	}
}

// TestFromStorage_AlreadyError valida que *Error passa direto.
func TestFromStorage_AlreadyError(t *testing.T) {
	original := &Error{Code: "Foo", Message: "bar"}
	got := FromStorage(original, "AnythingDoesNotExist")
	if got != original {
		t.Errorf("FromStorage(awserr.Error) não preservou identidade")
	}
}

// TestFromStorage_NotFound valida mapeamento ErrQueueNotFound /
// ErrTopicNotFound para o notFoundCode fornecido.
func TestFromStorage_NotFound(t *testing.T) {
	cases := []struct {
		err          error
		notFoundCode string
		want         string
	}{
		{storage.ErrQueueNotFound, "QueueDoesNotExist", "QueueDoesNotExist"},
		{storage.ErrTopicNotFound, "TopicDoesNotExist", "TopicDoesNotExist"},
	}
	for _, c := range cases {
		got := FromStorage(c.err, c.notFoundCode)
		if got == nil {
			t.Fatal("FromStorage retornou nil")
		}
		if got.Code != c.want {
			t.Errorf("Code: got %s, want %s", got.Code, c.want)
		}
	}
}

// TestFromStorage_QueueAlreadyExists valida mapeamento direto.
func TestFromStorage_QueueAlreadyExists(t *testing.T) {
	got := FromStorage(storage.ErrQueueAlreadyExists, "QueueDoesNotExist")
	if got.Code != CodeQueueNameExists {
		t.Errorf("Code: got %s, want %s", got.Code, CodeQueueNameExists)
	}
}

// TestFromStorage_QueueFull valida mapeamento para OverLimit com mensagem
// útil pro cliente (backoff).
func TestFromStorage_QueueFull(t *testing.T) {
	got := FromStorage(storage.ErrQueueFull, "QueueDoesNotExist")
	if got.Code != CodeOverLimit {
		t.Errorf("Code: got %s, want %s", got.Code, CodeOverLimit)
	}
	if got.Message == "" {
		t.Error("Message vazio; cliente não recebe dica para backoff")
	}
}

// TestFromStorage_MessageTooLarge valida mapeamento com sintaxe correta
// (errors.As pega o struct tipado).
func TestFromStorage_MessageTooLarge(t *testing.T) {
	original := storage.ErrMessageTooLarge(300, 256)
	got := FromStorage(original, "QueueDoesNotExist")
	if got.Code != CodeMessageTooLarge {
		t.Errorf("Code: got %s, want %s", got.Code, CodeMessageTooLarge)
	}
	if got.Message != original.Error() {
		t.Errorf("Message deve preservar Error() original")
	}
}

// TestFromStorage_ReceiptHandleInvalido valida mapeamento.
func TestFromStorage_ReceiptHandleInvalido(t *testing.T) {
	original := storage.ErrInvalidReceiptHandle("rh corrompido")
	got := FromStorage(original, "QueueDoesNotExist")
	if got.Code != CodeReceiptHandleIsInvalid {
		t.Errorf("Code: got %s, want %s", got.Code, CodeReceiptHandleIsInvalid)
	}
}

// TestFromStorage_InvalidArgument valida mapeamento para InvalidParameter.
func TestFromStorage_InvalidArgument(t *testing.T) {
	original := storage.ErrInvalidArgument("body vazio")
	got := FromStorage(original, "QueueDoesNotExist")
	if got.Code != CodeInvalidParameterValue {
		t.Errorf("Code: got %s, want %s", got.Code, CodeInvalidParameterValue)
	}
}

// TestFromStorage_FallbackInternal garante que erro desconhecido vira
// InternalError em vez de explodir.
func TestFromStorage_FallbackInternal(t *testing.T) {
	original := errors.New("kaboom — não mapeado")
	got := FromStorage(original, "QueueDoesNotExist")
	if got.Code != CodeInternalError {
		t.Errorf("Code: got %s, want %s", got.Code, CodeInternalError)
	}
	if got.Message != original.Error() {
		t.Errorf("Message deve preservar original.Error(); got %q", got.Message)
	}
}

// TestError_NilIsSenderFault valida que nil receiver não panica.
// (Importante — server pode chamar IsSenderFault em código defensivo.)
func TestError_NilIsSenderFault(t *testing.T) {
	var e *Error
	if got := e.IsSenderFault(); got != false {
		t.Errorf("(*Error)(nil).IsSenderFault(): got %v, want false", got)
	}
}
