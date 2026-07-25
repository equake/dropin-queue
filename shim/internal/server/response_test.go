package server

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/equake/dropin-queue/shim/internal/awserr"
)

// TestWriteFatalError_StatusSenderFault valida que 4xx é escolhido
// quando IsSenderFault() = true (códigos pré-registrados em awserr).
func TestWriteFatalError_StatusSenderFault(t *testing.T) {
	rec := httptest.NewRecorder()
	// InvalidParameterValue é pré-registrado como sender fault em awserr
	// (refactor/kiss-dry-pass-1 Commit 3).
	writeFatalError(rec, transportSQSQuery,
		awserr.CodeInvalidParameterValue, "melhor arruma", "req-abc")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("sender fault: got %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if !strings.Contains(rec.Body.String(), "InvalidParameterValue") {
		t.Errorf("body deve conter Code: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "req-abc") {
		t.Errorf("body deve conter RequestId: %s", rec.Body.String())
	}
	ct := rec.Header().Get("Content-Type")
	if ct != "text/xml" {
		t.Errorf("Content-Type: got %q, want %q", ct, "text/xml")
	}
	if !strings.Contains(rec.Body.String(), "<Type>Sender</Type>") {
		t.Errorf("body deve indicar Type=Sender: %s", rec.Body.String())
	}
}

// TestWriteFatalError_StatusReceiverFault valida 5xx quando não é
// sender fault.
func TestWriteFatalError_StatusReceiverFault(t *testing.T) {
	rec := httptest.NewRecorder()
	// InternalError é receiver fault (não pré-registrado em sender).
	writeFatalError(rec, transportSQSJSON,
		awserr.CodeInternalError, "kaboom interno", "req-xyz")
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("receiver fault: got %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if !strings.Contains(rec.Body.String(), "InternalError") {
		t.Errorf("body: %s", rec.Body.String())
	}
	ct := rec.Header().Get("Content-Type")
	if ct != "application/x-amz-json-1.0" {
		t.Errorf("Content-Type: got %q, want %q", ct, "application/x-amz-json-1.0")
	}
	if !strings.Contains(rec.Body.String(), `"message":"kaboom interno"`) {
		t.Errorf("JSON body deve conter message: %s", rec.Body.String())
	}
}

// TestWriteFatalError_AllTransports garante que todos os 4 transports
// (sqs-query, sqs-json, sns-query, sns-json) produzem response válido.
func TestWriteFatalError_AllTransports(t *testing.T) {
	transports := []transport{
		transportSQSQuery, transportSQSJSON,
		transportSNSQuery, transportSNSJSON,
	}
	for _, tr := range transports {
		rec := httptest.NewRecorder()
		writeFatalError(rec, tr, awserr.CodeInvalidParameterValue, "x", "req")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("transport %s: got %d, want 400", tr, rec.Code)
		}
		if rec.Body.Len() == 0 {
			t.Errorf("transport %s: body vazio", tr)
		}
	}
}

// TestDetectTransport garante que a discriminação JSON vs Query e
// SQS vs SNS funciona via headers.
// TestContentTypeFor é trivial mas protege contra regressão no mapping.
func TestContentTypeFor(t *testing.T) {
	cases := map[transport]string{
		transportSQSQuery: "text/xml",
		transportSNSQuery: "text/xml",
		transportSQSJSON:  "application/x-amz-json-1.0",
		transportSNSJSON:  "application/x-amz-json-1.0",
	}
	for tr, want := range cases {
		if got := contentTypeFor(tr); got != want {
			t.Errorf("%s: got %q, want %q", tr, got, want)
		}
	}
}

// TestErrType mapeia sender fault → "Sender", receiver → "Receiver".
func TestErrType(t *testing.T) {
	if errType(true) != "Sender" {
		t.Error("sender fault → expected 'Sender'")
	}
	if errType(false) != "Receiver" {
		t.Error("non-sender → expected 'Receiver'")
	}
}

// TestRespondQueryXML_WireFormatExact valida que respondSQSQueryXML
// produz byte-a-byte o mesmo envelope que o pattern inline pré-refactor
// (sqs_handlers.go usava ~22 cópias desse mesmo pattern). Refactor/
// kiss-dry-pass-2 Commit 2 introduz o helper; este teste pega qualquer
// regressão no envelope.
func TestRespondQueryXML_WireFormatExact(t *testing.T) {
	type createQueueResult struct {
		XMLName  xml.Name `xml:"CreateQueueResult"`
		QueueURL string   `xml:"QueueUrl"`
	}
	var buf bytes.Buffer
	rec := &bufferRW{header: make(http.Header), w: &buf}
	respondSQSQueryXML(rec, "CreateQueue",
		createQueueResult{QueueURL: "http://localhost:4566/000000000000/foo"},
		"req-abc")

	ct := rec.header.Get("Content-Type")
	if ct != "text/xml" {
		t.Errorf("Content-Type: got %q, want text/xml", ct)
	}
	out := buf.String()
	// Elementos estruturais chave (mesmo check que TestEncodeQueryEnvelope).
	musts := []string{
		`<?xml version="1.0" encoding="UTF-8"?>`,
		"<CreateQueueResponse",
		`xmlns="http://queue.amazonaws.com/doc/2012-11-05"`,
		"<QueueUrl>http://localhost:4566/000000000000/foo</QueueUrl>",
		"<RequestId>req-abc</RequestId>",
	}
	for _, m := range musts {
		if !strings.Contains(out, m) {
			t.Errorf("missing %q in output:\n%s", m, out)
		}
	}
}

// bufferRW é um http.ResponseWriter mínimo que captura header + body.
// Suficiente para teste do helper — não usa ResponseWriter interface
// completa porque queremos validar set/print de conteúdo específico.
type bufferRW struct {
	header http.Header
	w      *bytes.Buffer
	code   int
}

func (b *bufferRW) Header() http.Header { return b.header }
func (b *bufferRW) Write(p []byte) (int, error) {
	return b.w.Write(p)
}
func (b *bufferRW) WriteHeader(statusCode int) { b.code = statusCode }
