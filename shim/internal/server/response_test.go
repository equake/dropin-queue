package server

import (
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
func TestDetectTransport(t *testing.T) {
	cases := []struct {
		contentType string
		target      string
		isSNS       bool
		want        transport
	}{
		{"application/x-www-form-urlencoded", "", false, transportSQSQuery},
		{"application/x-www-form-urlencoded", "", true, transportSNSQuery},
		{"application/x-amz-json-1.0", "AmazonSQS.CreateQueue", false, transportSQSJSON},
		{"application/x-amz-json-1.0", "AmazonSNS.Publish", true, transportSNSJSON},
	}
	for _, c := range cases {
		req, _ := http.NewRequest("POST", "/", strings.NewReader(""))
		req.Header.Set("Content-Type", c.contentType)
		req.Header.Set("X-Amz-Target", c.target)
		got := detectTransport(req, c.isSNS)
		if got != c.want {
			tt := t
			tt.Errorf("ct=%q target=%q isSNS=%v: got %s, want %s",
				c.contentType, c.target, c.isSNS, got, c.want)
		}
	}
}

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
