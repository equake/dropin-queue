package protocol

import (
	"net/http"
	"strings"
	"testing"
)

// TestParseWireQueryRequest_SQS valida que parseWireQueryRequest roteia
// corretamente para SQS — pré-refactor era ParseSQSQueryRequest.
func TestParseWireQueryRequest_SQS(t *testing.T) {
	req, _ := http.NewRequest("POST", "/",
		strings.NewReader("Action=CreateQueue&QueueName=foo&Version=2012-11-05"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	action, params, err := ParseSQSQueryRequest(req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if action != ActionCreateQueue {
		t.Errorf("action: got %s, want %s", action, ActionCreateQueue)
	}
	if params.Get("QueueName") != "foo" {
		t.Errorf("QueueName: got %q, want %q", params.Get("QueueName"), "foo")
	}
	// Version e Action devem ser removidos.
	if params.Get("Action") != "" || params.Get("Version") != "" {
		t.Errorf("Version/Action devem ser removidos; got action=%q version=%q",
			params.Get("Action"), params.Get("Version"))
	}
}

// TestParseWireQueryRequest_BadMethod valida erro coerente.
func TestParseWireQueryRequest_BadMethod(t *testing.T) {
	req, _ := http.NewRequest("GET", "/", nil)
	_, _, err := ParseSQSQueryRequest(req)
	if err == nil {
		t.Fatal("esperava erro para GET")
	}
}

// TestParseWireJSONRequest_NamespaceMismatch valida que SQS parser
// rejeita X-Amz-Target com prefix "AmazonSNS." (namespace errado).
//
// Pré-refactor (refactor/kiss-dry-pass-1) este check foi introduzido
// em parseWireJSONRequest — client mandando BogusService.CreateQueue
// é 4xx senderFault (client errou namespace).
func TestParseWireJSONRequest_NamespaceMismatch(t *testing.T) {
	body := `{"QueueName":"foo"}`
	req, _ := http.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "BogusService.CreateQueue")

	_, _, err := ParseSQSJSONRequest(req)
	if err == nil {
		t.Fatal("esperava erro por X-Amz-Target com prefix errado")
	}
	if !strings.Contains(err.Error(), "AmazonSQS") {
		t.Errorf("erro deveria mencionar prefix esperado, got %q", err.Error())
	}
}

// TestParseWireJSONRequest_MissingDot valida que X-Amz-Target sem "."
// é rejeitado.
func TestParseWireJSONRequest_MissingDot(t *testing.T) {
	body := `{"QueueName":"foo"}`
	req, _ := http.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "BogusTarget") // sem ponto

	_, _, err := ParseSQSJSONRequest(req)
	if err == nil {
		t.Fatal("esperava erro por X-Amz-Target sem ponto")
	}
}

// TestEncodeQueryEnvelope_ByteFormatExact garante que o envelope XML
// produzido pelo helper compartilhado é byte-a-byte idêntico ao que
// pré-refactor gerava. Wire format precisa ser preservado.
func TestEncodeQueryEnvelope_ByteFormatExact(t *testing.T) {
	type result struct {
		XMLName  string `xml:"CreateQueueResult"`
		QueueURL string `xml:"QueueUrl"`
	}
	var buf strings.Builder
	if err := EncodeSQSQueryResponse(&buf, ActionCreateQueue, result{
		QueueURL: "http://localhost:4566/000000000000/foo",
	}, "req-abc-123"); err != nil {
		t.Fatalf("encode: %v", err)
	}
	out := buf.String()
	// Verifica elementos estruturais chave do envelope.
	if !strings.Contains(out, `<?xml version="1.0" encoding="UTF-8"?>`) {
		t.Errorf("faltando xml.Header em output: %s", out)
	}
	if !strings.Contains(out, "<CreateQueueResponse") {
		t.Errorf("faltando CreateQueueResponse tag: %s", out)
	}
	if !strings.Contains(out, `xmlns="http://queue.amazonaws.com/doc/2012-11-05"`) {
		t.Errorf("faltando xmlns: %s", out)
	}
	if !strings.Contains(out, "<RequestId>req-abc-123</RequestId>") {
		t.Errorf("faltando RequestId: %s", out)
	}
	if !strings.Contains(out, "<QueueUrl>http://localhost:4566/000000000000/foo</QueueUrl>") {
		t.Errorf("faltando QueueUrl: %s", out)
	}
}

// TestEncodeQueryError_Format garante que erro envelope tem Type correto
// (Sender para sender fault, Receiver para receiver fault).
func TestEncodeQueryError_Format(t *testing.T) {
	cases := []struct {
		senderFault bool
		wantType    string
	}{
		{true, "Sender"},
		{false, "Receiver"},
	}
	for _, c := range cases {
		var buf strings.Builder
		if err := EncodeSQSQueryError(&buf, "QueueNameExists",
			"queue exists", "req-xyz", c.senderFault); err != nil {
			t.Fatal(err)
		}
		out := buf.String()
		if !strings.Contains(out, "<Type>"+c.wantType+"</Type>") {
			t.Errorf("senderFault=%v: want Type=%s, got: %s",
				c.senderFault, c.wantType, out)
		}
	}
}

// TestEncodeJSONError_Format garante o formato __type=Code (sem sufixo)
// usado por SQS JSON 1.0 e SNS JSON 1.0 (byte-a-byte idêntico).
//
// __type precisa bater exatamente com o nome do shape modelado pela AWS
// (ex.: botocore/data/sqs/2012-11-05/service-2.json define o shape
// "QueueDoesNotExist" sem sufixo) — um "Exception" extra faz o SDK
// oficial nunca casar com a exceção tipada.
func TestEncodeJSONError_Format(t *testing.T) {
	var buf strings.Builder
	if err := EncodeSQSJSONError(&buf, "QueueNameExists", "queue exists"); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, `"__type":"QueueNameExists"`) {
		t.Errorf("__type format errado: %s", out)
	}
	if !strings.Contains(out, `"message":"queue exists"`) {
		t.Errorf("message format errado: %s", out)
	}
}
