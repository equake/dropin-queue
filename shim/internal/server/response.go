// Package server — helpers compartilhados para response writing.
//
// Pré-refactor (refactor/kiss-dry-pass-1): o http.go tinha 4 funções
// write<X><Y>FatalError:
//
//   - writeSQSFatalError (XML, SQS, 80% idêntica)
//   - writeSQSJSONFatalError (JSON, SQS, 80% idêntica)
//   - writeSNSFatalError (XML, SNS, 80% idêntica)
//   - writeSNSJSONFatalError (JSON, SNS, 80% idêntica)
//
// Todas faziam:
//
//	awsErr := &<pkg>.AWSError{Code: code, Message: message}
//	w.Header().Set("Content-Type", "<xml|json>")
//	status := 500; if awsErr.IsSenderFault() { status = 400 }
//	w.WriteHeader(status)
//	_ = protocol.Encode<X><Y>Error(w, code, message, requestID, senderFault)
//
// Diferenças reais: Content-Type xml vs json; encoder SQS vs SNS.
// Os demais 80% eram copy-paste. Pós-refactor: 1 função writeFatalError
// parametrizada por transport.
//
// Adicionalmente, handlers usavam newRequestID() para gerar request ID
// da response DIVERGENTE do X-Request-ID no header (o middleware já gera
// um ID e expõe via context). Esta versão expõe requestFromContext(r)
// que mantém correlation ID consistente.
//
// ATENÇÃO: AWSError struct agora é awserr.Error (Commit 3 refactor).
// O pacote server ganhou acesso via import awserr; os call sites
// sqs.AsAWSError / sns.AsAWSError continuam funcionando porque o
// alias type AWSError = awserr.Error preserva identidade.
package server

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"

	"github.com/equake/dropin-queue/shim/internal/awserr"
	"github.com/equake/dropin-queue/shim/internal/protocol"
)

// transport identifica qual par service/protocol estamos serializando.
//
// Permite writeFatalError ser uma ÚNICA função parametrizada por transport
// em vez das 4 cópias do pré-refactor.
type transport string

const (
	transportSQSQuery transport = "sqs-query"
	transportSQSJSON  transport = "sqs-json"
	transportSNSQuery transport = "sns-query"
	transportSNSJSON  transport = "sns-json"
)

// detectTransport decide se o request é SQS/SNS Query/JSON baseado em
// headers. Pré-fix era feito inline em handleAWS; agora reutilizável.
func detectTransport(r *http.Request, isSNS bool) transport {
	if isJSONProtocol(r) {
		if isSNS {
			return transportSNSJSON
		}
		return transportSQSJSON
	}
	if isSNS {
		return transportSNSQuery
	}
	return transportSQSQuery
}

// contentTypeFor devolve o Content-Type header esperado para o transport.
func contentTypeFor(t transport) string {
	switch t {
	case transportSQSQuery, transportSNSQuery:
		return "text/xml"
	default:
		return "application/x-amz-json-1.0"
	}
}

// isJSONProtocol detecta se o request veio em JSON 1.0 (via
// X-Amz-Target prefix + Content-Type).
func isJSONProtocol(r *http.Request) bool {
	target := r.Header.Get("X-Amz-Target")
	ct := r.Header.Get("Content-Type")
	if target == "" {
		return false
	}
	return len(ct) >= 22 && ct[:22] == "application/x-amz-json"
}

// httpStatusLabel devolve "400" ou "500" para uso em labels Prometheus,
// baseado em IsSenderFault. Pré-refactor era statusFromAWSError em http.go;
// precisava porque ObserveHTTP aceita string.
//
//	observability.ObserveHTTP("sqs", action, httpStatusLabel(err), dur)
func httpStatusLabel(err error) string {
	var awsErr *awserr.Error
	if errors.As(err, &awsErr) && awsErr.IsSenderFault() {
		return "400"
	}
	return "500"
}

// sentinel importações para evitar go vet complaints em alguns cenários.
var _ = fmt.Sprintf

// writeFatalError serializa um erro AWS (Query ou JSON) com status HTTP correto.
//
// Substitui as 4 funções write<X><Y>FatalError do pré-refactor (commit 6
// do refactor/kiss-dry-pass-1). A única divergência real entre os
// transports era:
//
//  1. Content-Type (xml vs json)
//  2. Encoder call (EncodeSQSQueryError vs EncodeSNSQueryError vs
//     EncodeSQSJSONError vs EncodeSNSJSONError)
//
// Centralizado em 1 switch sobre o transport. IsSenderFault determina
// status (4xx = sender, 5xx = receiver) — exatamente como antes.
//
// Cacheia XML em buffer (encoder é stateful, não pode ser re-aproveitado
// em paralelo — mas por request estamos single-thread, então ok).
func writeFatalError(w http.ResponseWriter, t transport,
	code, message, requestID string) {

	err := &awserr.Error{Code: code, Message: message}
	status := http.StatusInternalServerError
	if err.IsSenderFault() {
		status = http.StatusBadRequest
	}

	w.Header().Set("Content-Type", contentTypeFor(t))
	w.WriteHeader(status)

	switch t {
	case transportSQSQuery:
		_ = protocol.EncodeSQSQueryError(w, code, message, requestID, err.IsSenderFault())
	case transportSNSQuery:
		_ = protocol.EncodeSNSQueryError(w, code, message, requestID, err.IsSenderFault())
	case transportSQSJSON:
		_ = protocol.EncodeSQSJSONError(w, code, message)
	case transportSNSJSON:
		_ = protocol.EncodeSNSJSONError(w, code, message)
	}
}

// errType devolve "Sender" ou "Receiver" baseado em senderFault.
// Mantido para helper local (não precisa mais importar awserr.Error
// aqui — só usávamos para IsSenderFault).
func errType(senderFault bool) string {
	if senderFault {
		return "Sender"
	}
	return "Receiver"
}

// writeQueryErrorEnvelope escreve o envelope padrão AWS Query error.
//
// Format:
//
//	<?xml ...><ErrorResponse>
//	  <Error><Type>Sender|Receiver</Type>
//	  <Code>...</Code><Message>...</Message></Error>
//	  <RequestId>...</RequestId>
//	</ErrorResponse>
//
// SQS e SNS usam EXATAMENTE o mesmo envelope (mesma hierarquia XML,
// mesmas tags).
func writeQueryErrorEnvelope(w http.ResponseWriter, code, message,
	requestID string, senderFault bool) error {

	env := struct {
		XMLName   xml.Name `xml:"ErrorResponse"`
		Type      string   `xml:"Error>Type"`
		Code      string   `xml:"Error>Code"`
		Message   string   `xml:"Error>Message"`
		RequestID string   `xml:"RequestId"`
	}{
		Type:      errType(senderFault),
		Code:      code,
		Message:   message,
		RequestID: requestID,
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	if err := enc.Encode(env); err != nil {
		return err
	}
	return enc.Flush()
}

// encodeJSONError serializa erro JSON 1.0 (compartilhado entre SQS+SNS).
//
// Format:
//
//	{"__type":"<Code>Exception","message":"<Message>"}
func encodeJSONError(w http.ResponseWriter, code, message string) error {
	resp := map[string]string{
		"__type":  code + "Exception",
		"message": message,
	}
	return json.NewEncoder(w).Encode(resp)
}
