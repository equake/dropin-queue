package protocol

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// ParseSQSQueryRequest faz parse de um request SQS no protocolo Query
// (form-encoded body, X-Amz-* em headers).
//
// Retorna:
//   - action: a operação pedida (Action header)
//   - params: form values parseados (após canonicalização de nomes)
//   - err: erro de parsing
//
// Formato esperado:
//
//   POST / HTTP/1.1
//   Content-Type: application/x-www-form-urlencoded
//   X-Amz-Date: 20240101T000000Z
//   X-Amz-Algorithm: AWS4-HMAC-SHA256
//
//   Action=CreateQueue&QueueName=myqueue&Version=2012-11-05
func ParseSQSQueryRequest(r *http.Request) (Action, url.Values, error) {
	if r.Method != http.MethodPost {
		return "", nil, fmt.Errorf("SQS Query requer POST, recebeu %s", r.Method)
	}

	ct := r.Header.Get("Content-Type")
	if ct != "" && !strings.HasPrefix(ct, "application/x-www-form-urlencoded") {
		// Tolerar Content-Type ausente (alguns clientes não enviam).
		return "", nil, fmt.Errorf("Content-Type esperado application/x-www-form-urlencoded, recebeu %q", ct)
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10<<20)) // 10 MB cap
	if err != nil {
		return "", nil, fmt.Errorf("ler body: %w", err)
	}
	defer r.Body.Close()

	if len(body) == 0 {
		return "", nil, errors.New("body vazio")
	}

	values, err := url.ParseQuery(string(body))
	if err != nil {
		return "", nil, fmt.Errorf("parse form: %w", err)
	}

	actionStr := values.Get("Action")
	if actionStr == "" {
		return "", nil, errors.New("parâmetro Action ausente")
	}
	action := Action(actionStr)
	if !IsValidAction(ServiceSQS, action) {
		return "", nil, fmt.Errorf("Action inválido: %q", action)
	}

	// Remover Version e Action dos params (são metadados, não parâmetros da op).
	values.Del("Version")
	values.Del("Action")

	return action, values, nil
}

// SQSQueryResponse é a estrutura envelopada de qualquer resposta SQS Query.
//
// Formato AWS:
//
//	<?xml version="1.0" encoding="UTF-8"?>
//	<Action>Response xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
//	  <ActionResult>
//	    ... campos específicos da operação ...
//	  </ActionResult>
//	  <ResponseMetadata>
//	    <RequestId>...</RequestId>
//	  </ResponseMetadata>
//	</Action>Response>
type SQSQueryResponse struct {
	XMLName           xml.Name
	Xmlns             string      `xml:"xmlns,attr"`
	Result            interface{} `xml:",omitempty"`
	ResponseMetadata  ResponseMetadata
}

// ResponseMetadata é o envelope padrão de toda resposta AWS.
type ResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

// SQSErrorEnvelope é a estrutura de erros SQS Query.
//
// Formato AWS:
//
//	<?xml version="1.0" encoding="UTF-8"?>
//	<ErrorResponse>
//	  <Error>
//	    <Type>Sender|Receiver</Type>
//	    <Code>QueueAlreadyExists</Code>
//	    <Message>...</Message>
//	  </Error>
//	  <RequestId>...</RequestId>
//	</ErrorResponse>
type SQSErrorEnvelope struct {
	XMLName   xml.Name `xml:"ErrorResponse"`
	Error     SQSError `xml:"Error"`
	RequestID string   `xml:"RequestId"`
}

// SQSError representa um erro individual.
type SQSError struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// EncodeSQSQueryResponse serializa uma resposta SQS no formato XML.
//
// actionResult: estrutura específica da operação (struct com tags xml).
// requestID: RequestId injetado em ResponseMetadata.
func EncodeSQSQueryResponse(w io.Writer, action Action, actionResult interface{}, requestID string) error {
	resp := SQSQueryResponse{
		XMLName:  xml.Name{Local: string(action) + "Response"},
		Xmlns:    "http://queue.amazonaws.com/doc/" + AWSProtocolVersion,
		Result:   actionResult,
		ResponseMetadata: ResponseMetadata{
			RequestID: requestID,
		},
	}
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	if err := enc.Encode(resp); err != nil {
		return err
	}
	return enc.Flush()
}

// EncodeSQSQueryError serializa um erro SQS no formato XML.
//
// Códigos AWS oficiais: QueueAlreadyExists, QueueDoesNotExist,
// InvalidParameterValue, etc. (https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/CommonErrors.html)
func EncodeSQSQueryError(w io.Writer, code, message, requestID string, senderFault bool) error {
	typ := "Receiver"
	if senderFault {
		typ = "Sender"
	}
	env := SQSErrorEnvelope{
		Error: SQSError{
			Type:    typ,
			Code:    code,
			Message: message,
		},
		RequestID: requestID,
	}
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	if err := enc.Encode(env); err != nil {
		return err
	}
	return enc.Flush()
}

// SortValues devolve as form values ordenadas por chave (útil para logging).
func SortValues(v url.Values) []string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		vs := v[k]
		sort.Strings(vs)
		out = append(out, fmt.Sprintf("%s=%s", k, strings.Join(vs, ",")))
	}
	return out
}
