package protocol

import (
	"encoding/base64"
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

// ExtractQueryMessageAttributes extrai pares MessageAttribute do form-encoded.
//
// AWS SQS Query usa formato numerado:
//
//	MessageAttribute.1.Name=foo
//	MessageAttribute.1.DataType=String
//	MessageAttribute.1.StringValue=bar
//	MessageAttribute.2.Name=count
//	MessageAttribute.2.DataType=Number
//	MessageAttribute.2.StringValue=42
//
// DataType = String|Number|Binary|String.List
//
// Para String.List o valor vem em MessageAttribute.N.StringListValue.1, .2, ...
// (suportado: parseamos como JSON array).
//
// Para Binary vem em MessageAttribute.N.BinaryValue (base64).
func ExtractQueryMessageAttributes(params url.Values) map[string]MessageAttributeValue {
	out := make(map[string]MessageAttributeValue)
	// Coleta por índice: 1 → {Name, DataType, StringValue, BinaryValue}
	type entry struct {
		Name, DataType, StringValue, BinaryValue string
		StringList                               []string
	}
	byIdx := make(map[string]*entry)

	prefix := "MessageAttribute."
	for k, vs := range params {
		if !strings.HasPrefix(k, prefix) || len(vs) == 0 {
			continue
		}
		rest := k[len(prefix):] // "1.Name" ou "1.StringListValue.3"
		dot := strings.IndexByte(rest, '.')
		if dot < 0 {
			continue
		}
		idx := rest[:dot]
		field := rest[dot+1:]
		if _, ok := byIdx[idx]; !ok {
			byIdx[idx] = &entry{}
		}
		e := byIdx[idx]
		v := vs[0]
		switch {
		case field == "Name":
			e.Name = v
		case field == "DataType":
			e.DataType = v
		case field == "StringValue":
			e.StringValue = v
		case field == "BinaryValue":
			e.BinaryValue = v
		case strings.HasPrefix(field, "StringListValue."):
			e.StringList = append(e.StringList, v)
		}
	}

	for _, e := range byIdx {
		if e.Name == "" || e.DataType == "" {
			continue
		}
		switch e.DataType {
		case "String", "Number":
			out[e.Name] = MessageAttributeValue{
				DataType:    e.DataType,
				StringValue: e.StringValue,
			}
		case "Binary":
			out[e.Name] = MessageAttributeValue{
				DataType:    "Binary",
				BinaryValue: decodeBase64(e.BinaryValue),
			}
		case "String.List":
			out[e.Name] = MessageAttributeValue{
				DataType:    "String.List",
				StringValue: joinList(e.StringList),
			}
		}
	}
	return out
}

// MessageAttributeValue é o formato interno para MessageAttribute parsed.
//
// StringValue para tipos simples (String, Number) E serialização compact
// de String.List (valores separados por |). BinaryValue para Binary.
type MessageAttributeValue struct {
	DataType    string
	StringValue string
	BinaryValue []byte
}

// decodeBase64 decodifica base64 padrão; em erro retorna slice vazio.
func decodeBase64(s string) []byte {
	if s == "" {
		return nil
	}
	dec, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil
	}
	return dec
}

// joinList serializa lista como valores separados por | (decode em service).
func joinList(items []string) string {
	return strings.Join(items, "|")
}

// ExtractQuerySystemAttributes extrai pares Attribute.N.Name/Value do form-encoded.
//
// System Attributes SQS (SentTimestamp, ApproximateReceiveCount, etc.) usam
// "Attribute.N.Name" e "Attribute.N.Value" (não MessageAttribute).
func ExtractQuerySystemAttributes(params url.Values) map[string]string {
	out := make(map[string]string)
	prefix := "Attribute."
	for k, vs := range params {
		if !strings.HasPrefix(k, prefix) || len(vs) == 0 {
			continue
		}
		rest := k[len(prefix):]
		dot := strings.IndexByte(rest, '.')
		if dot < 0 {
			continue
		}
		idx := rest[:dot]
		field := rest[dot+1:]
		if field == "Name" {
			name := vs[0]
			valueKey := prefix + idx + ".Value"
			if vvs, ok := params[valueKey]; ok && len(vvs) > 0 {
				out[name] = vvs[0]
			}
		}
	}
	return out
}
