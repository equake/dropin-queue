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
	"strconv"
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
//	POST / HTTP/1.1
//	Content-Type: application/x-www-form-urlencoded
//	X-Amz-Date: 20240101T000000Z
//	X-Amz-Algorithm: AWS4-HMAC-SHA256
//
//	Action=CreateQueue&QueueName=myqueue&Version=2012-11-05
func ParseSQSQueryRequest(r *http.Request) (Action, url.Values, error) {
	if r.Method != http.MethodPost {
		return "", nil, fmt.Errorf("SQS Query requer POST, recebeu %s", r.Method)
	}

	ct := r.Header.Get("Content-Type")
	if ct != "" && !strings.HasPrefix(ct, "application/x-www-form-urlencoded") {
		// Tolerar Content-Type ausente (alguns clientes não enviam).
		return "", nil, fmt.Errorf("Content-Type esperado application/x-www-form-urlencoded, recebeu %q", ct)
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, MaxWireBodyBytes))
	if err != nil {
		return "", nil, fmt.Errorf("ler body: %w", err)
	}
	defer func() { _ = r.Body.Close() }()

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
	XMLName          xml.Name
	Xmlns            string      `xml:"xmlns,attr"`
	Result           interface{} `xml:",omitempty"`
	ResponseMetadata ResponseMetadata
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
		XMLName: xml.Name{Local: string(action) + "Response"},
		Xmlns:   "http://queue.amazonaws.com/doc/" + AWSProtocolVersion,
		Result:  actionResult,
		ResponseMetadata: ResponseMetadata{
			RequestID: requestID,
		},
	}
	_, _ = w.Write([]byte(xml.Header))
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
	_, _ = w.Write([]byte(xml.Header))
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

// msgAttrEntry é uma entrada MessageAttribute parseada intermediária.
type msgAttrEntry struct {
	Name, DataType, StringValue, BinaryValue string
	StringList                               []string
}

// ExtractQueryMessageAttributes extrai pares MessageAttribute do form-encoded.
//
// Suporta dois formatos:
//
// (1) Padrão AWS:
//
//	MessageAttribute.1.Name=foo
//	MessageAttribute.1.DataType=String
//	MessageAttribute.1.StringValue=bar
//
// (2) boto3 entry wrapper (usado em SNS):
//
//	MessageAttributes.entry.1.Name=type
//	MessageAttributes.entry.1.Value.DataType=String
//	MessageAttributes.entry.1.Value.StringValue=alert
//
// DataType = String|Number|Binary|String.List
//
// Para String.List o valor vem em MessageAttribute.N.StringListValue.1, .2, ...
// (suportado: parseamos como JSON array).
//
// Para Binary vem em MessageAttribute.N.BinaryValue (base64).
func ExtractQueryMessageAttributes(params url.Values) map[string]MessageAttributeValue {
	out := make(map[string]MessageAttributeValue)
	byIdx := make(map[string]*msgAttrEntry)

	parseQueryAttrs(params, "MessageAttribute.", byIdx)

	// Formato entry wrapper (boto3 SNS): DataType/StringValue ficam sob "Value.".
	// Achatamos renomeando esses campos para nomes diretos.
	entryByIdx := make(map[string]*msgAttrEntry)
	parseQueryAttrs(params, "MessageAttributes.entry.", entryByIdx)
	for idx, e := range entryByIdx {
		flat := byIdx[idx]
		if flat == nil {
			flat = &msgAttrEntry{}
			byIdx[idx] = flat
		}
		if flat.Name == "" {
			flat.Name = e.Name
		}
		if flat.DataType == "" {
			flat.DataType = e.DataType
		}
		if flat.StringValue == "" {
			flat.StringValue = e.StringValue
		}
		if flat.BinaryValue == "" {
			flat.BinaryValue = e.BinaryValue
		}
		if len(flat.StringList) == 0 {
			flat.StringList = e.StringList
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

// parseQueryAttrs popula byIdx a partir de params com key começando com prefix.
//
// Cada key após o prefix é "<idx>.<campo>" — mas o campo pode ter pontos
// aninhados como "Value.DataType" (formato boto3 entry wrapper). Para esse
// formato achatamos: se o campo começa com "Value.", extraímos só o sufixo.
func parseQueryAttrs(params url.Values, prefix string, byIdx map[string]*msgAttrEntry) {
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
		// Achata "Value.DataType" → "DataType", etc.
		field = strings.TrimPrefix(field, "Value.")
		if _, ok := byIdx[idx]; !ok {
			byIdx[idx] = &msgAttrEntry{}
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
}

// MessageAttributeValue é o formato interno para MessageAttribute parsed.
//
// StringValue para tipos simples (String, Number) E serialização compact
// de String.List (valores separados por |). BinaryValue para Binary.
//
// ATENÇÃO: type movido para message_attribute.go (refactor/kiss-dry-pass-1).
// Mantido aqui apenas para não quebrar o CI enquanto a migração está em
// progresso — REMOVER no próximo commit do refactor.

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

// QuerySendMessageBatchEntry representa uma entry do SendMessageBatch em Query.
//
// Formato AWS:
//
//	SendMessageBatchRequestEntry.1.Id=foo
//	SendMessageBatchRequestEntry.1.MessageBody=bar
//	SendMessageBatchRequestEntry.1.DelaySeconds=0
//	SendMessageBatchRequestEntry.1.MessageGroupId=g1
//	SendMessageBatchRequestEntry.1.MessageDeduplicationId=d1
//	SendMessageBatchRequestEntry.1.MessageAttribute.1.Name=...
//	SendMessageBatchRequestEntry.1.MessageAttribute.1.DataType=...
//	SendMessageBatchRequestEntry.1.MessageAttribute.1.StringValue=...
type QuerySendMessageBatchEntry struct {
	Id                     string
	MessageBody            string
	DelaySeconds           int32
	MessageGroupId         string
	MessageDeduplicationId string
	MessageAttributes      map[string]MessageAttributeValue
}

// QueryDeleteMessageBatchEntry representa uma entry do DeleteMessageBatch em Query.
//
// Formato AWS:
//
//	DeleteMessageBatchRequestEntry.1.Id=foo
//	DeleteMessageBatchRequestEntry.1.ReceiptHandle=rh1:...
//	DeleteMessageBatchRequestEntry.1.VisibilityTimeout=0  (opcional)
type QueryDeleteMessageBatchEntry struct {
	Id                string
	ReceiptHandle     string
	VisibilityTimeout int32 // 0 → não muda (equivalente a DeleteMessage direto)
}

// ExtractQuerySendMessageBatchEntries extrai entries do SendMessageBatch em Query.
//
// Retorna slice na ordem dos índices (1..N) encontrados.
func ExtractQuerySendMessageBatchEntries(params url.Values) []QuerySendMessageBatchEntry {
	prefix := "SendMessageBatchRequestEntry."
	indices := collectBatchIndices(params, prefix)

	out := make([]QuerySendMessageBatchEntry, 0, len(indices))
	for _, idx := range indices {
		e := QuerySendMessageBatchEntry{
			Id:                     params.Get(prefix + idx + ".Id"),
			MessageBody:            params.Get(prefix + idx + ".MessageBody"),
			DelaySeconds:           parseInt32DefaultQ(params.Get(prefix + idx + ".DelaySeconds")),
			MessageGroupId:         params.Get(prefix + idx + ".MessageGroupId"),
			MessageDeduplicationId: params.Get(prefix + idx + ".MessageDeduplicationId"),
			MessageAttributes:      extractQueryEntryMsgAttrs(params, prefix+idx+".MessageAttribute"),
		}
		out = append(out, e)
	}
	return out
}

// ExtractQueryDeleteMessageBatchEntries extrai entries do DeleteMessageBatch em Query.
func ExtractQueryDeleteMessageBatchEntries(params url.Values) []QueryDeleteMessageBatchEntry {
	prefix := "DeleteMessageBatchRequestEntry."
	indices := collectBatchIndices(params, prefix)

	out := make([]QueryDeleteMessageBatchEntry, 0, len(indices))
	for _, idx := range indices {
		e := QueryDeleteMessageBatchEntry{
			Id:                params.Get(prefix + idx + ".Id"),
			ReceiptHandle:     params.Get(prefix + idx + ".ReceiptHandle"),
			VisibilityTimeout: parseInt32DefaultQ(params.Get(prefix + idx + ".VisibilityTimeout")),
		}
		out = append(out, e)
	}
	return out
}

// collectBatchIndices encontra índices 1..N para entries de batch no form-encoded.
//
// Para um prefixo como "SendMessageBatchRequestEntry.", retorna ["1","2",...] na
// ordem encontrada.
func collectBatchIndices(params url.Values, prefix string) []string {
	seen := make(map[string]bool)
	var indices []string
	for k := range params {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := k[len(prefix):]
		dot := strings.IndexByte(rest, '.')
		if dot <= 0 {
			continue
		}
		idx := rest[:dot]
		if seen[idx] {
			continue
		}
		seen[idx] = true
		indices = append(indices, idx)
	}
	sort.Strings(indices) // deterministic order
	return indices
}

// extractQueryEntryMsgAttrs extrai MessageAttribute.N.* de uma entry de batch.
//
// prefix típico: "SendMessageBatchRequestEntry.1.MessageAttribute"
func extractQueryEntryMsgAttrs(params url.Values, prefix string) map[string]MessageAttributeValue {
	// Filtra apenas as chaves deste prefixo, depois reaproveita ExtractQueryMessageAttributes
	// renomeando as chaves para "MessageAttribute.N.*".
	filtered := make(url.Values)
	for k, v := range params {
		if strings.HasPrefix(k, prefix+".") {
			// renomeia "1.Name" → "MessageAttribute.1.Name"
			renamed := "MessageAttribute." + k[len(prefix)+1:]
			filtered[renamed] = v
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return ExtractQueryMessageAttributes(filtered)
}

// parseInt32DefaultQ é uma versão interna de parseInt32Default que retorna 0 em erro.
// Diferente do service.go (que retorna default configurável), aqui queremos 0 porque
// é o valor "ausente" para campos opcionais em batch entries.
func parseInt32DefaultQ(s string) int32 {
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int32(n)
}
