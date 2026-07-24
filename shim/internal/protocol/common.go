package protocol

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/equake/dropin-queue/shim/pkg/types"
)

// MaxWireBodyBytes limita tamanho de body wire parseável. Defesa em
// profundidade — server/http.go também aplica cfg.MaxRequestBodyBytes
// ANTES do Parse. 10 MB cobre SQS+SNS no pior caso (Publish com 256 KiB
// + envelope + assinatura × alguma margem).
const MaxWireBodyBytes = 10 << 20

// ServiceName identifica qual AWS API está sendo usada no request.
// (Já definido em types.go — re-aproveitamos aqui.)
//
// Pré-refactor (refactor/kiss-dry-pass-1): cada uma das funções
// ParseSQSQueryRequest / ParseSNSQueryRequest fazia a mesma
// sequência (valida método → valida Content-Type → lê body → parseia
// form → extrai Action → valida no set do serviço) com pequenas
// variações de error string. Centralizado em parseWireQueryRequest
// parametrizado por ServiceName + label.

// parseWireQueryRequest faz parse de qualquer wire Query (form-encoded)
// request — SQS ou SNS — parametrizado pelo Service para validar o
// Action no set correto.
//
// Formato esperado:
//
//	POST / HTTP/1.1
//	Content-Type: application/x-www-form-urlencoded
//	X-Amz-Date: 20240101T000000Z
//	X-Amz-Algorithm: AWS4-HMAC-SHA256
//
//	Action=CreateQueue&QueueName=myqueue&Version=2012-11-05
//
// label é "SQS" ou "SNS" — usado em mensagens de erro para distinguir
// qual protocolo estava sendo parseado (melhor debuggabilidade que
// erro genérico).
//
// Pré-refactor (refactor/kiss-dry-pass-1): ~40 linhas de lógica
// idênticas em ParseSQSQueryRequest + ParseSNSQueryRequest. Centralizado.
func parseWireQueryRequest(r *http.Request, svc ServiceName, label string) (Action, url.Values, error) {
	if r.Method != http.MethodPost {
		return "", nil, fmt.Errorf("%s Query requer POST, recebeu %s", label, r.Method)
	}

	ct := r.Header.Get("Content-Type")
	if ct != "" && !strings.HasPrefix(ct, "application/x-www-form-urlencoded") {
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
	if !IsValidAction(svc, action) {
		return "", nil, fmt.Errorf("Action %s inválido: %q", label, action)
	}

	// Remover Version e Action dos params (são metadados, não parâmetros da op).
	values.Del("Version")
	values.Del("Action")

	return action, values, nil
}

// ParseSQSQueryRequest delega ao parseWireQueryRequest. Mantido como
// entry point para preservar call sites existentes.
func ParseSQSQueryRequest(r *http.Request) (Action, url.Values, error) {
	return parseWireQueryRequest(r, ServiceSQS, "SQS")
}

// ParseSNSQueryRequest delega ao parseWireQueryRequest.
func ParseSNSQueryRequest(r *http.Request) (Action, url.Values, error) {
	return parseWireQueryRequest(r, ServiceSNS, "SNS")
}

// parseWireJSONRequest faz parse de qualquer wire JSON 1.0 request —
// SQS ou SNS — parametrizado pelo Service.
//
// Formato esperado:
//
//	POST / HTTP/1.1
//	Content-Type: application/x-amz-json-1.0
//	X-Amz-Target: <serviceNamespace>.<Action>
//
//	{"QueueName":"my-queue","Attribute":[{"Name":"VisibilityTimeout","Value":"30"}]}
//
// É obrigatório que Content-Type comece com "application/x-amz-json"
// (o servidor infere namespace SQS vs SNS via prefix X-Amz-Target).
//
// O X-Amz-Target deve começar com "AmazonSQS." ou "AmazonSNS." conforme
// o Service — client mandando namespace errado (BogusService.X) é
// classificado como 4xx senderFault.
func parseWireJSONRequest(r *http.Request, svc ServiceName) (Action, map[string]any, error) {
	if r.Method != http.MethodPost {
		return "", nil, fmt.Errorf("%s JSON requer POST, recebeu %s", svc, r.Method)
	}

	ct := r.Header.Get("Content-Type")
	if ct == "" || !strings.HasPrefix(ct, "application/x-amz-json") {
		return "", nil, fmt.Errorf("Content-Type esperado application/x-amz-json-*, recebeu %q", ct)
	}

	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		return "", nil, errors.New("header X-Amz-Target ausente")
	}

	// X-Amz-Target formato: "<Namespace>.<Action>"
	// Namespace identifica o serviço (AmazonSQS. ou AmazonSNS.).
	// Pré-refactor checava o prefix string. Aqui usamos IsValidAction
	// contra o Service correto para validar o Action
	// (parsing mais robusto que string-compare).
	dot := strings.IndexByte(target, '.')
	if dot < 0 {
		return "", nil, fmt.Errorf("X-Amz-Target deve conter '.', recebeu %q", target)
	}
	prefix := target[:dot+1]
	expectedPrefix := ""
	switch svc {
	case ServiceSQS:
		expectedPrefix = "AmazonSQS."
	case ServiceSNS:
		expectedPrefix = "AmazonSNS."
	}
	if prefix != expectedPrefix {
		return "", nil, fmt.Errorf("X-Amz-Target deve começar com %q, recebeu %q", expectedPrefix, prefix)
	}
	rawAction := Action(target[dot+1:])
	if !IsValidAction(svc, rawAction) {
		return "", nil, fmt.Errorf("Action %s inválido em X-Amz-Target: %q", svc, rawAction)
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, MaxWireBodyBytes))
	if err != nil {
		return "", nil, fmt.Errorf("ler body: %w", err)
	}
	defer func() { _ = r.Body.Close() }()

	if len(body) == 0 {
		return "", nil, errors.New("body vazio")
	}

	params := make(map[string]any)
	if err := json.Unmarshal(body, &params); err != nil {
		return "", nil, fmt.Errorf("parse JSON: %w", err)
	}

	return rawAction, params, nil
}

// ParseSQSJSONRequest delega ao parseWireJSONRequest. Mantido como
// entry point para preservar call sites.
func ParseSQSJSONRequest(r *http.Request) (Action, map[string]any, error) {
	return parseWireJSONRequest(r, ServiceSQS)
}

// ParseSNSJSONRequest delega ao parseWireJSONRequest.
func ParseSNSJSONRequest(r *http.Request) (Action, map[string]any, error) {
	return parseWireJSONRequest(r, ServiceSNS)
}

// --- Encoder compartilhado para Query results. ---

// writeQueryEnvelope escreve o envelope XML padrão AWS para respostas
// Query de sucesso (RequestId + action-specific result).
//
// Format:
//
//	<?xml version="1.0" encoding="UTF-8"?>
//	<Action>Response xmlns="<namespace-doc-version>">
//	  <ActionResult>
//	    ... campos específicos da operação ...
//	  </ActionResult>
//	  <ResponseMetadata>
//	    <RequestId>...</RequestId>
//	  </ResponseMetadata>
//	</Action>Response>
//
// actionResult: structure específica da operação (struct com tags xml).
// requestID: valor injetado em ResponseMetadata.
func writeQueryEnvelope(w io.Writer, action Action, namespace string,
	actionResult interface{}, requestID string) error {
	resp := struct {
		XMLName               xml.Name
		Xmlns                 string           `xml:"xmlns,attr"`
		Result                interface{}      `xml:",omitempty"`
		ResponseMetadataInner ResponseMetadata `xml:"ResponseMetadata"`
	}{
		XMLName: xml.Name{Local: string(action) + "Response"},
		Xmlns:   namespace,
		Result:  actionResult,
		ResponseMetadataInner: ResponseMetadata{
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

// ResponseMetadata é o envelope padrão de toda resposta AWS.
type ResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

// EncodeSQSQueryResponse usa o envelope compartilhado writeQueryEnvelope
// com namespace SQS. Mantido para preservar call sites existentes.
func EncodeSQSQueryResponse(w io.Writer, action Action, actionResult interface{}, requestID string) error {
	return writeQueryEnvelope(w, action,
		"http://queue.amazonaws.com/doc/"+AWSProtocolVersion,
		actionResult, requestID)
}

// EncodeSNSQueryResponse usa o envelope compartilhado com namespace SNS.
func EncodeSNSQueryResponse(w io.Writer, action Action, actionResult interface{}, requestID string) error {
	return writeQueryEnvelope(w, action,
		"http://sns.amazonaws.com/doc/"+SNSProtocolVersion,
		actionResult, requestID)
}

// --- Encoder compartilhado para Query errors. ---

// ErrorEnvelopeXML é envelope padrão AWS para erros Query:
//
//	<?xml version="1.0" encoding="UTF-8"?>
//	<ErrorResponse>
//	  <Error>
//	    <Type>Sender|Receiver</Type>
//	    <Code>...</Code>
//	    <Message>...</Message>
//	  </Error>
//	  <RequestId>...</RequestId>
//	</ErrorResponse>
//
// Type é derivado de senderFault (true → "Sender", false → "Receiver").
type ErrorEnvelopeXML struct {
	XMLName   xml.Name  `xml:"ErrorResponse"`
	Error     ErrorBody `xml:"Error"`
	RequestID string    `xml:"RequestId"`
}

// ErrorBody é o sub-elemento <Error>.
type ErrorBody struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func writeQueryError(w io.Writer, code, message, requestID string, senderFault bool) error {
	typ := "Receiver"
	if senderFault {
		typ = "Sender"
	}
	env := ErrorEnvelopeXML{
		Error: ErrorBody{
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

// EncodeSQSQueryError delega ao writeQueryError.
func EncodeSQSQueryError(w io.Writer, code, message, requestID string, senderFault bool) error {
	return writeQueryError(w, code, message, requestID, senderFault)
}

// EncodeSNSQueryError delega ao writeQueryError. a função de SQS é
// byte-a-byte idêntica à de SNS — Amazon definiu o envelope Error
// da mesma forma para os dois serviços (a versão e o namespace
// diferenciam só na response).
func EncodeSNSQueryError(w io.Writer, code, message, requestID string, senderFault bool) error {
	return writeQueryError(w, code, message, requestID, senderFault)
}

// --- JSON encoders (simples — não compartilhavam duplicação significativa). ---

// EncodeSQSJSONResponse serializa uma resposta SQS JSON (formato
// simples: objeto JSON com os campos do result).
func EncodeSQSJSONResponse(w io.Writer, result any) error {
	return json.NewEncoder(w).Encode(result)
}

// EncodeSNSJSONResponse serializa uma resposta SNS JSON.
func EncodeSNSJSONResponse(w io.Writer, result any) error {
	return json.NewEncoder(w).Encode(result)
}

// EncodeSQSJSONError serializa erro SQS JSON 1.0:
//
//	{"__type":"<Code>","message":"<Message>"}
//
// __type PRECISA ser o nome exato do shape modelado pela AWS (ex.:
// "QueueDoesNotExist"), sem sufixo. botocore faz match exato via
// error_shape.error_code — um "Exception" extra faz o SDK oficial
// cair sempre em ClientError genérico em vez da exceção tipada
// (ex.: client.exceptions.QueueDoesNotExist nunca dispara).
func EncodeSQSJSONError(w io.Writer, code, message string) error {
	resp := map[string]string{
		"__type":  code,
		"message": message,
	}
	return json.NewEncoder(w).Encode(resp)
}

// EncodeSNSJSONError serializa erro SNS JSON 1.0 — formato
// byte-a-byte idêntico ao SQS (Amazon usou o mesmo envelope).
func EncodeSNSJSONError(w io.Writer, code, message string) error {
	resp := map[string]string{
		"__type":  code,
		"message": message,
	}
	return json.NewEncoder(w).Encode(resp)
}

// dummy usage to keep types import (compile check)
var _ types.MessageAttribute
