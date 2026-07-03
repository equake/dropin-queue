package protocol

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// ParseSNSQueryRequest faz parse de um request SNS no protocolo Query
// (form-encoded body, X-Amz-* em headers).
//
// Formato esperado:
//
//	POST / HTTP/1.1
//	Content-Type: application/x-www-form-urlencoded
//	X-Amz-Date: 20240101T000000Z
//
//	Action=CreateTopic&Name=my-topic&Version=2010-03-31
func ParseSNSQueryRequest(r *http.Request) (Action, url.Values, error) {
	if r.Method != http.MethodPost {
		return "", nil, fmt.Errorf("SNS Query requer POST, recebeu %s", r.Method)
	}

	ct := r.Header.Get("Content-Type")
	if ct != "" && !strings.HasPrefix(ct, "application/x-www-form-urlencoded") {
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
	if !IsValidAction(ServiceSNS, action) {
		return "", nil, fmt.Errorf("Action SNS inválido: %q", action)
	}

	// Remover Version e Action dos params.
	values.Del("Version")
	values.Del("Action")

	return action, values, nil
}

// SNSQueryResponse é o envelope de resposta bem-sucedida de qualquer operação SNS Query.
//
// Formato AWS:
//
//	<?xml version="1.0" encoding="UTF-8"?>
//	<Action>Response xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
//	  <ActionResult>
//	    ... campos específicos da operação ...
//	  </ActionResult>
//	  <ResponseMetadata>
//	    <RequestId>...</RequestId>
//	  </ResponseMetadata>
//	</Action>Response>
type SNSQueryResponse struct {
	XMLName          xml.Name
	Xmlns            string      `xml:"xmlns,attr"`
	Result           interface{} `xml:",omitempty"`
	ResponseMetadata ResponseMetadata
}

// SNSErrorEnvelope é a estrutura de erros SNS Query.
type SNSErrorEnvelope struct {
	XMLName   xml.Name `xml:"ErrorResponse"`
	Error     SQSError `xml:"Error"`
	RequestID string   `xml:"RequestId"`
}

// EncodeSNSQueryResponse serializa uma resposta SNS no formato XML.
func EncodeSNSQueryResponse(w io.Writer, action Action, actionResult interface{}, requestID string) error {
	resp := SNSQueryResponse{
		XMLName: xml.Name{Local: string(action) + "Response"},
		Xmlns:   "http://sns.amazonaws.com/doc/" + SNSProtocolVersion,
		Result:  actionResult,
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

// EncodeSNSQueryError serializa um erro SNS no formato XML.
func EncodeSNSQueryError(w io.Writer, code, message, requestID string, senderFault bool) error {
	typ := "Receiver"
	if senderFault {
		typ = "Sender"
	}
	env := SNSErrorEnvelope{
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
