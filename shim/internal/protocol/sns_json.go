package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// ParseSNSJSONRequest faz parse de um request SNS no protocolo AWS JSON 1.0.
//
// Formato esperado:
//
//	POST / HTTP/1.1
//	Content-Type: application/x-amz-json-1.0
//	X-Amz-Target: AmazonSNS.CreateTopic
//
//	{"Name": "my-topic"}
func ParseSNSJSONRequest(r *http.Request) (Action, map[string]any, error) {
	if r.Method != http.MethodPost {
		return "", nil, fmt.Errorf("SNS JSON requer POST, recebeu %s", r.Method)
	}

	ct := r.Header.Get("Content-Type")
	if ct == "" || !strings.HasPrefix(ct, "application/x-amz-json") {
		return "", nil, fmt.Errorf("Content-Type esperado application/x-amz-json-*, recebeu %q", ct)
	}

	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		return "", nil, errors.New("header X-Amz-Target ausente")
	}
	const prefix = "AmazonSNS."
	if !strings.HasPrefix(target, prefix) {
		return "", nil, fmt.Errorf("X-Amz-Target deve começar com %q, recebeu %q", prefix, target)
	}
	action := Action(target[len(prefix):])
	if !IsValidAction(ServiceSNS, action) {
		return "", nil, fmt.Errorf("Action SNS inválido em X-Amz-Target: %q", action)
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

	return action, params, nil
}

// EncodeSNSJSONResponse serializa uma resposta SNS JSON.
func EncodeSNSJSONResponse(w io.Writer, result any) error {
	return json.NewEncoder(w).Encode(result)
}

// EncodeSNSJSONError serializa um erro SNS no formato JSON 1.0.
//
// Mesmo formato dos erros SQS JSON 1.0.
func EncodeSNSJSONError(w io.Writer, code, message string) error {
	resp := map[string]string{
		"__type":  code + "Exception",
		"message": message,
	}
	return json.NewEncoder(w).Encode(resp)
}
