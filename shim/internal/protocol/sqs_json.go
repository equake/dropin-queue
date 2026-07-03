package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// maxJSONBodyBytes limita tamanho de body JSON parseável.
const maxJSONBodyBytes = 10 << 20 // 10 MB

// ParseSQSJSONRequest faz parse de um request SQS no protocolo AWS JSON 1.0.
//
// Formato esperado:
//
//	POST / HTTP/1.1
//	Content-Type: application/x-amz-json-1.0
//	X-Amz-Target: AmazonSQS.CreateQueue
//
//	{"QueueName":"my-queue","Attribute":[{"Name":"VisibilityTimeout","Value":"30"}]}
//
// Retorna:
//
//   - action: extraída do X-Amz-Target (após "AmazonSQS.")
//   - params: map[string]any com os campos do JSON
//   - err: erro de parsing/validação
func ParseSQSJSONRequest(r *http.Request) (Action, map[string]any, error) {
	if r.Method != http.MethodPost {
		return "", nil, fmt.Errorf("SQS JSON requer POST, recebeu %s", r.Method)
	}

	ct := r.Header.Get("Content-Type")
	if ct == "" || !strings.HasPrefix(ct, "application/x-amz-json") {
		return "", nil, fmt.Errorf("Content-Type esperado application/x-amz-json-*, recebeu %q", ct)
	}

	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		return "", nil, errors.New("header X-Amz-Target ausente")
	}
	// Formato: "AmazonSQS.<Action>"
	const prefix = "AmazonSQS."
	if !strings.HasPrefix(target, prefix) {
		return "", nil, fmt.Errorf("X-Amz-Target deve começar com %q, recebeu %q", prefix, target)
	}
	action := Action(target[len(prefix):])
	if !IsValidAction(ServiceSQS, action) {
		return "", nil, fmt.Errorf("Action SQS inválida em X-Amz-Target: %q", action)
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSONBodyBytes))
	if err != nil {
		return "", nil, fmt.Errorf("ler body: %w", err)
	}
	defer r.Body.Close()

	if len(body) == 0 {
		return "", nil, errors.New("body vazio")
	}

	params := make(map[string]any)
	if err := json.Unmarshal(body, &params); err != nil {
		return "", nil, fmt.Errorf("parse JSON: %w", err)
	}

	return action, params, nil
}

// EncodeSQSJSONResponse serializa uma resposta SQS JSON.
//
// O AWS SDK aceita {"__type": "...", ...} em erros, mas para sucessos
// é simplesmente o JSON do result.
func EncodeSQSJSONResponse(w io.Writer, result any) error {
	return json.NewEncoder(w).Encode(result)
}

// EncodeSQSJSONError serializa um erro SQS no formato JSON 1.0.
//
// Formato AWS:
//
//	{"__type":"QueueAlreadyExistsException","message":"..."}
//
// O __type segue convenção "<Code>Exception".
func EncodeSQSJSONError(w io.Writer, code, message string) error {
	resp := map[string]string{
		"__type":  code + "Exception",
		"message": message,
	}
	return json.NewEncoder(w).Encode(resp)
}

// JSONAttribute representa um atributo SQS no formato JSON 1.0.
//
// Formato: [{"Name":"VisibilityTimeout","Value":"30"}]
type JSONAttribute struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// JSONTag representa uma tag SQS no formato JSON 1.0.
//
// Formato: [{"Key":"env","Value":"prod"}]
type JSONTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// ExtractJSONAttributes extrai pares do JSON params.
//
// AWS SQS JSON 1.0 aceita dois formatos para Attribute/Tag:
//
// Formato 1 (documentado AWS):
//
//	"Attribute": [{"Name":"VisibilityTimeout","Value":"30"}, ...]
//	"Tag":       [{"Key":"env","Value":"prod"}, ...]
//
// Formato 2 (forma compacta, usada por boto3 ≥ 1.40 e smithy-rs):
//
//	"Attributes": {"VisibilityTimeout": "30"}
//	"Tags":       {"env": "prod"}
//
// O parâmetro `key` é a chave principal ("Attribute"/"Attributes" etc).
// Quando termina em 's', tratamos como formato 2 (map direto).
// Quando é singular, tratamos como formato 1 (array).
//
// O parâmetro `idField` indica qual campo é o identificador
// ("Name" para Attribute, "Key" para Tag).
//
// Retorna map[string]string com os pares parseados.
func ExtractJSONAttributes(params map[string]any, key, idField string) map[string]string {
	out := make(map[string]string)
	raw, ok := params[key]
	if !ok {
		// Tenta a forma singular/plural alternativa.
		if strings.HasSuffix(key, "s") {
			raw, ok = params[key[:len(key)-1]]
			if !ok {
				return out
			}
		} else {
			raw, ok = params[key+"s"]
			if !ok {
				return out
			}
		}
	}

	// Formato 2: map[string]any direto.
	if m, ok := raw.(map[string]any); ok {
		for k, v := range m {
			if s, ok := v.(string); ok {
				out[k] = s
			}
		}
		return out
	}

	// Formato 1: array de objetos.
	arr, ok := raw.([]any)
	if !ok {
		return out
	}
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		id, _ := m[idField].(string)
		value, _ := m["Value"].(string)
		if id != "" {
			out[id] = value
		}
	}
	return out
}

// JSONMessageAttribute representa um MessageAttribute SQS no JSON 1.0.
//
// Formato (documentado AWS):
//
//	{"Name": "foo", "DataType": "String", "StringValue": "bar"}
//	{"Name": "count", "DataType": "Number", "StringValue": "42"}
//	{"Name": "blob", "DataType": "Binary", "BinaryValue": "base64..."}
//	{"Name": "tags", "DataType": "String.List", "StringListValues": ["a","b"]}
type JSONMessageAttribute struct {
	Name             string   `json:"Name"`
	DataType         string   `json:"DataType"`
	StringValue      string   `json:"StringValue,omitempty"`
	BinaryValue      string   `json:"BinaryValue,omitempty"` // base64
	StringListValues []string `json:"StringListValues,omitempty"`
}

// ExtractJSONMessageAttributes extrai MessageAttributes do JSON params.
//
// Suporta 2 formatos:
//
// Documentado:
//
//	"MessageAttribute": [
//	  {"Name":"foo","DataType":"String","StringValue":"bar"},
//	  ...
//	]
//
// Compacto (boto3 ≥ 1.40):
//
//	"MessageAttributes": {
//	  "foo": {"DataType":"String","StringValue":"bar"},
//	  ...
//	}
//
// Retorna map[string]MessageAttributeValue pronto para conversão em
// types.MessageAttribute.
func ExtractJSONMessageAttributes(params map[string]any) map[string]MessageAttributeValue {
	out := make(map[string]MessageAttributeValue)

	// Tenta formato compacto primeiro (boto3 ≥ 1.40 envia este).
	if raw, ok := params["MessageAttributes"]; ok {
		if m, ok := raw.(map[string]any); ok {
			for name, v := range m {
				if obj, ok := v.(map[string]any); ok {
					parseJSONMsgAttr(out, name, obj)
				}
			}
			return out
		}
	}

	// Formato documentado (array).
	if raw, ok := params["MessageAttribute"]; ok {
		if arr, ok := raw.([]any); ok {
			for _, item := range arr {
				if obj, ok := item.(map[string]any); ok {
					name, _ := obj["Name"].(string)
					if name == "" {
						continue
					}
					parseJSONMsgAttr(out, name, obj)
				}
			}
		}
	}

	return out
}

// parseJSONMsgAttr processa um objeto JSON individual e adiciona ao map out.
func parseJSONMsgAttr(out map[string]MessageAttributeValue, name string, obj map[string]any) {
	dt, _ := obj["DataType"].(string)
	if dt == "" {
		return
	}
	switch dt {
	case "String", "Number":
		sv, _ := obj["StringValue"].(string)
		out[name] = MessageAttributeValue{DataType: dt, StringValue: sv}
	case "Binary":
		bv, _ := obj["BinaryValue"].(string)
		out[name] = MessageAttributeValue{
			DataType:    "Binary",
			BinaryValue: decodeBase64(bv),
		}
	case "String.List":
		arr, _ := obj["StringListValues"].([]any)
		items := make([]string, 0, len(arr))
		for _, v := range arr {
			if s, ok := v.(string); ok {
				items = append(items, s)
			}
		}
		out[name] = MessageAttributeValue{
			DataType:    "String.List",
			StringValue: joinList(items),
		}
	}
}
