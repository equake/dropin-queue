package protocol

import (
	"encoding/base64"

	"github.com/equake/dropin-queue/shim/pkg/types"
)

// MessageAttributeValue é o tipo intermediário entre o que vem da wire
// (SQS Query e JSON, SNS Query e JSON) e o que o storage espera.
//
// DataType controla como StringValue/BinaryValue são interpretados:
//
//   - "String":       value = string
//   - "Number":       value = string com digits (mantido como string para
//     preservar precisão)
//   - "Binary":       value = []byte (base64-decoded na leitura)
//   - "String.List":  value = items joined por "|" (ex: "a|b|c")
//
// Pré-refactor (refactor/kiss-dry-pass-1) o campo `StringValue` em SQS e
// SNS usava formas diferentes para String.List. Agora centralizado aqui.
type MessageAttributeValue struct {
	DataType    string
	StringValue string
	BinaryValue []byte
}

// MessageAttributeToTypes converte do tipo intermediário (wire) para o
// tipo interno (storage). Centralizado: pré-refactor existia em
// sqs/service.go e sns/service.go byte-a-byte idêntico (duplicação).
func MessageAttributeToTypes(in map[string]MessageAttributeValue) map[string]types.MessageAttribute {
	if in == nil {
		return nil
	}
	out := make(map[string]types.MessageAttribute, len(in))
	for k, v := range in {
		out[k] = types.MessageAttribute{
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}
	return out
}

// MessageAttributesSize soma o tamanho on-wire dos atributos (nome +
// DataType + valores) em bytes.
//
// AWS SQS/SNS conta estes bytes DENTRO do limite de MaximumMessageSize
// (256 KiB). Pré-refactor (refactor/kiss-dry-pass-1): cálculo implementado
// em sqs/service.go como função nomeada + inline em sns/service.go
// (potencial divergência futura). Centralizado aqui.
//
// NOTA: para String.List, StringValue contém os items joined por "|";
// contaria o resultado final (size já correto).
func MessageAttributesSize(attrs map[string]MessageAttributeValue) int {
	total := 0
	for name, v := range attrs {
		total += len(name)
		total += len(v.DataType)
		total += len(v.StringValue)
		total += len(v.BinaryValue)
	}
	return total
}

// EncodeBase64 mantém o encode usado em JSON 1.0 — exposto aqui caso
// outros pacotes precisem (handler JSON de ReceiveMessage).
func EncodeBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}
