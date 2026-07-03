package protocol

import (
	"net/http"
	"strings"
	"testing"
)

func TestParseSQSJSONRequest_OK(t *testing.T) {
	body := `{"QueueName":"my-queue","Attribute":[{"Name":"VisibilityTimeout","Value":"30"}]}`
	req, _ := http.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")

	action, params, err := ParseSQSJSONRequest(req)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if action != ActionCreateQueue {
		t.Errorf("action: got %s", action)
	}
	if params["QueueName"] != "my-queue" {
		t.Errorf("QueueName: got %v", params["QueueName"])
	}

	attrs := ExtractJSONAttributes(params, "Attribute", "Name")
	if attrs["VisibilityTimeout"] != "30" {
		t.Errorf("attrs: got %v", attrs)
	}
}

func TestParseSQSJSONRequest_MissingTarget(t *testing.T) {
	body := `{"QueueName":"foo"}`
	req, _ := http.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	_, _, err := ParseSQSJSONRequest(req)
	if err == nil {
		t.Fatal("esperava erro por X-Amz-Target ausente")
	}
}

func TestParseSQSJSONRequest_InvalidTarget(t *testing.T) {
	body := `{"QueueName":"foo"}`
	req, _ := http.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "BogusService.CreateQueue")

	_, _, err := ParseSQSJSONRequest(req)
	if err == nil {
		t.Fatal("esperava erro por X-Amz-Target com service errado")
	}
}

func TestParseSQSJSONRequest_InvalidAction(t *testing.T) {
	body := `{"QueueName":"foo"}`
	req, _ := http.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.FakeAction")

	_, _, err := ParseSQSJSONRequest(req)
	if err == nil {
		t.Fatal("esperava erro por action inválida")
	}
}

func TestParseSQSJSONRequest_WrongMethod(t *testing.T) {
	req, _ := http.NewRequest("GET", "/", strings.NewReader(""))
	_, _, err := ParseSQSJSONRequest(req)
	if err == nil {
		t.Fatal("esperava erro por método errado")
	}
}

func TestParseSQSJSONRequest_InvalidContentType(t *testing.T) {
	req, _ := http.NewRequest("POST", "/", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")

	_, _, err := ParseSQSJSONRequest(req)
	if err == nil {
		t.Fatal("esperava erro por Content-Type errado")
	}
}

func TestParseSQSJSONRequest_InvalidJSON(t *testing.T) {
	body := `{invalid json`
	req, _ := http.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")

	_, _, err := ParseSQSJSONRequest(req)
	if err == nil {
		t.Fatal("esperava erro por JSON inválido")
	}
}

func TestExtractJSONAttributes_Tags(t *testing.T) {
	params := map[string]any{
		"Tag": []any{
			map[string]any{"Key": "env", "Value": "prod"},
			map[string]any{"Key": "team", "Value": "data"},
		},
	}
	tags := ExtractJSONAttributes(params, "Tag", "Key")
	if tags["env"] != "prod" || tags["team"] != "data" {
		t.Errorf("tags: got %v", tags)
	}
}

func TestExtractJSONAttributes_MapForm(t *testing.T) {
	// Formato compacto usado por boto3 ≥ 1.40.
	params := map[string]any{
		"Attributes": map[string]any{
			"VisibilityTimeout":      "30",
			"MessageRetentionPeriod": "86400",
		},
	}
	attrs := ExtractJSONAttributes(params, "Attribute", "Name")
	if attrs["VisibilityTimeout"] != "30" {
		t.Errorf("VisibilityTimeout: got %v", attrs)
	}
	if attrs["MessageRetentionPeriod"] != "86400" {
		t.Errorf("MessageRetentionPeriod: got %v", attrs)
	}
}

func TestExtractJSONAttributes_TagsMapForm(t *testing.T) {
	params := map[string]any{
		"Tags": map[string]any{
			"env":  "prod",
			"team": "data",
		},
	}
	tags := ExtractJSONAttributes(params, "Tag", "Key")
	if tags["env"] != "prod" || tags["team"] != "data" {
		t.Errorf("tags: got %v", tags)
	}
}

func TestExtractJSONAttributes_Empty(t *testing.T) {
	got := ExtractJSONAttributes(map[string]any{}, "Attribute", "Name")
	if len(got) != 0 {
		t.Errorf("esperava map vazio, got %v", got)
	}
}

func TestExtractJSONAttributes_WrongType(t *testing.T) {
	params := map[string]any{"Attribute": "string-not-array"}
	got := ExtractJSONAttributes(params, "Attribute", "Name")
	if len(got) != 0 {
		t.Errorf("tipo errado deve devolver map vazio, got %v", got)
	}
}

func TestEncodeSQSJSONResponse(t *testing.T) {
	var buf strings.Builder
	err := EncodeSQSJSONResponse(&buf, map[string]string{"QueueUrl": "http://x/y"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out := strings.TrimSpace(buf.String())
	if out != `{"QueueUrl":"http://x/y"}` {
		t.Errorf("got %q", out)
	}
}

func TestEncodeSQSJSONError(t *testing.T) {
	var buf strings.Builder
	err := EncodeSQSJSONError(&buf, "QueueAlreadyExists", "queue already exists")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out := strings.TrimSpace(buf.String())
	if !strings.Contains(out, `"__type":"QueueAlreadyExistsException"`) {
		t.Errorf("__type: got %s", out)
	}
	if !strings.Contains(out, `"message":"queue already exists"`) {
		t.Errorf("message: got %s", out)
	}
}
