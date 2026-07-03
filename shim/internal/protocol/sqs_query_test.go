package protocol

import (
	"encoding/xml"
	"testing"
	"net/http"
	"net/url"
	"strings"
)

func TestIsValidAction_SQS(t *testing.T) {
	known := []Action{
		ActionCreateQueue, ActionGetQueueUrl, ActionGetQueueAttributes,
		ActionListQueues, ActionSendMessage, ActionReceiveMessage,
		ActionDeleteMessage, ActionChangeMessageVisibility,
		ActionPurgeQueue, ActionDeleteQueue, ActionSetQueueAttributes,
		ActionSendMessageBatch, ActionDeleteMessageBatch,
	}
	for _, a := range known {
		if !IsValidAction(ServiceSQS, a) {
			t.Errorf("ação SQS %q deveria ser válida", a)
		}
	}
	unknown := []Action{"FakeAction", "ListBuckets", ""}
	for _, a := range unknown {
		if IsValidAction(ServiceSQS, a) {
			t.Errorf("ação %q não deveria ser válida SQS", a)
		}
	}
}

func TestIsValidAction_SNS(t *testing.T) {
	known := []Action{
		ActionCreateTopic, ActionGetTopicAttributes, ActionListTopics,
		ActionSubscribe, ActionUnsubscribe, ActionPublish,
		ActionListSubscriptions, ActionDeleteTopic, ActionConfirmSubscription,
	}
	for _, a := range known {
		if !IsValidAction(ServiceSNS, a) {
			t.Errorf("ação SNS %q deveria ser válida", a)
		}
	}
}

func TestRequestKind_Validate(t *testing.T) {
	tests := []struct {
		rk   RequestKind
		want bool
	}{
		{RequestKind{Service: ServiceSQS, Action: ActionCreateQueue, Protocol: ProtocolQuery, Region: "us-east-1"}, true},
		{RequestKind{Service: ServiceSQS, Action: ActionCreateQueue, Protocol: ProtocolJSON, Region: "us-east-1"}, true},
		{RequestKind{Service: "rds", Action: "Foo"}, false}, // serviço inválido
		{RequestKind{Service: ServiceSQS, Action: "", Protocol: ProtocolQuery, Region: "us-east-1"}, false}, // action vazia
		{RequestKind{Service: ServiceSQS, Action: ActionCreateQueue, Protocol: "xml", Region: "us-east-1"}, false}, // proto inválido
		{RequestKind{Service: ServiceSQS, Action: ActionCreateQueue, Protocol: ProtocolQuery, AccountID: "123", Region: "us-east-1"}, false}, // account curto
		{RequestKind{Service: ServiceSQS, Action: ActionCreateQueue, Protocol: ProtocolQuery, Region: ""}, false}, // region vazio
	}
	for _, tc := range tests {
		err := tc.rk.Validate()
		if tc.want && err != nil {
			t.Errorf("esperava válido, got erro: %v", err)
		}
		if !tc.want && err == nil {
			t.Errorf("esperava erro, got nil")
		}
	}
}

func TestNewSQSARN(t *testing.T) {
	arn := NewSQSARN("us-east-1", "000000000000", "my-queue")
	want := "arn:aws:sqs:us-east-1:000000000000:my-queue"
	if string(arn) != want {
		t.Errorf("got %q, want %q", arn, want)
	}
}

func TestNewSNSARN(t *testing.T) {
	arn := NewSNSARN("us-east-1", "111122223333", "topic-orders")
	want := "arn:aws:sns:us-east-1:111122223333:topic-orders"
	if string(arn) != want {
		t.Errorf("got %q, want %q", arn, want)
	}
}

func TestNewQueueURL(t *testing.T) {
	u := NewQueueURL("http://localhost:4566", "000000000000", "my-queue")
	want := "http://localhost:4566/000000000000/my-queue"
	if string(u) != want {
		t.Errorf("got %q, want %q", u, want)
	}
}

func TestParseSQSQueryRequest_OK(t *testing.T) {
	body := "Action=CreateQueue&QueueName=my-queue&Version=2012-11-05"
	req, _ := http.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	action, params, err := ParseSQSQueryRequest(req)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if action != ActionCreateQueue {
		t.Errorf("action: got %s", action)
	}
	if params.Get("QueueName") != "my-queue" {
		t.Errorf("QueueName: got %s", params.Get("QueueName"))
	}
	// Version removido
	if params.Has("Version") {
		t.Errorf("Version deveria ter sido removido")
	}
}

func TestParseSQSQueryRequest_MissingAction(t *testing.T) {
	body := "QueueName=foo&Version=2012-11-05"
	req, _ := http.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	_, _, err := ParseSQSQueryRequest(req)
	if err == nil {
		t.Fatal("esperava erro por Action ausente")
	}
}

func TestParseSQSQueryRequest_InvalidAction(t *testing.T) {
	body := "Action=BogusAction&Version=2012-11-05"
	req, _ := http.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	_, _, err := ParseSQSQueryRequest(req)
	if err == nil {
		t.Fatal("esperava erro por Action inválida")
	}
}

func TestParseSQSQueryRequest_WrongMethod(t *testing.T) {
	req, _ := http.NewRequest("GET", "/?Action=CreateQueue", nil)
	_, _, err := ParseSQSQueryRequest(req)
	if err == nil {
		t.Fatal("esperava erro por método errado")
	}
}

func TestEncodeSQSQueryResponse(t *testing.T) {
	type result struct {
		QueueURL string `xml:"QueueUrl"`
	}
	r := result{QueueURL: "http://example.com/000000000000/q"}

	var buf strings.Builder
	err := EncodeSQSQueryResponse(&buf, ActionCreateQueue, r, "req-123")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "CreateQueueResponse") {
		t.Errorf("deveria conter CreateQueueResponse: %s", out)
	}
	if !strings.Contains(out, "<QueueUrl>http://example.com/000000000000/q</QueueUrl>") {
		t.Errorf("deveria conter QueueUrl: %s", out)
	}
	if !strings.Contains(out, "<RequestId>req-123</RequestId>") {
		t.Errorf("deveria conter RequestId: %s", out)
	}
	if !strings.HasPrefix(out, xml.Header) {
		t.Errorf("deveria começar com XML declaration")
	}
}

func TestEncodeSQSQueryError(t *testing.T) {
	var buf strings.Builder
	err := EncodeSQSQueryError(&buf, "QueueAlreadyExists", "queue exists", "req-456", true)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "<Code>QueueAlreadyExists</Code>") {
		t.Errorf("deveria conter Code: %s", out)
	}
	if !strings.Contains(out, "<Type>Sender</Type>") {
		t.Errorf("deveria conter Type=Sender: %s", out)
	}
	if !strings.Contains(out, "queue exists") {
		t.Errorf("deveria conter Message: %s", out)
	}
}

func TestSortValues(t *testing.T) {
	v := url.Values{
		"Zebra": {"z"},
		"Apple": {"a"},
		"Alpha": {"a", "b"},
	}
	got := SortValues(v)
	want := []string{"Alpha=a,b", "Apple=a", "Zebra=z"}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d: %v", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("entry %d: got %s, want %s", i, got[i], want[i])
		}
	}
}
