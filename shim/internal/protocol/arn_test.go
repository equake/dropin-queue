package protocol

import "testing"

// TestResourceNameFromARN valida extração do nome do recurso (queue OU
// topic) do ARN SQS/SNS — formato byte-a-byte idêntico entre os dois.
func TestResourceNameFromARN(t *testing.T) {
	cases := []struct {
		name string
		arn  string
		want string
	}{
		{"SQS", "arn:aws:sqs:us-east-1:000000000000:my-queue", "my-queue"},
		{"SNS", "arn:aws:sns:us-east-1:000000000000:my-topic", "my-topic"},
		{"FIFO", "arn:aws:sqs:us-east-1:000000000000:my-queue.fifo", "my-queue.fifo"},
	}
	for _, c := range cases {
		if got := ResourceNameFromARN(c.arn); got != c.want {
			t.Errorf("ResourceNameFromARN(%s, %s): got %q, want %q",
				c.name, c.arn, got, c.want)
		}
	}
}

// TestResourceNameFromARN_Malformed valida que ARN malformado devolve "".
func TestResourceNameFromARN_Malformed(t *testing.T) {
	cases := []string{
		"",
		"arn:aws:sqs:no-account:queue", // só 5 tokens
		"not-an-arn",
	}
	for _, arn := range cases {
		if got := ResourceNameFromARN(arn); got != "" {
			t.Errorf("ResourceNameFromARN(%q): got %q, want empty string", arn, got)
		}
	}
}

// TestQueueNameFromURL valida extração de queueName tanto de ARN quanto
// de URL HTTP path.
func TestQueueNameFromURL(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
		want     string
	}{
		{"ARN", "arn:aws:sqs:us-east-1:000000000000:my-queue", "my-queue"},
		{"URL_dev", "http://localhost:4566/000000000000/my-queue", "my-queue"},
		{"URL_aws", "https://sqs.us-east-1.amazonaws.com/000000000000/foo", "foo"},
		{"URL_HTTPS", "https://example.com/123456789012/bar.fifo", "bar.fifo"},
		{"ARN_no_account", "arn:aws:sqs:no-account:queue", ""}, // 5 tokens = ARN malformado
	}
	for _, c := range cases {
		if got := QueueNameFromURL(c.endpoint); got != c.want {
			t.Errorf("QueueNameFromURL(%s, %s): got %q, want %q",
				c.name, c.endpoint, got, c.want)
		}
	}
}

// TestQueueNameFromURL_NoPath devolve input se não tem "/" — útil para
// edge cases onde endpoint é só o nome da queue (boto3 às vezes envia).
func TestQueueNameFromURL_NoPath(t *testing.T) {
	if got := QueueNameFromURL("just-a-name"); got != "just-a-name" {
		t.Errorf("got %q, want %q", got, "just-a-name")
	}
}

// TestQueueNameFromURL_TrailingSlash devolve o input original se
// termina em "/" (sem nome extraível). Edge case: caller tem o cuidado
// de validar — não criamos queue com nome vazio inadvertidamente.
func TestQueueNameFromURL_TrailingSlash(t *testing.T) {
	if got := QueueNameFromURL("http://x/"); got != "http://x/" {
		t.Errorf("trailing /: got %q, want %q", got, "http://x/")
	}
}
