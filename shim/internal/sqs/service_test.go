package sqs

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/anomalyco/generic_queue/shim/pkg/types"
)

func TestIsValidQueueName(t *testing.T) {
	cases := []struct {
		name string
		ok   bool
	}{
		{"my-queue", true},
		{"my_queue", true},
		{"MyQueue123", true},
		{"queue.with.dots", true},
		{"a", true},
		{"", false},
		{"queue.with.spaces and more", false},
		{".fifo", true},        // suffix válido
		{"my.fifo.queue", false}, // .fifo deve ser no final
		{"queue.com.unicode.ç", false},
	}
	for _, tc := range cases {
		got := isValidQueueName(tc.name)
		if got != tc.ok {
			t.Errorf("isValidQueueName(%q) = %v, want %v", tc.name, got, tc.ok)
		}
	}
}

func TestIsValidQueueName_Length(t *testing.T) {
	short := "a"
	if !isValidQueueName(short) {
		t.Errorf("nome de 1 char deve ser válido")
	}
	long := make([]byte, 81)
	for i := range long {
		long[i] = 'a'
	}
	if isValidQueueName(string(long)) {
		t.Errorf("nome > 80 chars deve ser inválido")
	}
	max := make([]byte, 80)
	for i := range max {
		max[i] = 'a'
	}
	if !isValidQueueName(string(max)) {
		t.Errorf("nome de 80 chars deve ser válido")
	}
}

func TestExtractAttributes(t *testing.T) {
	v := url.Values{
		"Attribute.1.Name":          {"VisibilityTimeout"},
		"Attribute.1.Value":         {"60"},
		"Attribute.2.Name":          {"MaximumMessageSize"},
		"Attribute.2.Value":         {"65536"},
		"Attribute.3.Name":          {"FifoQueue"},
		"Attribute.3.Value":         {"true"},
		"Tag.1.Name":                {"env"},
		"Tag.1.Value":               {"prod"},
	}
	got := extractAttributes(v, "Attribute")
	want := map[string]string{
		"VisibilityTimeout":  "60",
		"MaximumMessageSize": "65536",
		"FifoQueue":          "true",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("extractAttributes: got %v, want %v", got, want)
	}

	tags := extractAttributes(v, "Tag")
	if tags["env"] != "prod" {
		t.Errorf("tags: got %v", tags)
	}
}

func TestExtractAttributes_OutOfOrder(t *testing.T) {
	// Cliente pode mandar Value antes de Name (em teoria). Testamos ordem Name->Value.
	v := url.Values{
		"Attribute.10.Name":  {"DelaySeconds"},
		"Attribute.10.Value": {"5"},
	}
	got := extractAttributes(v, "Attribute")
	if got["DelaySeconds"] != "5" {
		t.Errorf("got %v", got)
	}
}

func TestParseInt32Default(t *testing.T) {
	if parseInt32Default("", 42) != 42 {
		t.Errorf("vazio deve usar default")
	}
	if parseInt32Default("100", 42) != 100 {
		t.Errorf("100 deve virar 100")
	}
	if parseInt32Default("invalid", 42) != 42 {
		t.Errorf("inválido deve usar default")
	}
}

func TestValidateAttributes_OK(t *testing.T) {
	a := types.DefaultQueueAttributes()
	if err := validateAttributes(a); err != nil {
		t.Errorf("defaults devem validar: %v", err)
	}
}

func TestValidateAttributes_OutOfRange(t *testing.T) {
	tests := []struct {
		mutate func(a *types.QueueAttributes)
		field  string
	}{
		{func(a *types.QueueAttributes) { a.VisibilityTimeout = -1 }, "VisibilityTimeout"},
		{func(a *types.QueueAttributes) { a.VisibilityTimeout = 99999 }, "VisibilityTimeout"},
		{func(a *types.QueueAttributes) { a.MessageRetentionPeriod = 30 }, "MessageRetentionPeriod"},
		{func(a *types.QueueAttributes) { a.MessageRetentionPeriod = 99999999 }, "MessageRetentionPeriod"},
		{func(a *types.QueueAttributes) { a.MaximumMessageSize = 100 }, "MaximumMessageSize"},
		{func(a *types.QueueAttributes) { a.DelaySeconds = 1000 }, "DelaySeconds"},
		{func(a *types.QueueAttributes) { a.ReceiveMessageWaitTimeSeconds = 30 }, "ReceiveMessageWaitTimeSeconds"},
	}
	for _, tc := range tests {
		a := types.DefaultQueueAttributes()
		tc.mutate(&a)
		err := validateAttributes(a)
		if err == nil {
			t.Errorf("esperava erro para %s fora de range", tc.field)
		}
		awsErr := AsAWSError(err)
		if awsErr == nil {
			t.Errorf("esperava AWSError, got nil")
		} else if awsErr.Code != ErrCodeInvalidParameterValue {
			t.Errorf("código: got %s, want %s", awsErr.Code, ErrCodeInvalidParameterValue)
		}
	}
}

func TestAWSError_IsSenderFault(t *testing.T) {
	senderCases := []string{
		ErrCodeQueueAlreadyExists, ErrCodeQueueDoesNotExist,
		ErrCodeInvalidParameterValue, ErrCodeMissingParameter,
		ErrCodeOverLimit,
	}
	for _, code := range senderCases {
		e := &AWSError{Code: code}
		if !e.IsSenderFault() {
			t.Errorf("%s deveria ser sender fault", code)
		}
	}
	receiverCases := []string{ErrCodeInternalError, ErrCodeUnsupportedOperation}
	for _, code := range receiverCases {
		e := &AWSError{Code: code}
		if e.IsSenderFault() {
			t.Errorf("%s NÃO deveria ser sender fault", code)
		}
	}
}

func TestAsAWSError_Nil(t *testing.T) {
	if AsAWSError(nil) != nil {
		t.Errorf("nil deve devolver nil")
	}
}
