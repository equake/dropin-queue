package nats

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/anomalyco/generic_queue/shim/pkg/types"
)

func TestSanitizeStreamName(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"my-queue", "my-queue"},
		{"my_queue", "my_queue"},
		{"MyQueue123", "MyQueue123"},
		{"queue.with.dots", "queue_with_dots"},
		// "queue com espaços" tem 17 bytes UTF-8 ('ç' = 2 bytes), então vira:
		// "queue_com_espa__os" (cada byte não-alfanumérico → '_').
		{"queue com espaços", "queue_com_espa__os"},
		// "queue-with-unicode-ç" tem 19 bytes UTF-8, vira:
		// "queue-with-unicode-__" (ç = 0xc3 0xa7, ambos viram '_').
		{"queue-with-unicode-ç", "queue-with-unicode-__"},
		{"queue.with/slash", "queue_with_slash"},
		{"a/b/c", "a_b_c"},
	}
	for _, tc := range tests {
		got := sanitizeStreamName(tc.in)
		if got != tc.want {
			t.Errorf("sanitize(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSanitizeStreamName_Length(t *testing.T) {
	long := make([]byte, 300)
	for i := range long {
		long[i] = 'a'
	}
	got := sanitizeStreamName(string(long))
	if len(got) > 255 {
		t.Errorf("output deve ter <= 255 chars, got %d", len(got))
	}
}

func TestStreamCfg_Defaults(t *testing.T) {
	c := &Client{prefix: ""}
	q := types.Queue{
		Name: "test-queue",
		Attributes: types.DefaultQueueAttributes(),
	}
	cfg := c.streamCfg(q, true)

	if cfg.Name != "queue-test-queue" {
		t.Errorf("Name: got %s", cfg.Name)
	}
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != "q.test-queue.>" {
		t.Errorf("Subjects: got %v", cfg.Subjects)
	}
	if cfg.Replicas != 1 {
		t.Errorf("dev mode deve ter 1 réplica, got %d", cfg.Replicas)
	}
	if cfg.Storage != jetstream.FileStorage {
		t.Errorf("Storage deve ser File, got %v", cfg.Storage)
	}
	if cfg.Retention != jetstream.LimitsPolicy {
		t.Errorf("Retention deve ser Limits, got %v", cfg.Retention)
	}
	if cfg.MaxMsgs != 10_000_000 {
		t.Errorf("MaxMsgs default: got %d", cfg.MaxMsgs)
	}
}

func TestStreamCfg_CustomRetention(t *testing.T) {
	c := &Client{prefix: ""}
	q := types.Queue{
		Name: "x",
		Attributes: types.QueueAttributes{
			MessageRetentionPeriod: 86400, // 1 dia
		},
	}
	cfg := c.streamCfg(q, true)
	want := 86400 * time.Second
	if cfg.MaxAge != want {
		t.Errorf("MaxAge: got %v, want %v", cfg.MaxAge, want)
	}
}

func TestStreamCfg_ProductionReplicas(t *testing.T) {
	c := &Client{prefix: "tenant1"} // prefix != "" indica prod
	q := types.Queue{Name: "x"}
	cfg := c.streamCfg(q, c.isDevMode())
	if cfg.Replicas != 3 {
		t.Errorf("prod mode deve ter 3 réplicas, got %d", cfg.Replicas)
	}
}

func TestStreamCfg_FIFO(t *testing.T) {
	c := &Client{prefix: ""}
	q := types.Queue{Name: "my-queue.fifo", FIFO: true}
	cfg := c.streamCfg(q, true)
	if !q.FIFO {
		t.Error("FIFO flag deve estar setada")
	}
	// Sanity: subjects seguem mesmo padrão (FIFO é tratado por camada superior)
	if cfg.Subjects[0] != "q.my-queue.fifo.>" {
		t.Errorf("Subjects: got %v", cfg.Subjects)
	}
}

func TestIsDevMode(t *testing.T) {
	if !(&Client{prefix: ""}).isDevMode() {
		t.Error("prefix vazio deve ser dev mode")
	}
	if (&Client{prefix: "tenant1"}).isDevMode() {
		t.Error("prefix != '' deve ser prod mode")
	}
}

func TestHasPrefix(t *testing.T) {
	if !hasPrefix("queue-foo", "queue-") {
		t.Error("hasPrefix deve aceitar prefixo correto")
	}
	if hasPrefix("qfoo", "queue-") {
		t.Error("hasPrefix deve rejeitar string menor")
	}
	if hasPrefix("topic-foo", "queue-") {
		t.Error("hasPrefix deve rejeitar prefixo diferente")
	}
}
