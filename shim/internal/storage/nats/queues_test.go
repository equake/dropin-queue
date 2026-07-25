package nats

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/equake/dropin-queue/shim/pkg/types"
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
	c := &Client{replicas: 1}
	q := types.Queue{
		Name:       "test-queue",
		Attributes: types.DefaultQueueAttributes(),
	}
	cfg := c.streamCfg(q)

	if cfg.Name != "queue-test-queue" {
		t.Errorf("Name: got %s", cfg.Name)
	}
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != "q.test-queue.>" {
		t.Errorf("Subjects: got %v", cfg.Subjects)
	}
	if cfg.Replicas != 1 {
		t.Errorf("replicas deve vir da config, got %d", cfg.Replicas)
	}
	if cfg.Storage != jetstream.FileStorage {
		t.Errorf("Storage deve ser File, got %v", cfg.Storage)
	}
	// WorkQueuePolicy: mensagem consumida (acked) é apagada na hora —
	// sem isso, todo o tráfego já processado fica em disco até MaxAge.
	if cfg.Retention != jetstream.WorkQueuePolicy {
		t.Errorf("Retention deve ser WorkQueue, got %v", cfg.Retention)
	}
	// DiscardNew: fila cheia rejeita publish com erro; DiscardOld
	// perderia mensagens antigas não-consumidas silenciosamente.
	if cfg.Discard != jetstream.DiscardNew {
		t.Errorf("Discard deve ser New, got %v", cfg.Discard)
	}
	if cfg.MaxMsgs != defaultMaxMsgsPerQueue {
		t.Errorf("MaxMsgs default: got %d", cfg.MaxMsgs)
	}
	// Duplicates precisa ser setado explicitamente — sem isso o
	// nats-server usa seu próprio default de 2min, divergindo da spec
	// SQS FIFO (5min) e do adapter Postgres (dedupWindow = 5min).
	if cfg.Duplicates != dedupWindow {
		t.Errorf("Duplicates: got %v, want %v (dedupWindow)", cfg.Duplicates, dedupWindow)
	}
}

// TestStreamCfg_DuplicatesCappedByShortRetention prova o cuidado extra do
// fix: o nats-server rejeita CreateStream se Duplicates > MaxAge. Uma fila
// com MessageRetentionPeriod curto (SQS aceita a partir de 60s, bem abaixo
// dos 5min de dedupWindow) precisa ter Duplicates capado ao MaxAge, não
// travado num valor fixo que excede a retenção da fila.
func TestStreamCfg_DuplicatesCappedByShortRetention(t *testing.T) {
	c := &Client{replicas: 1}
	q := types.Queue{
		Name: "short-retention",
		Attributes: types.QueueAttributes{
			MessageRetentionPeriod: 60, // mínimo aceito pela spec SQS
		},
	}
	cfg := c.streamCfg(q)
	want := 60 * time.Second
	if cfg.Duplicates != want {
		t.Errorf("Duplicates deveria ser capado por MaxAge: got %v, want %v", cfg.Duplicates, want)
	}
	if cfg.Duplicates > cfg.MaxAge {
		t.Errorf("Duplicates (%v) nunca pode exceder MaxAge (%v) — nats-server rejeita CreateStream", cfg.Duplicates, cfg.MaxAge)
	}
}

func TestStreamCfg_CustomRetention(t *testing.T) {
	c := &Client{replicas: 1}
	q := types.Queue{
		Name: "x",
		Attributes: types.QueueAttributes{
			MessageRetentionPeriod: 86400, // 1 dia
		},
	}
	cfg := c.streamCfg(q)
	want := 86400 * time.Second
	if cfg.MaxAge != want {
		t.Errorf("MaxAge: got %v, want %v", cfg.MaxAge, want)
	}
}

func TestStreamCfg_ReplicasFromConfig(t *testing.T) {
	// Réplicas vêm SEMPRE da config explícita — nunca inferidas de
	// prefix/ambiente. Um deploy de produção sem GQ_STREAM_REPLICAS=3
	// falha na validação de config, não roda silenciosamente sem HA.
	c := &Client{replicas: 3, prefix: "tenant1"}
	q := types.Queue{Name: "x"}
	cfg := c.streamCfg(q)
	if cfg.Replicas != 3 {
		t.Errorf("replicas deve vir da config, got %d", cfg.Replicas)
	}
}

func TestStreamCfg_FIFO(t *testing.T) {
	c := &Client{replicas: 1}
	q := types.Queue{Name: "my-queue.fifo", FIFO: true}
	cfg := c.streamCfg(q)
	if !q.FIFO {
		t.Error("FIFO flag deve estar setada")
	}
	// Sanity: subjects seguem mesmo padrão (FIFO é tratado por camada superior)
	if cfg.Subjects[0] != "q.my-queue.fifo.>" {
		t.Errorf("Subjects: got %v", cfg.Subjects)
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
