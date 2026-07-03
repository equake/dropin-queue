package observability

import (
	"errors"
	"testing"
	"time"
)

func TestSetupMetrics_Idempotent(t *testing.T) {
	r1 := SetupMetrics()
	r2 := SetupMetrics()
	if r1 != r2 {
		t.Fatal("SetupMetrics deve retornar o mesmo registry")
	}
}

func TestObserveHTTP_RecordMetrics(t *testing.T) {
	SetupMetrics()
	ObserveHTTP("sqs", "CreateQueue", "200", 10*time.Millisecond)
	ObserveHTTP("sqs", "CreateQueue", "200", 20*time.Millisecond)
	ObserveHTTP("sqs", "CreateQueue", "500", 5*time.Second)
	ObserveHTTP("sns", "Publish", "200", 1*time.Millisecond)

	// Coletar e verificar presença.
	mfs, err := Registry().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	found := map[string]bool{}
	for _, mf := range mfs {
		found[mf.GetName()] = true
	}

	wanted := []string{
		"shim_http_requests_total",
		"shim_http_request_duration_seconds",
		"shim_http_inflight_requests",
		"shim_storage_ops_total",
		"shim_auth_evaluations_total",
		"shim_sqs_longpoll_active_connections",
		"shim_sqs_longpoll_duration_seconds",
		"shim_sqs_queue_depth",
		"shim_sns_publish_total",
		"shim_sns_fanout_duration_seconds",
		// runtime:
		"go_goroutines",
		"process_cpu_seconds_total",
	}
	for _, name := range wanted {
		if !found[name] {
			t.Errorf("métrica %q não foi registrada", name)
		}
	}
}

func TestObserveStorage_OKAndError(t *testing.T) {
	SetupMetrics()
	ObserveStorage("publish", nil, 1*time.Millisecond)
	ObserveStorage("publish", errors.New("boom"), 1*time.Millisecond)
	ObserveStorage("fetch", nil, 2*time.Millisecond)
}

func TestSetQueueDepth(t *testing.T) {
	SetupMetrics()
	SetQueueDepth("my-queue", 42)
	SetQueueDepth("my-queue", 7)
}

func TestIncDecInflight(t *testing.T) {
	SetupMetrics()
	IncInflight("sqs")
	IncInflight("sqs")
	DecInflight("sqs")
}

func TestObserveAuth(t *testing.T) {
	SetupMetrics()
	ObserveAuth("allow")
	ObserveAuth("deny")
	ObserveAuth("error")
}

func TestIncDecLongPoll(t *testing.T) {
	SetupMetrics()
	IncLongPoll("q1")
	IncLongPoll("q1")
	DecLongPoll("q1")
	ObserveLongPollDuration("q1", 250*time.Millisecond)
}

func TestObserveSNSPublish(t *testing.T) {
	SetupMetrics()
	ObserveSNSPublish("topic-orders", "ok", 5*time.Millisecond)
	ObserveSNSPublish("topic-orders", "error", 5*time.Millisecond)
}

// TestStartObserve_OkAndError valida o helper introduzido no Commit 1
// (refactor/kiss-dry-pass-1) que substitui o pattern `defer Observe` +
// `Observe` explícito (que causava double-count).
//
// Padrão correto: StartObserve guarda start time + op name, Done(&err)
// lê o *error* no momento do defer e chama ObserveStorage 1× só.
func TestStartObserve_OkAndError(t *testing.T) {
	SetupMetrics()

	// Sucesso: err fica nil → métrica "ok".
	{
		var err error
		rec := StartObserve("test_op_ok")
		rec.Done(&err)
	}

	// Erro: err != nil → métrica "error".
	{
		err := errors.New("boom")
		rec := StartObserve("test_op_err")
		rec.Done(&err)
	}
}

// TestStartObserve_OnlyOneCall garante que Done chamado 1x resulta em
// exatamente 1 métrica registrada (o bug pré-fix era chamar Done explícito
// depois do defer, gerando 2 chamadas).
func TestStartObserve_OnlyOneCall(t *testing.T) {
	SetupMetrics()

	// Snapshot antes.
	before := countStorageOpsFor("only_one_op")

	var err error
	rec := StartObserve("only_one_op")
	rec.Done(&err)

	after := countStorageOpsFor("only_one_op")

	if (after - before) != 1 {
		t.Errorf("Done deveria incrementar contador exatamente 1x, got %d", after-before)
	}
}

// countStorageOpsFor conta o total de storage_ops_total para uma op específica.
func countStorageOpsFor(op string) int {
	mfs, err := Registry().Gather()
	if err != nil {
		return 0
	}
	for _, mf := range mfs {
		if mf.GetName() != "shim_storage_ops_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "op" && l.GetValue() == op {
					return int(m.GetCounter().GetValue())
				}
			}
		}
	}
	return 0
}
