package nats

import (
	"sync"
	"sync/atomic"
	"testing"
)

// lazyKVMini é uma versão mini do lazyKVCache usada em testes. Usa *T
// interno (em vez de T direto) porque o zero-value check de T genérico
// é proibido pelo compilador Go ("incomparable types in type set"),
// mas *T permite nil-check trivial.
//
// O padrão em produção (lazyKVCache com jetstream.KeyValue não-pointer)
// é equivalente: cache == nil é o mesmo que "T not initialized" para
// jetstream.KeyValue (interface nil-check funciona da mesma forma).
type lazyKVMini[T any] struct {
	mu    sync.Mutex
	cache *T
}

func (l *lazyKVMini[T]) load(create func() (T, error)) (T, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cache != nil {
		return *l.cache, nil
	}
	v, err := create()
	if err != nil {
		var zero T
		return zero, err
	}
	l.cache = &v
	return v, nil
}

// fakeKVMarker é o tipo usado nos testes (em vez de jetstream.KeyValue).
type fakeKVMarker struct{ id int }

// errFakeLazyKV é sentinela para simular erro de create().
type lazyKVErr string

func (e lazyKVErr) Error() string { return string(e) }

var errFakeLazyKV = lazyKVErr("fake error for lazy_kv test")

// TestLazyKV_ConcurrentLoadOnlyOnce verifica que load chama create() exatamente
// 1 vez sob alta concorrência.
//
// Caso real (com jetstream.KeyValue): o mesmo padrão está em metadataKV,
// topicKV, subscriptionKV dentro do storage/nats/. Concorrência alta é
// o caminho normal em produção (múltiplas réplicas do shim fazendo
// chamadas concorrentes). Sem o Lock/Unlock, 2 goroutines em paralelo
// podem disparar CreateOrUpdateKeyValue simultaneamente — uma escreve
// o ponteiro `c.kvCache = kv` enquanto a outra lê o ponteiro antigo;
// o data race seria pego pelo `go test -race`.
func TestLazyKV_ConcurrentLoadOnlyOnce(t *testing.T) {
	var calls int32
	create := func() (fakeKVMarker, error) {
		atomic.AddInt32(&calls, 1)
		return fakeKVMarker{id: 1}, nil
	}

	var lc lazyKVMini[fakeKVMarker]
	const goroutines = 100

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = lc.load(create)
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("create chamado %d vezes, esperado 1", got)
	}
}

// TestLazyKV_CachesAfterFirstLoad garante que load devolve o MESMO
// objeto em chamadas subsequentes (sem recriar).
func TestLazyKV_CachesAfterFirstLoad(t *testing.T) {
	var lc lazyKVMini[fakeKVMarker]
	var first fakeKVMarker

	for i := 0; i < 3; i++ {
		kv, err := lc.load(func() (fakeKVMarker, error) { return fakeKVMarker{id: i}, nil })
		if err != nil {
			t.Fatalf("load err: %v", err)
		}
		if first.id == 0 {
			first = kv
		} else if kv != first {
			t.Errorf("load() %d retornou instância diferente da primeira", i)
		}
	}
}

// TestLazyKV_CreateErrorNaoPreencheCache garante que se create falha,
// o cache continua vazio (próxima load tenta de novo).
func TestLazyKV_CreateErrorNaoPreencheCache(t *testing.T) {
	var lc lazyKVMini[fakeKVMarker]
	var attempts int32
	failingCreate := func() (fakeKVMarker, error) {
		atomic.AddInt32(&attempts, 1)
		var zero fakeKVMarker
		return zero, errFakeLazyKV
	}

	if _, err := lc.load(failingCreate); err == nil {
		t.Fatal("esperava erro, got nil")
	}
	if _, err := lc.load(failingCreate); err == nil {
		t.Fatal("esperava erro, got nil")
	}

	if got := atomic.LoadInt32(&attempts); got != 2 {
		t.Errorf("create deveria ter sido chamado 2 vezes (erro não preenche cache), got %d", got)
	}
}
