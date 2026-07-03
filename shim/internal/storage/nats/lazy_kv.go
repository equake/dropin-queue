package nats

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

// lazyKVCache encapsula o lazy-init thread-safe de um bucket KV JetStream.
//
// 3 funções do storage usam este padrão (metadataKV, topicKV, subscriptionKV).
// Antes, cada uma tinha sua própria implementação — uma esqueceu o mutex,
// causando data race (ver Commit 1). Agora todas compartilham este helper.
type lazyKVCache struct {
	mu    sync.Mutex
	cache jetstream.KeyValue // nil = não inicializado
}

// loadKV inicializa o KV sob demanda se cache está vazio. Idempotente e
// goroutine-safe — múltiplas chamadas concorrentes em paralelo executam
// CreateOrUpdateKeyValue exatamente 1 vez (ou até 1× por erro).
func (l *lazyKVCache) loadKV(
	ctx context.Context,
	create func(context.Context) (jetstream.KeyValue, error),
) (jetstream.KeyValue, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cache != nil {
		return l.cache, nil
	}
	kv, err := create(ctx)
	if err != nil {
		return nil, err
	}
	l.cache = kv
	return kv, nil
}
