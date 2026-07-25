package postgres

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/equake/dropin-queue/shim/pkg/types"
)

// Testes de integração reais contra um Postgres — mesmo princípio do
// projeto para o adapter NATS ("nunca mockar o broker", ver AGENTS.md).
//
// Requer GQ_TEST_POSTGRES_DSN apontando para um Postgres descartável
// (ex: docker run --rm -e POSTGRES_PASSWORD=x -p 55432:5432 postgres:16-alpine).
// Sem a env var, os testes são pulados (não falham o `make test` normal).
func testClient(t *testing.T) *Client {
	t.Helper()
	dsn := os.Getenv("GQ_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GQ_TEST_POSTGRES_DSN não setado — pulando testes de integração Postgres")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Connect(ctx, Options{DSN: dsn, PollInterval: 50 * time.Millisecond, NotifyCoalesce: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	})
	return c
}

func uniqueName(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func TestCreateGetListDeleteQueue(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	name := uniqueName(t, "q")

	created, err := c.CreateQueue(ctx, types.Queue{Name: name, Attributes: types.DefaultQueueAttributes()})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if created.Attributes.VisibilityTimeout != 30 {
		t.Errorf("VisibilityTimeout default: got %d", created.Attributes.VisibilityTimeout)
	}

	// Idempotente: criar de novo devolve a mesma fila, sem erro.
	created2, err := c.CreateQueue(ctx, types.Queue{Name: name, Attributes: types.QueueAttributes{VisibilityTimeout: 999}})
	if err != nil {
		t.Fatalf("CreateQueue idempotente: %v", err)
	}
	if created2.Attributes.VisibilityTimeout != 30 {
		t.Errorf("CreateQueue idempotente não deveria sobrescrever atributos: got %d", created2.Attributes.VisibilityTimeout)
	}

	got, err := c.GetQueue(ctx, name)
	if err != nil {
		t.Fatalf("GetQueue: %v", err)
	}
	if got.Name != name {
		t.Errorf("GetQueue name: got %s", got.Name)
	}

	list, err := c.ListQueues(ctx, "")
	if err != nil {
		t.Fatalf("ListQueues: %v", err)
	}
	found := false
	for _, q := range list {
		if q.Name == name {
			found = true
		}
	}
	if !found {
		t.Errorf("ListQueues não encontrou %s", name)
	}

	if err := c.DeleteQueue(ctx, name); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
	if _, err := c.GetQueue(ctx, name); err == nil {
		t.Error("GetQueue após delete deveria falhar")
	}
	// Idempotente.
	if err := c.DeleteQueue(ctx, name); err != nil {
		t.Errorf("DeleteQueue idempotente: %v", err)
	}
}

func TestSetQueueAttributes(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	name := uniqueName(t, "q")

	if _, err := c.CreateQueue(ctx, types.Queue{Name: name, Attributes: types.DefaultQueueAttributes()}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if err := c.SetQueueAttributes(ctx, name, types.QueueAttributes{VisibilityTimeout: 120}); err != nil {
		t.Fatalf("SetQueueAttributes: %v", err)
	}
	got, err := c.GetQueue(ctx, name)
	if err != nil {
		t.Fatalf("GetQueue: %v", err)
	}
	if got.Attributes.VisibilityTimeout != 120 {
		t.Errorf("VisibilityTimeout: got %d, want 120", got.Attributes.VisibilityTimeout)
	}
	// MaximumMessageSize não foi fornecido (0) — deve manter o default.
	if got.Attributes.MaximumMessageSize != 262144 {
		t.Errorf("MaximumMessageSize deveria ser preservado: got %d", got.Attributes.MaximumMessageSize)
	}
}

func TestSendReceiveDeleteMessage(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	name := uniqueName(t, "q")

	if _, err := c.CreateQueue(ctx, types.Queue{Name: name, Attributes: types.DefaultQueueAttributes()}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	sent, err := c.SendMessage(ctx, name, &types.Message{Body: "hello"})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if sent.MD5OfBody == "" {
		t.Error("MD5OfBody vazio")
	}

	msgs, err := c.ReceiveMessage(ctx, name, 1, 0, 30)
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("esperava 1 mensagem, got %d", len(msgs))
	}
	if msgs[0].Body != "hello" {
		t.Errorf("Body: got %q", msgs[0].Body)
	}
	if msgs[0].Attributes["ApproximateReceiveCount"] != "1" {
		t.Errorf("ApproximateReceiveCount: got %s", msgs[0].Attributes["ApproximateReceiveCount"])
	}

	// Mensagem invisível — outro receive não deve trazer nada.
	msgs2, err := c.ReceiveMessage(ctx, name, 1, 0, 30)
	if err != nil {
		t.Fatalf("ReceiveMessage 2: %v", err)
	}
	if len(msgs2) != 0 {
		t.Fatalf("mensagem deveria estar invisível, got %d", len(msgs2))
	}

	if err := c.DeleteMessage(ctx, name, msgs[0].ReceiptHandle); err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}
	// Idempotente.
	if err := c.DeleteMessage(ctx, name, msgs[0].ReceiptHandle); err != nil {
		t.Errorf("DeleteMessage idempotente: %v", err)
	}

	depth, err := c.QueueDepth(ctx, name)
	if err != nil {
		t.Fatalf("QueueDepth: %v", err)
	}
	if depth != 0 {
		t.Errorf("QueueDepth após delete: got %d", depth)
	}
}

func TestDecodeReceiptHandleInvalid(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	name := uniqueName(t, "q")
	if _, err := c.CreateQueue(ctx, types.Queue{Name: name, Attributes: types.DefaultQueueAttributes()}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if err := c.DeleteMessage(ctx, name, "rh1:notreal:9999"); err == nil {
		t.Error("esperava erro para receipt handle malformado")
	}
}

func TestChangeMessageVisibilityZeroMakesImmediatelyVisible(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	name := uniqueName(t, "q")
	if _, err := c.CreateQueue(ctx, types.Queue{Name: name, Attributes: types.DefaultQueueAttributes()}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := c.SendMessage(ctx, name, &types.Message{Body: "vis-test"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msgs, err := c.ReceiveMessage(ctx, name, 1, 0, 60)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("ReceiveMessage: %v (%d msgs)", err, len(msgs))
	}
	if err := c.ChangeMessageVisibility(ctx, name, msgs[0].ReceiptHandle, 0); err != nil {
		t.Fatalf("ChangeMessageVisibility: %v", err)
	}
	msgs2, err := c.ReceiveMessage(ctx, name, 1, 0, 30)
	if err != nil {
		t.Fatalf("ReceiveMessage 2: %v", err)
	}
	if len(msgs2) != 1 {
		t.Fatalf("esperava mensagem redelivered imediatamente, got %d", len(msgs2))
	}
}

func TestFIFOOrderingWithinGroup(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	name := uniqueName(t, "q") + ".fifo"
	if _, err := c.CreateQueue(ctx, types.Queue{Name: name, FIFO: true, Attributes: types.DefaultQueueAttributes()}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := c.SendMessage(ctx, name, &types.Message{
			Body:           fmt.Sprintf("msg-%d", i),
			MessageGroupId: "group-a",
		}); err != nil {
			t.Fatalf("SendMessage %d: %v", i, err)
		}
	}

	// Mesma semântica do adapter NATS: um ReceiveMessage entrega TODAS as
	// mensagens visíveis do grupo de uma vez (até o batch pedido), na
	// ordem em que foram enviadas — não há restrição de "1 in-flight por
	// grupo" em nenhum dos dois backends hoje.
	msgs, err := c.ReceiveMessage(ctx, name, 10, 0, 60)
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("esperava as 3 mensagens do grupo em uma chamada, got %d", len(msgs))
	}
	for i, m := range msgs {
		want := fmt.Sprintf("msg-%d", i)
		if m.Body != want {
			t.Errorf("ordem FIFO no índice %d: esperava %s, got %s", i, want, m.Body)
		}
	}
}

func TestFIFOContentBasedDeduplication(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	name := uniqueName(t, "q") + ".fifo"
	attrs := types.DefaultQueueAttributes()
	attrs.ContentBasedDeduplication = true
	if _, err := c.CreateQueue(ctx, types.Queue{Name: name, FIFO: true, Attributes: attrs}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	msg := &types.Message{Body: "same-body", MessageGroupId: "g1"}
	first, err := c.SendMessage(ctx, name, msg)
	if err != nil {
		t.Fatalf("SendMessage 1: %v", err)
	}
	second, err := c.SendMessage(ctx, name, &types.Message{Body: "same-body", MessageGroupId: "g1"})
	if err != nil {
		t.Fatalf("SendMessage 2: %v", err)
	}
	if second.Attributes["Duplicate"] != "true" {
		t.Errorf("esperava Duplicate=true na segunda mensagem idêntica")
	}
	if first.ID == second.ID {
		t.Log("IDs diferentes esperado (segunda é sintética) — ok")
	}

	depth, err := c.QueueDepth(ctx, name)
	if err != nil {
		t.Fatalf("QueueDepth: %v", err)
	}
	if depth != 1 {
		t.Errorf("dedup deveria resultar em 1 mensagem só na fila, got %d", depth)
	}
}

func TestTopicSubscribePublishFanoutAndFilterPolicy(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	topicName := uniqueName(t, "t")
	queueName := uniqueName(t, "q")

	if _, err := c.CreateTopic(ctx, types.Topic{Name: topicName}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := c.CreateQueue(ctx, types.Queue{Name: queueName, Attributes: types.DefaultQueueAttributes()}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	topicARN := "arn:aws:sns:us-east-1:000000000000:" + topicName
	queueURL := "http://localhost:4566/000000000000/" + queueName

	sub, err := c.Subscribe(ctx, types.Subscription{
		TopicARN:     topicARN,
		Protocol:     "sqs",
		Endpoint:     queueURL,
		FilterPolicy: `{"event":["order_placed"]}`,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if sub.Pending {
		t.Error("subscription SQS não deveria ficar pending")
	}

	subs, err := c.ListSubscriptions(ctx, topicName)
	if err != nil {
		t.Fatalf("ListSubscriptions: %v", err)
	}
	if len(subs) != 1 {
		t.Fatalf("esperava 1 subscription, got %d", len(subs))
	}

	// Mensagem que NÃO casa o filter policy — não deve chegar na fila.
	if _, err := c.Publish(ctx, topicName, &types.Message{
		Body: "no-match",
		MessageAttributes: map[string]types.MessageAttribute{
			"event": {DataType: "String", StringValue: "order_cancelled"},
		},
	}); err != nil {
		t.Fatalf("Publish (no match): %v", err)
	}

	// Mensagem que casa — deve chegar na fila via fan-out síncrono.
	if _, err := c.Publish(ctx, topicName, &types.Message{
		Body: "matched",
		MessageAttributes: map[string]types.MessageAttribute{
			"event": {DataType: "String", StringValue: "order_placed"},
		},
	}); err != nil {
		t.Fatalf("Publish (match): %v", err)
	}

	msgs, err := c.ReceiveMessage(ctx, queueName, 10, 0, 30)
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("esperava 1 mensagem entregue via fan-out (filter policy deveria bloquear a outra), got %d", len(msgs))
	}
	if msgs[0].Body != "matched" {
		t.Errorf("Body: got %q", msgs[0].Body)
	}

	if err := c.Unsubscribe(ctx, sub.ARN); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	// Idempotente — igual DELETE de SQL (e igual kv.Delete() do NATS, que
	// não retorna ErrKeyNotFound para chave já removida).
	if err := c.Unsubscribe(ctx, sub.ARN); err != nil {
		t.Errorf("Unsubscribe de ARN já removido deveria ser idempotente: %v", err)
	}
	if err := c.Unsubscribe(ctx, "arn:aws:sns:us-east-1:000000000000:never-existed:0"); err != nil {
		t.Errorf("Unsubscribe de ARN que nunca existiu deveria ser idempotente: %v", err)
	}

	if err := c.DeleteTopic(ctx, topicName); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
}

// TestConcurrentReceiveNeverDoubleDelivers é a verificação do risco real do
// design: SKIP LOCKED precisa garantir que ReceiveMessage concorrentes
// nunca reivindiquem a MESMA mensagem duas vezes, mesmo disputando o
// mesmo pool de linhas visíveis ao mesmo tempo.
func TestConcurrentReceiveNeverDoubleDelivers(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	name := uniqueName(t, "q")
	if _, err := c.CreateQueue(ctx, types.Queue{Name: name, Attributes: types.DefaultQueueAttributes()}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	const nMsgs = 50
	for i := 0; i < nMsgs; i++ {
		if _, err := c.SendMessage(ctx, name, &types.Message{Body: fmt.Sprintf("m-%d", i)}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
	}

	const nWorkers = 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[string]int) // body -> quantas vezes foi entregue
	for w := 0; w < nWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msgs, err := c.ReceiveMessage(ctx, name, 10, 0, 60)
			if err != nil {
				t.Errorf("ReceiveMessage: %v", err)
				return
			}
			mu.Lock()
			for _, m := range msgs {
				seen[m.Body]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	total := 0
	for body, count := range seen {
		total += count
		if count > 1 {
			t.Errorf("mensagem %q entregue %d vezes — SKIP LOCKED falhou em evitar double-delivery", body, count)
		}
	}
	if total != nMsgs {
		t.Fatalf("esperava %d mensagens únicas entregues no total, got %d", nMsgs, total)
	}
}

func TestLongPollWakesUpOnNotify(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	name := uniqueName(t, "q")
	if _, err := c.CreateQueue(ctx, types.Queue{Name: name, Attributes: types.DefaultQueueAttributes()}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	resultCh := make(chan []types.Message, 1)
	errCh := make(chan error, 1)
	go func() {
		msgs, err := c.ReceiveMessage(ctx, name, 1, 5, 30)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- msgs
	}()

	// Dá tempo do long-poll assinar o hub antes de publicar.
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	if _, err := c.SendMessage(ctx, name, &types.Message{Body: "wake-up"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("ReceiveMessage: %v", err)
	case msgs := <-resultCh:
		elapsed := time.Since(start)
		if len(msgs) != 1 {
			t.Fatalf("esperava 1 mensagem, got %d", len(msgs))
		}
		// Deve acordar via NOTIFY bem antes do prazo de 5s do long-poll —
		// tolerância generosa (poll de segurança é 50ms neste teste).
		if elapsed > 2*time.Second {
			t.Errorf("long-poll demorou %v para acordar — NOTIFY não está funcionando?", elapsed)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("timeout esperando ReceiveMessage retornar")
	}
}
