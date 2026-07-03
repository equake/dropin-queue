package nats

import (
	"github.com/equake/dropin-queue/shim/internal/storage"
)

// Queues devolve o adapter de filas (próprio Client satisfaz QueueStorage).
func (c *Client) Queues() storage.QueueStorage { return c }

// Messages devolve o adapter de mensagens (próprio Client satisfaz MessageStorage).
func (c *Client) Messages() storage.MessageStorage { return c }

// Topics devolve o adapter de tópicos.
func (c *Client) Topics() storage.TopicStorage { return c }

// Garantia em tempo de compilação que Client satisfaz todas as interfaces.
//
// Se alguma assinatura mudar, isso falha em build — exatamente o que queremos.
var (
	_ storage.QueueStorage   = (*Client)(nil)
	_ storage.MessageStorage = (*Client)(nil)
	_ storage.TopicStorage   = (*Client)(nil)
	_ storage.Storage        = (*Client)(nil)
)
