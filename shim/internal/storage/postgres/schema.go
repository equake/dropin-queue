package postgres

// schemaDDL cria o schema completo do backend Postgres — idempotente
// (IF NOT EXISTS em tudo), aplicado no Connect() a cada boot, mesmo
// espírito do CreateOrUpdateKeyValue lazy do adapter NATS: sem ferramenta
// de migration externa para o MVP.
//
// gen_random_uuid() é nativo do Postgres desde a versão 13 (não precisa da
// extensão pgcrypto) — usado para claim_token, o token stateless que torna
// ack/nak possíveis de qualquer réplica do shim, mesma garantia que o reply
// subject $JS.ACK dá no adapter NATS.
const schemaDDL = `
CREATE TABLE IF NOT EXISTS queues (
    id                                 BIGSERIAL PRIMARY KEY,
    name                                TEXT NOT NULL UNIQUE,
    fifo                                BOOLEAN NOT NULL DEFAULT false,
    visibility_timeout                  INT NOT NULL DEFAULT 30,
    message_retention_period            INT NOT NULL DEFAULT 345600,
    maximum_message_size                INT NOT NULL DEFAULT 262144,
    delay_seconds                       INT NOT NULL DEFAULT 0,
    receive_message_wait_time_seconds   INT NOT NULL DEFAULT 0,
    content_based_dedup                 BOOLEAN NOT NULL DEFAULT false,
    tags                                JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- messages: retention = delete-on-ack (mesma escolha do WorkQueuePolicy no
-- adapter NATS) — DeleteMessage faz DELETE, não soft-delete. Reter
-- mensagens já consumidas em disco até MaxAge multiplicaria custo de
-- armazenamento sem benefício a milhões de msgs/dia.
CREATE TABLE IF NOT EXISTS messages (
    id                   BIGSERIAL PRIMARY KEY,
    queue_id             BIGINT NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    group_key            TEXT NOT NULL DEFAULT 'default',
    body                 TEXT NOT NULL,
    message_attributes   JSONB NOT NULL DEFAULT '{}'::jsonb,
    enqueued_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    visible_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivery_count       INT NOT NULL DEFAULT 0,
    claim_token          UUID
);

-- Índice principal do claim (ReceiveMessage): mensagens visíveis, em ordem
-- de chegada, filtradas por fila. SKIP LOCKED faz o resto.
CREATE INDEX IF NOT EXISTS idx_messages_claim
    ON messages (queue_id, visible_at, id);

-- Índice do reaper de retenção (MessageRetentionPeriod) — ver
-- Client.reapOnce em client.go. Sem ele, a limpeza periódica faria
-- sequential scan da tabela inteira a cada tick.
CREATE INDEX IF NOT EXISTS idx_messages_retention
    ON messages (queue_id, enqueued_at);

-- message_dedup: dedup FIFO (MessageDeduplicationId explícito ou
-- ContentBasedDeduplication via SHA-256 do body), janela de 5min — mesmo
-- comportamento observável do header Nats-Msg-Id no adapter NATS.
-- INSERT ... ON CONFLICT DO NOTHING detecta duplicata sem round-trip extra.
--
-- message_id guarda o id da mensagem "vencedora" (a primeira a ser
-- inserida com esse dedup_id). Em duplicatas subsequentes dentro da
-- janela, devolvemos esse mesmo id como MessageId — mesmo comportamento
-- do PubAck.Duplicate do JetStream, que reaproveita o Sequence original
-- em vez de gerar um novo.
CREATE TABLE IF NOT EXISTS message_dedup (
    queue_id     BIGINT NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    dedup_id     TEXT NOT NULL,
    message_id   BIGINT NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue_id, dedup_id)
);
CREATE INDEX IF NOT EXISTS idx_message_dedup_expires ON message_dedup (expires_at);

CREATE TABLE IF NOT EXISTS topics (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- topic_arn é persistido verbatim (não reconstruído) porque o storage não
-- conhece account/region — quem monta o ARN é a camada sns/, que já passa
-- o ARN completo em Subscription.TopicARN. Mesmo padrão do
-- subscriptionMetadataV1 no adapter NATS.
CREATE TABLE IF NOT EXISTS subscriptions (
    arn             TEXT PRIMARY KEY,
    topic_id        BIGINT NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    topic_arn       TEXT NOT NULL,
    protocol        TEXT NOT NULL,
    endpoint        TEXT NOT NULL,
    filter_policy   TEXT NOT NULL DEFAULT '',
    raw_delivery    BOOLEAN NOT NULL DEFAULT false,
    pending         BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_subscriptions_topic ON subscriptions (topic_id);
`
