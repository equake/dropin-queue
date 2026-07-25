// Package config carrega e valida a configuração do shim a partir de
// flags CLI e variáveis de ambiente.
//
// Variáveis de ambiente sempre têm precedência sobre defaults.
// Flags CLI sempre têm precedência sobre variáveis de ambiente.
//
// Convenções:
//   - flags em kebab-case (--nats-url)
//   - env vars em UPPER_SNAKE_CASE prefixadas com GQ_ (GQ_NATS_URL)
//   - defaults seguros para dev
package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// AuthMode define o modo de autenticação do shim.
type AuthMode string

const (
	// AuthModeOff desabilita verificação de assinatura e IAM.
	// Aceita qualquer credencial. **APENAS para desenvolvimento/CI.**
	AuthModeOff AuthMode = "off"

	// AuthModeVerify verifica SigV4 das requests mas não avalia IAM policies
	// (allow-all para actions autenticadas). Útil para staging.
	AuthModeVerify AuthMode = "verify"

	// AuthModeStrict verifica SigV4 e avalia IAM policies JSON.
	// **Modo produção.**
	AuthModeStrict AuthMode = "strict"
)

// LogLevel define o nível de log.
type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

// Backend define qual broker de mensageria o shim usa. Trocável via config
// (GQ_BACKEND) — nunca simultâneo. A camada storage.Storage é a mesma
// interface para os dois; só o pacote de wiring em cmd/dropin-server muda
// qual adapter constrói.
type Backend string

const (
	// BackendNATS usa NATS JetStream (streams + KV). Default — mesmo
	// comportamento de antes deste campo existir.
	BackendNATS Backend = "nats"

	// BackendPostgres usa Postgres (tabelas + LISTEN/NOTIFY + SKIP LOCKED).
	// Opção de menor custo operacional para quem já roda Postgres; não
	// escala escrita horizontalmente como o cluster NATS (ver
	// docs/architecture.md).
	BackendPostgres Backend = "postgres"
)

// Config é a configuração completa do dropin-queue shim.
type Config struct {
	// Endereço HTTP que o shim escuta (host:porta). Default: :4566 (mesmo que LocalStack).
	Addr string

	// Backend seleciona o broker de mensageria: nats (default) ou postgres.
	// Chaveado via GQ_BACKEND — nunca os dois ao mesmo tempo. A camada
	// storage.Storage é idêntica para ambos; só cmd/dropin-server decide
	// qual adapter construir.
	Backend Backend

	// NATSURL é a URL de conexão com o broker NATS JetStream.
	// Suporta tls:// para mTLS. Usado só quando Backend == nats.
	NATSURL string

	// NATSCredentialsFile é o caminho do arquivo .creds do NATS para autenticação.
	// Se vazio, assume conexão sem autenticação (dev).
	NATSCredentialsFile string

	// NATSCACert é o CA para verificar o certificado TLS do broker.
	NATSCACert string

	// PostgresDSN é a connection string do Postgres (postgres://user:pass@host:5432/db).
	// Usado só quando Backend == postgres.
	PostgresDSN string

	// PostgresMaxConns é o tamanho máximo do pool de conexões (pgxpool),
	// sem contar a conexão dedicada de LISTEN. Se 0, assume 20.
	PostgresMaxConns int

	// PostgresPollInterval é o intervalo do poll de segurança do long-poll
	// (rede de segurança contra NOTIFY perdido — ver docs/architecture.md).
	// Se 0, assume 300ms.
	PostgresPollInterval time.Duration

	// PostgresNotifyCoalesce é o intervalo mínimo entre NOTIFYs emitidos
	// para a mesma fila. Evita 1 pg_notify() por SendMessage sob alto
	// throughput (trigger por INSERT limita a poucos milhares de writes/s).
	// Se 0, assume 20ms.
	PostgresNotifyCoalesce time.Duration

	// AccountID é o account ID default (12 dígitos) que o shim usa
	// quando não há IAM store configurado.
	AccountID string

	// Region default. AWS SDK exige; o shim não usa para roteamento mas
	// devolve nos ARNs.
	Region string

	// AuthMode define como credenciais e assinatura são tratadas.
	AuthMode AuthMode

	// LogLevel controla verbosidade dos logs.
	LogLevel LogLevel

	// MetricsAddr é o endereço separado para expor /metrics e /healthz.
	// Se vazio, usa o mesmo Addr.
	MetricsAddr string

	// ShutdownTimeout é quanto tempo esperar por shutdown gracioso.
	ShutdownTimeout time.Duration

	// MaxRequestBodyBytes limita tamanho de body aceito (defesa contra DoS).
	//
	// 5 MB default: AWS SDK inclui envelope + URL-encoded form (~5% extra)
	// sobre o tamanho do payload. SQS aceita 256 KiB por mensagem mas o
	// envelope HTTP ultrapassa isso. 5 MB cobre confortavelmente qualquer
	// workload SQS/SNS (single message, batch 10x, publish com header).
	// Ajustável via GQ_MAX_BODY_BYTES.
	MaxRequestBodyBytes int64

	// StreamReplicas é o número de réplicas Raft de cada stream JetStream
	// (filas e tópicos). 1 para dev/single-node; 3 para produção HA.
	// **Nunca inferido** — deploy de produção DEVE setar GQ_STREAM_REPLICAS=3
	// explicitamente, senão roda sem HA.
	StreamReplicas int

	// MaxAckPending limita quantas mensagens podem estar in-flight
	// (recebidas e não-acked) por fila. Equivale ao limite de mensagens
	// invisíveis do SQS (120k por fila). Valores maiores aumentam
	// throughput de consumo paralelo às custas de memória no broker.
	MaxAckPending int

	// TopicMaxAge é a retenção do stream de arquivo de cada tópico SNS.
	// O fan-out para subscribers é síncrono no Publish; o stream do tópico
	// é apenas um registro histórico. Retenção curta = custo de disco baixo.
	TopicMaxAge time.Duration
}

// Default retorna configuração default segura para dev.
func Default() Config {
	return Config{
		Addr:                   ":4566",
		Backend:                BackendNATS,
		NATSURL:                "nats://localhost:4222",
		PostgresMaxConns:       20,
		PostgresPollInterval:   300 * time.Millisecond,
		PostgresNotifyCoalesce: 20 * time.Millisecond,
		AccountID:              "000000000000",
		Region:                 "us-east-1",
		AuthMode:               AuthModeOff,
		LogLevel:               LogLevelInfo,
		MetricsAddr:            "",
		ShutdownTimeout:        30 * time.Second,
		MaxRequestBodyBytes:    5 << 20, // 5 MB; cobre SQS+SNS no pior caso
		StreamReplicas:         1,       // dev; produção seta 3 explicitamente
		MaxAckPending:          1000,
		TopicMaxAge:            time.Hour,
	}
}

// Load carrega a configuração a partir de args (CLI flags + env).
//
// args[0] é o nome do programa (descartado).
func Load(args []string) (Config, error) {
	cfg := Default()

	fs := flag.NewFlagSet("dropin-queue", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.StringVar(&cfg.Addr, "addr", envOr("GQ_ADDR", cfg.Addr), "Endereço HTTP (host:porta)")
	fs.StringVar((*string)(&cfg.Backend), "backend", envOr("GQ_BACKEND", string(cfg.Backend)), "Backend de mensageria: nats|postgres")
	fs.StringVar(&cfg.NATSURL, "nats-url", envOr("GQ_NATS_URL", cfg.NATSURL), "URL do broker NATS JetStream (nats:// ou tls://)")
	fs.StringVar(&cfg.NATSCredentialsFile, "nats-creds", envOr("GQ_NATS_CREDS", ""), "Arquivo .creds NATS para autenticação")
	fs.StringVar(&cfg.NATSCACert, "nats-ca-cert", envOr("GQ_NATS_CA_CERT", ""), "Certificado CA para validar TLS do broker")
	fs.StringVar(&cfg.PostgresDSN, "postgres-dsn", envOr("GQ_POSTGRES_DSN", ""), "Connection string Postgres (postgres://user:pass@host:5432/db)")
	fs.IntVar(&cfg.PostgresMaxConns, "postgres-max-conns", envIntOr("GQ_POSTGRES_MAX_CONNS", cfg.PostgresMaxConns), "Máximo de conexões no pool Postgres")
	fs.DurationVar(&cfg.PostgresPollInterval, "postgres-poll-interval", envDurationOr("GQ_POSTGRES_POLL_INTERVAL", cfg.PostgresPollInterval), "Poll de segurança do long-poll (fallback do NOTIFY)")
	fs.DurationVar(&cfg.PostgresNotifyCoalesce, "postgres-notify-coalesce", envDurationOr("GQ_POSTGRES_NOTIFY_COALESCE", cfg.PostgresNotifyCoalesce), "Intervalo mínimo entre NOTIFYs da mesma fila")
	fs.StringVar(&cfg.AccountID, "account-id", envOr("GQ_ACCOUNT_ID", cfg.AccountID), "Account ID default (12 dígitos)")
	fs.StringVar(&cfg.Region, "region", envOr("GQ_REGION", cfg.Region), "Região AWS default")
	fs.StringVar((*string)(&cfg.AuthMode), "auth-mode", envOr("GQ_AUTH_MODE", string(cfg.AuthMode)), "Modo de auth: off|verify|strict")
	fs.StringVar((*string)(&cfg.LogLevel), "log-level", envOr("GQ_LOG_LEVEL", string(cfg.LogLevel)), "Nível de log: debug|info|warn|error")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", envOr("GQ_METRICS_ADDR", ""), "Endereço separado para /metrics (vazio = mesmo que --addr)")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", envDurationOr("GQ_SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout), "Timeout para shutdown gracioso")
	fs.Int64Var(&cfg.MaxRequestBodyBytes, "max-body-bytes", envInt64Or("GQ_MAX_BODY_BYTES", cfg.MaxRequestBodyBytes), "Tamanho máximo do body (bytes)")
	fs.IntVar(&cfg.StreamReplicas, "stream-replicas", envIntOr("GQ_STREAM_REPLICAS", cfg.StreamReplicas), "Réplicas Raft por stream JetStream (1, 3 ou 5; produção = 3)")
	fs.IntVar(&cfg.MaxAckPending, "max-ack-pending", envIntOr("GQ_MAX_ACK_PENDING", cfg.MaxAckPending), "Máximo de mensagens in-flight por fila")
	fs.DurationVar(&cfg.TopicMaxAge, "topic-max-age", envDurationOr("GQ_TOPIC_MAX_AGE", cfg.TopicMaxAge), "Retenção do stream de arquivo dos tópicos SNS")

	if err := fs.Parse(args[1:]); err != nil {
		return Config{}, fmt.Errorf("parse flags: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate verifica invariantes da configuração.
func (c Config) Validate() error {
	var errs []string

	if c.Addr == "" {
		errs = append(errs, "addr não pode ser vazio")
	}
	switch c.Backend {
	case BackendNATS:
		if c.NATSURL == "" {
			errs = append(errs, "nats-url não pode ser vazio")
		}
		switch c.StreamReplicas {
		case 1, 3, 5:
			// ok — Raft exige quórum ímpar
		default:
			errs = append(errs, fmt.Sprintf("stream-replicas deve ser 1, 3 ou 5, recebido %d", c.StreamReplicas))
		}
		if c.IsProduction() && c.StreamReplicas == 1 {
			// Fail-loud: produção com 1 réplica = perda de dados na queda de 1 nó.
			errs = append(errs, "auth-mode verify/strict exige stream-replicas >= 3 (HA); use GQ_STREAM_REPLICAS=3")
		}
	case BackendPostgres:
		if c.PostgresDSN == "" {
			errs = append(errs, "postgres-dsn não pode ser vazio quando backend=postgres")
		}
		if c.PostgresMaxConns < 1 {
			errs = append(errs, "postgres-max-conns deve ser >= 1")
		}
		if c.PostgresPollInterval <= 0 {
			errs = append(errs, "postgres-poll-interval deve ser positivo")
		}
		if c.PostgresNotifyCoalesce < 0 {
			errs = append(errs, "postgres-notify-coalesce não pode ser negativo")
		}
	default:
		errs = append(errs, fmt.Sprintf("backend inválido: %q (esperado nats|postgres)", c.Backend))
	}
	if c.AccountID != "" && !isValidAccountID(c.AccountID) {
		errs = append(errs, fmt.Sprintf("account-id deve ter 12 dígitos, recebido %q", c.AccountID))
	}
	if c.Region == "" {
		errs = append(errs, "region não pode ser vazio")
	}
	switch c.AuthMode {
	case AuthModeOff, AuthModeVerify, AuthModeStrict:
		// ok
	default:
		errs = append(errs, fmt.Sprintf("auth-mode inválido: %q (esperado off|verify|strict)", c.AuthMode))
	}
	switch c.LogLevel {
	case LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError:
		// ok
	default:
		errs = append(errs, fmt.Sprintf("log-level inválido: %q", c.LogLevel))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, "shutdown-timeout deve ser positivo")
	}
	if c.MaxRequestBodyBytes < 1024 {
		errs = append(errs, "max-body-bytes deve ser >= 1024")
	}
	if c.MaxAckPending < 1 {
		errs = append(errs, "max-ack-pending deve ser >= 1")
	}
	if c.TopicMaxAge <= 0 {
		errs = append(errs, "topic-max-age deve ser positivo")
	}

	if len(errs) > 0 {
		return errors.New("configuração inválida: " + strings.Join(errs, "; "))
	}
	return nil
}

// IsProduction retorna true se a configuração atual exige verificação
// de assinatura (verify ou strict). Útil para fail-closed em produção.
func (c Config) IsProduction() bool {
	return c.AuthMode == AuthModeVerify || c.AuthMode == AuthModeStrict
}

// envOr retorna o valor da env var ou o fallback.
func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envDurationOr(key string, fallback time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return fallback
}

func envInt64Or(key string, fallback int64) int64 {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		var n int64
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return fallback
}

func isValidAccountID(s string) bool {
	if len(s) != 12 {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
