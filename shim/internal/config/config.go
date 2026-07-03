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

// Config é a configuração completa do dropin-queue shim.
type Config struct {
	// Endereço HTTP que o shim escuta (host:porta). Default: :4566 (mesmo que LocalStack).
	Addr string

	// NATSURL é a URL de conexão com o broker NATS JetStream.
	// Suporta tls:// para mTLS.
	NATSURL string

	// NATSCredentialsFile é o caminho do arquivo .creds do NATS para autenticação.
	// Se vazio, assume conexão sem autenticação (dev).
	NATSCredentialsFile string

	// NATSCACert é o CA para verificar o certificado TLS do broker.
	NATSCACert string

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
	MaxRequestBodyBytes int64
}

// Default retorna configuração default segura para dev.
func Default() Config {
	return Config{
		Addr:                ":4566",
		NATSURL:             "nats://localhost:4222",
		AccountID:           "000000000000",
		Region:              "us-east-1",
		AuthMode:            AuthModeOff,
		LogLevel:            LogLevelInfo,
		MetricsAddr:         "",
		ShutdownTimeout:     30 * time.Second,
		MaxRequestBodyBytes: 262144, // 256 KiB, mesmo que SQS
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
	fs.StringVar(&cfg.NATSURL, "nats-url", envOr("GQ_NATS_URL", cfg.NATSURL), "URL do broker NATS JetStream (nats:// ou tls://)")
	fs.StringVar(&cfg.NATSCredentialsFile, "nats-creds", envOr("GQ_NATS_CREDS", ""), "Arquivo .creds NATS para autenticação")
	fs.StringVar(&cfg.NATSCACert, "nats-ca-cert", envOr("GQ_NATS_CA_CERT", ""), "Certificado CA para validar TLS do broker")
	fs.StringVar(&cfg.AccountID, "account-id", envOr("GQ_ACCOUNT_ID", cfg.AccountID), "Account ID default (12 dígitos)")
	fs.StringVar(&cfg.Region, "region", envOr("GQ_REGION", cfg.Region), "Região AWS default")
	fs.StringVar((*string)(&cfg.AuthMode), "auth-mode", envOr("GQ_AUTH_MODE", string(cfg.AuthMode)), "Modo de auth: off|verify|strict")
	fs.StringVar((*string)(&cfg.LogLevel), "log-level", envOr("GQ_LOG_LEVEL", string(cfg.LogLevel)), "Nível de log: debug|info|warn|error")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", envOr("GQ_METRICS_ADDR", ""), "Endereço separado para /metrics (vazio = mesmo que --addr)")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", envDurationOr("GQ_SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout), "Timeout para shutdown gracioso")
	fs.Int64Var(&cfg.MaxRequestBodyBytes, "max-body-bytes", envInt64Or("GQ_MAX_BODY_BYTES", cfg.MaxRequestBodyBytes), "Tamanho máximo do body (bytes)")

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
	if c.NATSURL == "" {
		errs = append(errs, "nats-url não pode ser vazio")
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
