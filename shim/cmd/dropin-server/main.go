// Command dropin-server é o entrypoint do clone auto-hospedável de AWS SNS/SQS.
//
// dropin-server faz parse da configuração, conecta ao broker (NATS
// JetStream ou Postgres, conforme GQ_BACKEND), inicia o servidor HTTP e
// gerencia shutdown gracioso.
//
// Uso:
//
//	dropin-server [flags]
//
// Variáveis de ambiente equivalentes a flags (precedência flag > env > default):
//
//	GQ_ADDR, GQ_BACKEND, GQ_ACCOUNT_ID, GQ_REGION, GQ_AUTH_MODE, GQ_LOG_LEVEL,
//	GQ_METRICS_ADDR, GQ_SHUTDOWN_TIMEOUT, GQ_MAX_BODY_BYTES,
//
//	# backend=nats
//	GQ_NATS_URL, GQ_NATS_CREDS, GQ_NATS_CA_CERT,
//	GQ_STREAM_REPLICAS, GQ_MAX_ACK_PENDING, GQ_TOPIC_MAX_AGE
//
//	# backend=postgres
//	GQ_POSTGRES_DSN, GQ_POSTGRES_MAX_CONNS, GQ_POSTGRES_POLL_INTERVAL,
//	GQ_POSTGRES_NOTIFY_COALESCE
//
// Exemplos:
//
//	# dev local, backend NATS (sem TLS, AUTH_MODE=off)
//	dropin-server --addr=:4566 --backend=nats --nats-url=nats://localhost:4222
//
//	# dev local, backend Postgres
//	dropin-server --addr=:4566 --backend=postgres --postgres-dsn=postgres://user:pass@localhost:5432/dropin
//
//	# produção (TLS + IAM strict)
//	dropin-server --addr=:4566 \
//	       --nats-url=tls://nats-1.prod:4222 \
//	       --nats-creds=/etc/dropin-server/jetstream.creds \
//	       --auth-mode=strict
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/equake/dropin-queue/shim/internal/config"
	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/server"
	"github.com/equake/dropin-queue/shim/internal/sns"
	"github.com/equake/dropin-queue/shim/internal/sqs"
	"github.com/equake/dropin-queue/shim/internal/storage"
	natsstorage "github.com/equake/dropin-queue/shim/internal/storage/nats"
	pgstorage "github.com/equake/dropin-queue/shim/internal/storage/postgres"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "dropin-server: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// 1. Config
	cfg, err := config.Load(os.Args)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// 2. Logger
	logger := observability.SetupLogger(cfg)
	logger.Info("dropin-server iniciando",
		"addr", cfg.Addr,
		"backend", string(cfg.Backend),
		"account_id", cfg.AccountID,
		"region", cfg.Region,
		"auth_mode", string(cfg.AuthMode),
	)

	// 3. Metrics
	observability.SetupMetrics()

	// 4. Tracing (apenas em debug por enquanto)
	traceShutdown, err := observability.SetupTracing(context.Background(), cfg, os.Stderr)
	if err != nil {
		logger.Warn("setup tracing falhou (continuando sem)", "err", err.Error())
		traceShutdown = func(context.Context) error { return nil }
	}

	// 5. Storage (broker) — trocável via cfg.Backend (GQ_BACKEND). Os dois
	// adapters satisfazem a mesma interface storage.Storage; nada abaixo
	// deste ponto sabe qual foi escolhido.
	connectCtx, connectCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer connectCancel()

	var store storage.Storage
	switch cfg.Backend {
	case config.BackendPostgres:
		store, err = pgstorage.Connect(connectCtx, pgstorage.Options{
			DSN:            cfg.PostgresDSN,
			MaxConns:       int32(cfg.PostgresMaxConns),
			PollInterval:   cfg.PostgresPollInterval,
			NotifyCoalesce: cfg.PostgresNotifyCoalesce,
		})
	case config.BackendNATS:
		prefix := ""
		if cfg.IsProduction() {
			// Em prod, usar prefix garante isolamento por tenant/ambiente.
			prefix = fmt.Sprintf("%s-%s", cfg.AccountID, cfg.Region)
		}
		store, err = natsstorage.Connect(connectCtx, natsstorage.Options{
			URL:             cfg.NATSURL,
			CredentialsFile: cfg.NATSCredentialsFile,
			CACert:          cfg.NATSCACert,
			Name:            "dropin-queue",
			Prefix:          prefix,
			StreamReplicas:  cfg.StreamReplicas,
			MaxAckPending:   cfg.MaxAckPending,
			TopicMaxAge:     cfg.TopicMaxAge,
		})
	default:
		return fmt.Errorf("backend desconhecido: %q", cfg.Backend)
	}
	if err != nil {
		return fmt.Errorf("connect storage: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			logger.Warn("close storage", "err", err.Error())
		}
	}()

	// 6. Services
	sqsService := sqs.New(store, cfg.AccountID, cfg.Region, endpointFromAddr(cfg.Addr))
	snsService := sns.New(store, cfg.AccountID, cfg.Region, endpointFromAddr(cfg.Addr))

	// 7. HTTP server
	srv := server.New(cfg.Addr, &server.Handlers{
		Storage: store,
		SQS:     sqsService,
		SNS:     snsService,
	}, cfg.MaxRequestBodyBytes)

	// 8. Lifecycle: rodando em goroutine, escuta sinais de shutdown.
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()

	// Aguita sinal ou erro do servidor.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		logger.Info("servidor HTTP encerrado")
	case sig := <-sigCh:
		logger.Info("sinal recebido, iniciando shutdown gracioso", "signal", sig.String())
	}

	// Shutdown gracioso.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http shutdown", "err", err.Error())
	}
	if err := traceShutdown(shutdownCtx); err != nil {
		logger.Warn("tracing shutdown", "err", err.Error())
	}

	logger.Info("dropin-server encerrado")
	return nil
}

// endpointFromAddr deriva uma URL base (scheme://host:porta) do addr.
//
// addr pode ser ":4566", "0.0.0.0:4566" ou "localhost:4566".
// Como o shim não sabe seu scheme público (atrás de LB geralmente é HTTPS),
// assume HTTP para dev e documenta que em prod o LB termina TLS.
//
// O endpoint é usado para construir QueueURLs (ex: http://localhost:4566/<account>/<queue>).
// Em produção, o ideal é que o cliente seja configurado com o endpoint correto
// (e.g. https://sqs.example.com) e o QueueURL emitido pelo shim contenha esse
// endpoint. Para Fase 1 assumimos HTTP simples.
func endpointFromAddr(addr string) string {
	host := "localhost"
	port := "4566"
	if addr != "" {
		if addr[0] == ':' {
			port = addr[1:]
		} else {
			// encontra último ':' (IPv6 pode ter múltiplos)
			idx := lastColon(addr)
			if idx >= 0 {
				host = addr[:idx]
				port = addr[idx+1:]
			} else {
				host = addr
			}
		}
	}
	return fmt.Sprintf("http://%s:%s", host, port)
}

func lastColon(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return i
		}
	}
	return -1
}
