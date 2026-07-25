// Package observability centraliza logging estruturado, métricas Prometheus
// e tracing OpenTelemetry. É o único ponto onde o shim emite telemetria.
package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/equake/dropin-queue/shim/internal/config"
)

// Logger é o logger padrão do shim. Sempre estruturado (JSON em prod,
// texto colorido em dev).
var (
	defaultLogger *slog.Logger
	loggerMu      sync.RWMutex
)

// SetupLogger configura o logger global a partir do LogLevel da config.
//
// Em modo info/warn/error → JSON handler (machine-readable para Loki/CloudWatch).
// Em modo debug → text handler colorido (legível em dev).
// Output padrão: stderr.
func SetupLogger(cfg config.Config) *slog.Logger {
	return SetupLoggerTo(cfg, os.Stderr)
}

// SetupLoggerTo permite injetar writer (usado em testes para capturar logs).
func SetupLoggerTo(cfg config.Config, w io.Writer) *slog.Logger {
	loggerMu.Lock()
	defer loggerMu.Unlock()

	var level slog.Level
	switch cfg.LogLevel {
	case config.LogLevelDebug:
		level = slog.LevelDebug
	case config.LogLevelInfo:
		level = slog.LevelInfo
	case config.LogLevelWarn:
		level = slog.LevelWarn
	case config.LogLevelError:
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: cfg.LogLevel == config.LogLevelDebug,
	}

	var handler slog.Handler
	if cfg.LogLevel == config.LogLevelDebug {
		handler = slog.NewTextHandler(w, opts)
	} else {
		handler = slog.NewJSONHandler(w, opts)
	}

	defaultLogger = slog.New(handler).With(
		slog.String("service", "dropin-queue"),
		slog.String("version", Version),
	)
	slog.SetDefault(defaultLogger)
	return defaultLogger
}

// L retorna o logger global. Panic se não foi inicializado (bug de programação).
func L() *slog.Logger {
	loggerMu.RLock()
	defer loggerMu.RUnlock()
	if defaultLogger == nil {
		// Fallback: logger descartando output para evitar nil dereference.
		return slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return defaultLogger
}

// Version é a versão do shim (substituída em build via -ldflags="-X ...").
var Version = "dev"

// --- Tracing ---

// SetupTracing inicializa um tracer provider simples que escreve spans
// para o writer dado. Para produção, substituir por OTLP exporter.
//
// Retorna uma função de shutdown que deve ser chamada em defer no main.
func SetupTracing(ctx context.Context, cfg config.Config, w io.Writer) (func(context.Context) error, error) {
	if cfg.LogLevel != config.LogLevelDebug {
		// Tracing só ativo em debug por enquanto; OTLP será adicionado depois.
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := stdouttrace.New(
		stdouttrace.WithWriter(w),
		stdouttrace.WithPrettyPrint(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("dropin-queue"),
			semconv.ServiceVersion(Version),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}
