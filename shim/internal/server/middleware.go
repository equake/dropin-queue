package server

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/equake/dropin-queue/shim/internal/observability"
)

// requestIDMiddleware extrai X-Request-ID do request (se houver) ou
// gera um novo. Injeta em context e response header.
func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = newRequestID()
		}
		w.Header().Set("X-Request-ID", id)
		ctx := withRequestID(r.Context(), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// recoveryMiddleware captura panics e devolve 500 InternalError.
func recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				observability.L().Error("panic recovered",
					"panic", rec,
					"path", r.URL.Path,
					"method", r.Method,
				)
				if rec2, ok := rec.(error); ok {
					writeSQSFatalError(w, "InternalError", rec2.Error(), requestIDFromCtx(r.Context()))
				} else {
					writeSQSFatalError(w, "InternalError",
						"internal server error",
						requestIDFromCtx(r.Context()),
					)
				}
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// loggingMiddleware registra cada request com método, path, status, latência.
// Logs são estruturados (JSON), com campos padronizados.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := observability.NewStatusRecorder(w)

		next.ServeHTTP(rec, r)

		duration := time.Since(start)
		level := slog.LevelInfo
		if rec.Status >= 500 {
			level = slog.LevelError
		} else if rec.Status >= 400 {
			level = slog.LevelWarn
		}

		observability.L().LogAttrs(r.Context(), level, "request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.Status),
			slog.Int("bytes", rec.Bytes),
			slog.Duration("duration", duration),
			slog.String("request_id", requestIDFromCtx(r.Context())),
			slog.String("remote_addr", r.RemoteAddr),
			slog.String("user_agent", r.UserAgent()),
		)
	})
}

// metricsMiddleware emite métricas Prometheus para cada request HTTP.
//
// Métricas:
//   - shim_http_requests_total{service, action, status}
//   - shim_http_request_duration_seconds (histogram)
//   - shim_http_inflight_requests (gauge)
//
// service/action são inferidos do path (hoje só "/" para AWS).
func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := observability.NewStatusRecorder(w)

		service := "ops"
		action := r.URL.Path
		if r.URL.Path == "/" && r.Method == http.MethodPost {
			// Request AWS. Action será extraída depois pelo handler, mas aqui
			// emitimos uma métrica genérica que o handler atualiza via ObserveHTTP.
			service = "aws"
			action = "unknown"
		}

		observability.IncInflight(service)
		defer observability.DecInflight(service)

		next.ServeHTTP(rec, r)

		// Para requests AWS, o handler chama ObserveHTTP diretamente.
		// Aqui só emitimos métricas para endpoints não-AWS.
		if service != "aws" {
			observability.ObserveHTTP(service, action, strconv.Itoa(rec.Status), time.Since(start))
		}
	})
}

// contextKey é um tipo privado para keys em context (evita colisões).
type contextKey string

const requestIDKey contextKey = "request_id"

func withRequestID(parent context.Context, id string) context.Context {
	type k struct{ name contextKey }
	return context.WithValue(parent, k{requestIDKey}, id)
}

func requestIDFromCtx(ctx context.Context) string {
	type k struct{ name contextKey }
	if v, ok := ctx.Value(k{requestIDKey}).(string); ok {
		return v
	}
	return ""
}
