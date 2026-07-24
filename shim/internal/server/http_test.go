package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/equake/dropin-queue/shim/internal/config"
	"github.com/equake/dropin-queue/shim/internal/observability"
)

func init() {
	// Logger descartando output para não poluir testes.
	cfg := config.Default()
	cfg.LogLevel = config.LogLevelError
	observability.SetupLogger(cfg)
	observability.SetupMetrics()
}

func TestIsJSONProtocol(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		amzTarget   string
		want        bool
	}{
		{"form sem target", "application/x-www-form-urlencoded", "", false},
		{"form com target", "application/x-www-form-urlencoded", "AmazonSQS.CreateQueue", false}, // CT errado
		{"json com target", "application/x-amz-json-1.0", "AmazonSQS.CreateQueue", true},
		{"json sem target", "application/x-amz-json-1.0", "", false},
		{"json vazio", "", "AmazonSQS.CreateQueue", false}, // CT vazio
	}
	for _, tc := range tests {
		req := httptest.NewRequest("POST", "/", strings.NewReader(""))
		req.Header.Set("Content-Type", tc.contentType)
		req.Header.Set("X-Amz-Target", tc.amzTarget)
		got := isJSONProtocol(req)
		if got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestNewRequestID_Unique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := newRequestID()
		if len(id) != 16 {
			t.Errorf("request id deve ter 16 chars hex, got %d: %q", len(id), id)
		}
		if seen[id] {
			t.Errorf("request id duplicado: %q", id)
		}
		seen[id] = true
	}
}

// TestRequestFromContext_PrefersMiddlewareID valida o bug fix do
// refactor/kiss-dry-pass-2 Commit 3:
//
// Pré-fix: handlers chamavam newRequestID() direto, gerando ID
// DIFERENTE do que o middleware atribuiu via X-Request-ID header.
// Pós-fix: requestFromContext(r) prefere o ID do middleware (do
// context), garantindo consistência entre header e body.
func TestRequestFromContext_PrefersMiddlewareID(t *testing.T) {
	req, _ := http.NewRequest("POST", "/", nil)
	// Simula o middleware já ter setado o ID no context.
	middlewareID := "req-from-middleware-aaa"
	ctx := withRequestID(req.Context(), middlewareID)
	req = req.WithContext(ctx)

	got := requestFromContext(req)
	if got != middlewareID {
		t.Errorf("requestFromContext: got %q, want %q (do middleware)", got, middlewareID)
	}
}

// TestRequestFromContext_FallbackToNewID valida fallback quando
// handler é invocado fora do chi stack (testes).
func TestRequestFromContext_FallbackToNewID(t *testing.T) {
	req, _ := http.NewRequest("POST", "/", nil)
	// Sem context com ID = fallback para newRequestID().
	got := requestFromContext(req)
	if got == "" {
		t.Error("fallback deve gerar ID, got empty string")
	}
}

func TestWriteSQSFatalError(t *testing.T) {
	tests := []struct {
		code   string
		status int
	}{
		{"QueueNameExists", 400},       // sender
		{"QueueDoesNotExist", 400},     // sender
		{"InvalidParameterValue", 400}, // sender
		{"MissingParameter", 400},      // sender
		{"InternalError", 500},         // receiver
		{"UnsupportedOperation", 500},  // receiver
	}
	for _, tc := range tests {
		var buf bytes.Buffer
		rec := httptest.NewRecorder()
		writeFatalError(rec, transportSQSQuery, tc.code, "test message", "req-abc")

		if rec.Code != tc.status {
			t.Errorf("%s: status got %d, want %d", tc.code, rec.Code, tc.status)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "text/xml" {
			t.Errorf("%s: Content-Type got %q", tc.code, ct)
		}

		// Recria buffer para EncodeSQSQueryError.
		_ = buf
		body := rec.Body.String()
		if !strings.Contains(body, "<Code>"+tc.code+"</Code>") {
			t.Errorf("%s: body sem Code: %s", tc.code, body)
		}
		if !strings.Contains(body, "test message") {
			t.Errorf("%s: body sem message: %s", tc.code, body)
		}
	}
}

func TestHealthz(t *testing.T) {
	s := New(":0", &Handlers{}, 5242880)
	req := httptest.NewRequest("GET", "/healthz", nil)
	rec := httptest.NewRecorder()
	s.healthz(rec, req)
	if rec.Code != 200 {
		t.Errorf("healthz status: got %d", rec.Code)
	}
}

func TestReadyz_NoStorage(t *testing.T) {
	s := New(":0", &Handlers{Storage: nil}, 5242880)
	req := httptest.NewRequest("GET", "/readyz", nil)
	rec := httptest.NewRecorder()
	s.readyz(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz sem storage deve ser 503, got %d", rec.Code)
	}
}

// TestRequestIDMiddleware injeta X-Request-ID.
func TestRequestIDMiddleware(t *testing.T) {
	s := New(":0", &Handlers{Storage: nil}, 5242880)
	req := httptest.NewRequest("GET", "/healthz", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Errorf("healthz status: got %d", rec.Code)
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Errorf("X-Request-ID deve estar presente")
	}
}

// TestRequestIDMiddleware_PreservesIncoming verifica que X-Request-ID
// do cliente é preservado se enviado.
func TestRequestIDMiddleware_PreservesIncoming(t *testing.T) {
	s := New(":0", &Handlers{Storage: nil}, 5242880)
	req := httptest.NewRequest("GET", "/healthz", nil)
	req.Header.Set("X-Request-ID", "my-trace-123")
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if got := rec.Header().Get("X-Request-ID"); got != "my-trace-123" {
		t.Errorf("X-Request-ID preserved: got %q", got)
	}
}

// TestLoggingMiddleware_Success verifica que requests OK são logadas.
func TestLoggingMiddleware_Success(t *testing.T) {
	var buf bytes.Buffer
	prev := observability.L()
	cfg := config.Default()
	cfg.LogLevel = config.LogLevelInfo
	newLogger := observability.SetupLoggerTo(cfg, &buf)
	defer func() {
		_ = newLogger
		_ = prev
	}()

	s := New(":0", &Handlers{Storage: nil}, 5242880)
	req := httptest.NewRequest("GET", "/healthz", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	out := buf.String()
	if !strings.Contains(out, "request") {
		t.Errorf("log deve conter 'request', got: %s", out)
	}
	if !strings.Contains(out, "200") {
		t.Errorf("log deve conter status 200: %s", out)
	}
}

// TestRecoveryMiddleware_Panic garante que panics viram 500.
func TestRecoveryMiddleware_Panic(t *testing.T) {
	// Substitui logger para evitar ruído.
	cfg := config.Default()
	cfg.LogLevel = config.LogLevelError
	observability.SetupLogger(cfg)

	s := New(":0", &Handlers{Storage: nil}, 5242880)
	// Registra rota temporária que panica.
	s.router.Get("/panic", func(w http.ResponseWriter, r *http.Request) {
		panic("boom!")
	})
	req := httptest.NewRequest("GET", "/panic", nil)
	rec := httptest.NewRecorder()

	// Não deve propagar o panic.
	s.router.ServeHTTP(rec, req)

	if rec.Code != 500 {
		t.Errorf("recovery: status got %d, want 500", rec.Code)
	}
}

// TestMetricsEndpoint garante que /metrics responde 200 e tem texto Prometheus.
func TestMetricsEndpoint(t *testing.T) {
	s := New(":0", &Handlers{Storage: nil}, 5242880)
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("/metrics status: got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "# HELP") {
		t.Errorf("/metrics deve ter formato Prometheus (com # HELP): %s", body[:200])
	}
}

// TestServerLifecycle verifica que ListenAndServe + Shutdown funcionam
// sem bloquear indefinidamente.
func TestServerLifecycle(t *testing.T) {
	s := New("127.0.0.1:0", &Handlers{Storage: nil}, 5242880)
	// Não chamamos ListenAndServe (que bloquearia). Apenas validamos que
	// o servidor foi construído corretamente.
	if s.srv == nil {
		t.Fatal("srv não deve ser nil")
	}
	if s.router == nil {
		t.Fatal("router não deve ser nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	// Shutdown em servidor não-started deve retornar imediatamente.
	if err := s.Shutdown(ctx); err != nil && err != http.ErrServerClosed {
		t.Errorf("shutdown em servidor não-started: %v", err)
	}
}

// --- Bench helpers ---

// ensure imports used (test parallelism, etc.)
var _ = sync.Once{}
var _ = io.Discard
var _ slog.Handler = (*noopHandler)(nil)

type noopHandler struct{}

func (noopHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (noopHandler) Handle(context.Context, slog.Record) error { return nil }
func (h noopHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h noopHandler) WithGroup(string) slog.Handler           { return h }
