package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/equake/dropin-queue/shim/internal/config"
)

// captureLogger cria um logger que escreve em buffer para inspeção em testes.
func captureLogger(t *testing.T, level config.LogLevel) (*bytes.Buffer, func()) {
	t.Helper()
	buf := &bytes.Buffer{}
	cfg := config.Default()
	cfg.LogLevel = level
	_ = SetupLoggerTo(cfg, buf)
	return buf, func() { _ = buf }
}

func TestSetupLogger_JSONForInfo(t *testing.T) {
	buf, _ := captureLogger(t, config.LogLevelInfo)
	L().Info("hello", "key", "value")

	line := buf.String()
	if !strings.HasPrefix(strings.TrimSpace(line), "{") {
		t.Errorf("esperava JSON em info+, got: %s", line)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &m); err != nil {
		t.Fatalf("JSON inválido: %v / %s", err, line)
	}
	if m["msg"] != "hello" {
		t.Errorf("msg field: got %v", m["msg"])
	}
	if m["key"] != "value" {
		t.Errorf("key field: got %v", m["key"])
	}
	if m["service"] != "dropin-queue" {
		t.Errorf("service field: got %v", m["service"])
	}
}

func TestSetupLogger_TextForDebug(t *testing.T) {
	buf, _ := captureLogger(t, config.LogLevelDebug)
	L().Debug("hello")

	line := buf.String()
	// text handler usa formato key=value, não JSON.
	if strings.HasPrefix(strings.TrimSpace(line), "{") {
		t.Errorf("esperava text em debug, got JSON: %s", line)
	}
	if !strings.Contains(line, "hello") {
		t.Errorf("linha deve conter 'hello': %s", line)
	}
}

func TestSetupLogger_LevelFiltering(t *testing.T) {
	buf, _ := captureLogger(t, config.LogLevelWarn)
	L().Debug("ignored-debug")
	L().Info("ignored-info")
	L().Warn("kept-warn")
	L().Error("kept-error")

	out := buf.String()
	if strings.Contains(out, "ignored-debug") || strings.Contains(out, "ignored-info") {
		t.Errorf("logs abaixo de warn não devem aparecer: %s", out)
	}
	if !strings.Contains(out, "kept-warn") || !strings.Contains(out, "kept-error") {
		t.Errorf("warn/error devem aparecer: %s", out)
	}
}

func TestStartSpan(t *testing.T) {
	// Inicializa tracing para que StartSpan produza span válido.
	buf := &bytes.Buffer{}
	cfg := config.Default()
	cfg.LogLevel = config.LogLevelDebug
	shutdown, err := SetupTracing(context.Background(), cfg, buf)
	if err != nil {
		t.Fatalf("SetupTracing: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	ctx := context.Background()
	ctx2, span := StartSpan(ctx, "test-op")
	defer span.End()

	if !span.SpanContext().IsValid() {
		t.Fatal("span context deve ser válido com tracer configurado")
	}
	if ctx2 == nil {
		t.Fatal("ctx2 não pode ser nil")
	}
	if span.SpanContext().TraceID().IsValid() == false {
		t.Error("trace id deve ser válido")
	}
}

func TestStatusRecorder(t *testing.T) {
	// Sanidade do helper de status recorder.
	if StatusFromHTTP(200) != "200" {
		t.Errorf("StatusFromHTTP(200) != 200")
	}
}
