package observability

import (
	"bytes"
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
