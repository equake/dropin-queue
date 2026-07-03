package config

import (
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	c := Default()

	if c.Addr != ":4566" {
		t.Errorf("Addr default: esperado :4566, got %s", c.Addr)
	}
	if c.NATSURL != "nats://localhost:4222" {
		t.Errorf("NATSURL default: esperado nats://localhost:4222, got %s", c.NATSURL)
	}
	if c.AccountID != "000000000000" {
		t.Errorf("AccountID default: esperado 000000000000, got %s", c.AccountID)
	}
	if c.AuthMode != AuthModeOff {
		t.Errorf("AuthMode default deve ser 'off' para dev safe, got %s", c.AuthMode)
	}
	if c.MaxRequestBodyBytes != 262144 {
		t.Errorf("MaxRequestBodyBytes deve ser 256KiB, got %d", c.MaxRequestBodyBytes)
	}
}

func TestValidate_Ok(t *testing.T) {
	c := Default()
	if err := c.Validate(); err != nil {
		t.Fatalf("default config deve validar: %v", err)
	}
}

func TestValidate_InvalidAccountID(t *testing.T) {
	c := Default()
	c.AccountID = "123"
	if err := c.Validate(); err == nil {
		t.Fatal("esperava erro com account-id curto")
	}

	c.AccountID = "abcdefghijkl"
	if err := c.Validate(); err == nil {
		t.Fatal("esperava erro com account-id não-numérico")
	}

	c.AccountID = "000000000000"
	if err := c.Validate(); err != nil {
		t.Fatalf("account-id válido deve passar: %v", err)
	}
}

func TestValidate_InvalidAuthMode(t *testing.T) {
	c := Default()
	c.AuthMode = "broken"
	if err := c.Validate(); err == nil {
		t.Fatal("esperava erro com auth-mode inválido")
	}
}

func TestValidate_InvalidLogLevel(t *testing.T) {
	c := Default()
	c.LogLevel = "verbose"
	if err := c.Validate(); err == nil {
		t.Fatal("esperava erro com log-level inválido")
	}
}

func TestIsProduction(t *testing.T) {
	tests := []struct {
		mode AuthMode
		want bool
	}{
		{AuthModeOff, false},
		{AuthModeVerify, true},
		{AuthModeStrict, true},
	}
	for _, tc := range tests {
		c := Default()
		c.AuthMode = tc.mode
		if got := c.IsProduction(); got != tc.want {
			t.Errorf("AuthMode=%s: IsProduction()=%v, want %v", tc.mode, got, tc.want)
		}
	}
}

func TestLoad_Flags(t *testing.T) {
	// Limpa env vars que podem interferir
	t.Setenv("GQ_ADDR", "")
	t.Setenv("GQ_AUTH_MODE", "")
	t.Setenv("GQ_ACCOUNT_ID", "")

	c, err := Load([]string{"shimd", "--addr=:5000", "--auth-mode=strict", "--account-id=111122223333"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Addr != ":5000" {
		t.Errorf("Addr: got %s", c.Addr)
	}
	if c.AuthMode != AuthModeStrict {
		t.Errorf("AuthMode: got %s", c.AuthMode)
	}
	if c.AccountID != "111122223333" {
		t.Errorf("AccountID: got %s", c.AccountID)
	}
}

func TestLoad_Env(t *testing.T) {
	t.Setenv("GQ_ADDR", ":7000")
	t.Setenv("GQ_AUTH_MODE", "verify")
	t.Setenv("GQ_SHUTDOWN_TIMEOUT", "5s")

	c, err := Load([]string{"shimd"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Addr != ":7000" {
		t.Errorf("Addr from env: got %s", c.Addr)
	}
	if c.AuthMode != AuthModeVerify {
		t.Errorf("AuthMode from env: got %s", c.AuthMode)
	}
	if c.ShutdownTimeout != 5*time.Second {
		t.Errorf("ShutdownTimeout from env: got %v", c.ShutdownTimeout)
	}
}

func TestLoad_EnvOverridesDefaults(t *testing.T) {
	t.Setenv("GQ_NATS_URL", "tls://nats.example.com:4222")

	c, err := Load([]string{"shimd"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.NATSURL != "tls://nats.example.com:4222" {
		t.Errorf("NATSURL: got %s", c.NATSURL)
	}
}
