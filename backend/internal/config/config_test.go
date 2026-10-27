package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadIgnoresLegacyDelayOverride(t *testing.T) {
	t.Setenv("FIXED_DELAY_MINUTES", "1")
	t.Setenv("PROCESSING_DURATION_SECONDS", "2")
	if _, err := Load(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadAcceptsDefaults(t *testing.T) {
	t.Setenv("FIXED_DELAY_MINUTES", "5")
	t.Setenv("PROCESSING_DURATION_SECONDS", "2")
	t.Setenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
	t.Setenv("HTTP_ADDR", ":8080")
	t.Setenv("SQLITE_PATH", "./data/poc.db")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ProcessingDurationSeconds != 2 {
		t.Fatalf("%+v", cfg)
	}
}

func TestLoadDotEnvFillsMissingVariables(t *testing.T) {
	dir := t.TempDir()
	parent := filepath.Join(dir, "backend")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	contents := "AMQP_URL=amqp://guest:guest@127.0.0.1:5673/\nHTTP_ADDR=:9090\n"
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(parent)
	t.Setenv("AMQP_URL", "")
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("SQLITE_PATH", "./data/poc.db")
	t.Setenv("PROCESSING_DURATION_SECONDS", "2")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AMQPURL != "amqp://guest:guest@127.0.0.1:5673/" || cfg.HTTPAddr != ":9090" {
		t.Fatalf("%+v", cfg)
	}
}

func TestLoadDotEnvDoesNotOverrideExisting(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("AMQP_URL=amqp://from-file/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	t.Setenv("AMQP_URL", "amqp://from-shell/")
	t.Setenv("HTTP_ADDR", ":8080")
	t.Setenv("SQLITE_PATH", "./data/poc.db")
	t.Setenv("PROCESSING_DURATION_SECONDS", "2")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AMQPURL != "amqp://from-shell/" {
		t.Fatalf("%+v", cfg)
	}
}

func TestLoadRejectsProcessingOutOfRange(t *testing.T) {
	t.Setenv("FIXED_DELAY_MINUTES", "5")
	t.Setenv("PROCESSING_DURATION_SECONDS", "90")
	if _, err := Load(); err == nil {
		t.Fatal("expected processing duration to be rejected")
	}
}
