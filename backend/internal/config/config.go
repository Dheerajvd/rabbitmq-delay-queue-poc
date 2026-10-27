package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Config is the process configuration. Delay is chosen per job as 1, 2, or 5 minutes.
type Config struct {
	AMQPURL                   string
	HTTPAddr                  string
	SQLitePath                string
	ProcessingDurationSeconds int
}

func Load() (Config, error) {
	loadDotEnv()
	cfg := Config{
		AMQPURL:                   getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		HTTPAddr:                  getenv("HTTP_ADDR", ":8080"),
		SQLitePath:                getenv("SQLITE_PATH", "./data/poc.db"),
		ProcessingDurationSeconds: 2,
	}

	procRaw := getenv("PROCESSING_DURATION_SECONDS", "2")
	proc, err := strconv.Atoi(procRaw)
	if err != nil || proc < 0 || proc > 30 {
		return Config{}, fmt.Errorf("PROCESSING_DURATION_SECONDS must be an integer from 0 to 30 (got %q)", procRaw)
	}
	cfg.ProcessingDurationSeconds = proc

	if cfg.AMQPURL == "" || cfg.HTTPAddr == "" || cfg.SQLitePath == "" {
		return Config{}, fmt.Errorf("AMQP_URL, HTTP_ADDR, and SQLITE_PATH must be non-empty")
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// loadDotEnv fills unset environment variables from .env in the working
// directory, or in its parent when the server is started from backend/.
// A variable already set in the process is left unchanged.
func loadDotEnv() {
	cwd, err := os.Getwd()
	if err != nil {
		return
	}
	for _, path := range []string{filepath.Join(cwd, ".env"), filepath.Join(cwd, "..", ".env")} {
		if _, err := os.Stat(path); err != nil {
			continue
		}
		applyEnvFile(path)
		return
	}
}

func applyEnvFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}
		if key == "" || os.Getenv(key) != "" {
			continue
		}
		_ = os.Setenv(key, value)
	}
}
