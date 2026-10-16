package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// Config holds all environment-based configuration for the application.
type Config struct {
	UpstreamWSURL string
	APIKey        string
	SecretKey     string
	Port          string
	LogLevel      string
}

// Load loads the configuration from .env (if present) and system environment variables.
// It enforces that required fields like APIKey and SecretKey are present.
func Load() (*Config, error) {
	// Attempt to load .env file. In production/Docker, variables are injected
	// directly into the environment, so we ignore file-not-found errors.
	_ = godotenv.Load()

	cfg := &Config{
		UpstreamWSURL: getEnv("UPSTREAM_WS_URL", "wss://v3-prod.livefxhub.com/ws/algo"),
		APIKey:        strings.TrimSpace(os.Getenv("API_KEY")),
		SecretKey:     strings.TrimSpace(os.Getenv("SECRET_KEY")),
		Port:          getEnv("PORT", "8080"),
		LogLevel:      getEnv("LOG_LEVEL", "info"),
	}

	// Validation: Fail fast if credentials are not configured
	if cfg.APIKey == "" {
		return nil, errors.New("API_KEY environment variable is required")
	}
	if cfg.SecretKey == "" {
		return nil, errors.New("SECRET_KEY environment variable is required")
	}

	return cfg, nil
}

// MaskKey returns a masked version of a sensitive key (e.g., "50558c...93b5")
// to prevent leaking full credentials into terminal logs.
func MaskKey(key string) string {
	if len(key) <= 8 {
		return "******"
	}
	return fmt.Sprintf("%s...%s", key[:6], key[len(key)-4:])
}

// getEnv retrieves an environment variable or returns a fallback default.
func getEnv(key, defaultVal string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return defaultVal
}
