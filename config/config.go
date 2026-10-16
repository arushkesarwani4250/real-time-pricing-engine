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
	MockFeed      bool
}

// Load loads the configuration from .env (if present) and system environment variables.
func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{
		UpstreamWSURL: getEnv("UPSTREAM_WS_URL", "wss://v3-prod.livefxhub.com/ws/algo"),
		APIKey:        strings.TrimSpace(os.Getenv("API_KEY")),
		SecretKey:     strings.TrimSpace(os.Getenv("SECRET_KEY")),
		Port:          getEnv("PORT", "8080"),
		LogLevel:      getEnv("LOG_LEVEL", "info"),
		MockFeed:      strings.ToLower(strings.TrimSpace(os.Getenv("MOCK_FEED"))) == "true",
	}

	// Validation: Fail fast only if not running mock feed
	if !cfg.MockFeed {
		if cfg.APIKey == "" {
			return nil, errors.New("API_KEY environment variable is required")
		}
		if cfg.SecretKey == "" {
			return nil, errors.New("SECRET_KEY environment variable is required")
		}
	}

	return cfg, nil
}

// MaskKey returns a masked version of a sensitive key.
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
