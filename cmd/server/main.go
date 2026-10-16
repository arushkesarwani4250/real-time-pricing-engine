package main

import (
	"log"

	"real_time/config"
)

func main() {
	log.Println("[Main] Initializing Real-Time Pricing Engine...")

	// 1. Load application configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[Main] Configuration error: %v", err)
	}

	// 2. Verify and log configuration (with sensitive credentials masked)
	log.Printf("[Main] Configuration successfully loaded.")
	log.Printf("[Main] Upstream WebSocket: %s", cfg.UpstreamWSURL)
	log.Printf("[Main] API Key:          %s", config.MaskKey(cfg.APIKey))
	log.Printf("[Main] Secret Key:       %s", config.MaskKey(cfg.SecretKey))
	log.Printf("[Main] Downstream Port:  %s", cfg.Port)
	log.Printf("[Main] Log Level:        %s", cfg.LogLevel)

	log.Println("[Main] Step 1 setup complete. Ready for Step 2 (Upstream WebSocket connection).")
}
