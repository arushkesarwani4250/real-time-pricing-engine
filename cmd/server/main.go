package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"real_time/config"
	"real_time/internal/downstream"
	"real_time/internal/engine"
	"real_time/internal/storage"
	"real_time/internal/upstream"
)

func main() {
	log.Println("[Main] Initializing Real-Time Pricing Engine...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[Main] Configuration error: %v", err)
	}

	log.Printf("[Main] Configuration loaded.")
	log.Printf("[Main] Upstream WS: %s", cfg.UpstreamWSURL)
	log.Printf("[Main] API Key:     %s", config.MaskKey(cfg.APIKey))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 1. Initialize Persistence Store (Milestone 5)
	store, err := storage.NewFileStore("data/state.json")
	if err != nil {
		log.Fatalf("[Main] Failed to initialize file store: %v", err)
	}

	// 2. Initialize Engine components
	sanitizer := engine.NewSanitizer(0.50)
	pricingEngine := engine.NewPricingEngine()

	// 3. Restore state on startup (Survives process restarts)
	savedState, err := store.LoadState()
	if err != nil {
		log.Printf("[Main] Warning: Failed to load previous state: %v", err)
	} else if savedState != nil {
		restoredCount := pricingEngine.RestoreState(savedState, time.Now())
		log.Printf("[Main] [Recovery] Successfully restored 24h state for %d symbols from data/state.json", restoredCount)
	} else {
		log.Println("[Main] Cold start: No existing state file found. Starting fresh.")
	}

	// 4. Initialize Downstream WebSocket Hub & Server (Milestone 6)
	hub := downstream.NewHub(pricingEngine)
	go hub.Run(ctx)

	server := downstream.NewServer(cfg.Port, hub)
	go func() {
		if err := server.Start(ctx); err != nil {
			log.Printf("[Main] Downstream server stopped: %v", err)
		}
	}()

	// 5. Inbound channel for raw market ticks
	rawTickChan := make(chan []byte, 5000)

	// 6. Connect to live upstream or fallback to test feed generator
	client := upstream.NewClient(cfg.UpstreamWSURL, cfg.APIKey, cfg.SecretKey, upstream.AllRequiredSymbols)

	useMock := cfg.MockFeed
	if !useMock {
		if err := client.Connect(); err != nil {
			log.Printf("[Main] Live upstream connect failed: %v. Falling back to test feed.", err)
			useMock = true
		} else if err := client.Authenticate(); err != nil {
			log.Printf("[Main] Live upstream auth rejected: %v. Falling back to test feed.", err)
			useMock = true
		} else if err := client.Subscribe(); err != nil {
			log.Printf("[Main] Live upstream subscribe failed: %v. Falling back to test feed.", err)
			useMock = true
		}
	}

	if useMock {
		log.Println("[Main] Running with Mock Feed Generator (simulating 61 live currency pairs).")
		mock := upstream.NewMockFeed(upstream.AllRequiredSymbols)
		go mock.Run(ctx, rawTickChan, 20*time.Millisecond) // ~50 ticks/sec
	} else {
		log.Println("[Main] Streaming live market data from upstream WebSocket...")
		go func() {
			if err := client.Listen(ctx, rawTickChan); err != nil {
				log.Printf("[Main] Upstream listener error: %v", err)
			}
		}()
	}

	// 7. Processing Pipeline: Ingestion -> Sanitizer -> 24h Engine -> Downstream Hub
	go func() {
		log.Println("[Engine Worker] Real-Time 24-Hour calculation and broadcast pipeline active...")
		for {
			select {
			case <-ctx.Done():
				return
			case rawData, ok := <-rawTickChan:
				if !ok {
					return
				}

				cleanTick, err := sanitizer.SanitizeRaw(rawData)
				if err != nil {
					continue
				}

				enriched := pricingEngine.ProcessTick(cleanTick)

				// Broadcast delta price update to connected downstream WebSocket clients
				hub.Broadcast(enriched)

				m := sanitizer.GetMetrics()
				if m.ValidProcessed%50 == 0 {
					log.Printf("[Engine] [Tick #%4d] %-7s | Mid: %9.4f | 24h High: %9.4f | 24h Low: %9.4f | 24h Chg: %+6.2f%% | Clients: %d",
						m.ValidProcessed,
						enriched.Symbol,
						enriched.MidPrice,
						enriched.High24h,
						enriched.Low24h,
						enriched.Change24h,
						hub.ClientCount(),
					)
				}
			}
		}
	}()

	// 8. Background Periodic Persistence Ticker (Every 5 seconds)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				state := pricingEngine.ExportState()
				if err := store.SaveState(state); err != nil {
					log.Printf("[Store] Periodic state save failed: %v", err)
				}
			}
		}
	}()

	log.Printf("[Main] Engine running smoothly on :%s! Press Ctrl+C to stop.", cfg.Port)

	<-ctx.Done()
	log.Println("[Main] Shutdown signal received, terminating...")

	finalState := pricingEngine.ExportState()
	_ = store.SaveState(finalState)

	m := sanitizer.GetMetrics()
	log.Printf("[Main] Ingestion Summary: Processed=%d | Active Clients=%d", m.ValidProcessed, hub.ClientCount())
	log.Println("[Main] Clean shutdown complete.")
}
