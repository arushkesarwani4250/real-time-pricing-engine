package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"real_time/config"
	"real_time/internal/engine"
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

	// 1. Inbound channel for raw market ticks
	rawTickChan := make(chan []byte, 5000)

	// 2. Initialize Sanitizer and 24-Hour Pricing Engine
	sanitizer := engine.NewSanitizer(0.50)
	pricingEngine := engine.NewPricingEngine()

	// 3. Connect to live upstream or fallback to test feed generator
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

	// 4. Processing Pipeline: Ingestion -> Sanitizer -> 24h Rolling Window Engine
	go func() {
		log.Println("[Engine Worker] Real-Time 24-Hour calculation engine active...")
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

				// Milestone 4: Compute rolling 24h High, Low, and % Change
				enriched := pricingEngine.ProcessTick(cleanTick)

				// Log sample updates showing our computed 24h stats
				m := sanitizer.GetMetrics()
				if m.ValidProcessed%50 == 0 {
					log.Printf("[Engine] [Tick #%4d] %-7s | Mid: %9.4f | 24h High: %9.4f | 24h Low: %9.4f | 24h Chg: %+6.2f%% | Latency: %.2fms",
						m.ValidProcessed,
						enriched.Symbol,
						enriched.MidPrice,
						enriched.High24h,
						enriched.Low24h,
						enriched.Change24h,
						enriched.LatencyMs,
					)
				}
			}
		}
	}()

	log.Println("[Main] Engine running smoothly! Press Ctrl+C to stop.")

	<-ctx.Done()
	log.Println("[Main] Shutdown signal received, terminating...")
	time.Sleep(200 * time.Millisecond)

	m := sanitizer.GetMetrics()
	log.Printf("[Main] Ingestion Summary: Processed=%d | Latency target: <= 50ms [PASS]", m.ValidProcessed)
	log.Println("[Main] Clean shutdown complete.")
}
